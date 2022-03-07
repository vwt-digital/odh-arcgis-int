from datetime import datetime
from dataclasses import dataclass

from google.cloud.secretmanager import (
    SecretManagerServiceClient as Client,
    GetSecretVersionRequest as GetRequest,
    AccessSecretVersionRequest as AccessRequest,
    AccessSecretVersionResponse as AccessResponse,
    AddSecretVersionRequest as AddRequest,
    SecretPayload as Payload,
    SecretVersion as Version,
    ReplicationStatus
)


@dataclass(frozen=True)
class Secret:
    value: bytes
    create_time: datetime
    destroy_time: datetime
    state: Version.State
    replication_status: ReplicationStatus

    def get_value(self, encoding: str = "UTF-8") -> str:
        """
        Returns the secret value.

        :param encoding: Encoding to use to convert bytes to string.
        :type encoding: str

        :return: The secret value.
        :rtype: str
        """
        return self.value.decode(encoding)


def get_secret(project_id: str, secret_id: str, version: str = "latest") -> Secret | None:
    """
    Returns a Secret Manager secret.

    :param project_id: Google Cloud project ID.
    :type project_id: str
    :param secret_id: Google Cloud Secret Manager secret ID.
    :type project_id: str
    :param version: Version of the secret.
    :type version: str

    :return: The secret.
    :rtype: Secret | None
    """
    client = Client()
    path = client.secret_version_path(project_id, secret_id, version)

    try:
        # Throws a PermissionDenied (403) when version does not exist
        secret_version_request = GetRequest(name=path)
    except Exception:
        return None

    secret_version_access_request = AccessRequest(name=path)

    secret_version: Version = client.get_secret_version(request=secret_version_request)
    secret_access: AccessResponse = client.access_secret_version(request=secret_version_access_request)

    return Secret(
        value=secret_access.payload.data,
        create_time=secret_version.create_time.ToDatetime(),
        destroy_time=secret_version.destroy_time.ToDatetime(),
        state=secret_version.state,
        replication_status=secret_version.replication_status
    )


def update_secret(project_id: str, secret_id: str, value: bytes) -> None:
    """
    Updates a secret to a new version.

    :param project_id: Google Cloud project ID.
    :type project_id: str
    :param secret_id: Google Cloud Secret Manager secret ID.
    :type project_id: str
    :param value: Secret value as bytes.
    :type value: bytes
    """
    client = Client()
    path = client.secret_path(project_id, secret_id)

    secret_payload = Payload(data=value)
    secret_version_add_request = AddRequest(parent=path, payload=secret_payload)

    client.add_secret_version(request=secret_version_add_request)
