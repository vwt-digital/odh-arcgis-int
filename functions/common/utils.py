from datetime import datetime
from dataclasses import dataclass

from google.cloud.secretmanager import (
    SecretManagerServiceClient as Client,
    GetSecretVersionRequest,
    AccessSecretVersionRequest,
    AccessSecretVersionResponse,
    AddSecretVersionRequest,
    SecretPayload,
    SecretVersion,
    ReplicationStatus
)


@dataclass(frozen=True)
class Secret:
    value: bytes
    create_time: datetime
    destroy_time: datetime
    state: SecretVersion.State
    replication_status: ReplicationStatus

    def get_value(self, encoding: str = "UTF-8"):
        return self.value.decode(encoding)


def get_secret(project_id: str, secret_id: str, version: str = "latest") -> Secret:
    """
    Returns a Secret Manager secret.

    :param project_id: Google Cloud project ID
    :type project_id: str
    :param secret_id: Google Cloud Secret Manager secret ID
    :type project_id: str
    :param version: Version of the secret
    :type version: str

    :return: Secret value
    :rtype: str
    """
    client = Client()
    path = client.secret_version_path(project_id, secret_id, version)

    secret_version_request = GetSecretVersionRequest(path)
    secret_version_access_request = AccessSecretVersionRequest(path)

    secret_version: SecretVersion = client.get_secret_version(request=secret_version_request)
    secret_access: AccessSecretVersionResponse = client.access_secret_version(request=secret_version_access_request)

    return Secret(
        value=secret_access.payload.data,
        create_time=secret_version.create_time.ToDatetime(),
        destroy_time=secret_version.destroy_time.ToDatetime(),
        state=secret_version.state,
        replication_status=secret_version.replication_status
    )


def update_secret(project_id: str, secret_id: str, value: bytes) -> None:
    client = Client()
    path = client.secret_path(project_id, secret_id)

    secret_payload = SecretPayload(value)
    secret_version_add_request = AddSecretVersionRequest(path, payload=secret_payload)

    client.add_secret_version(request=secret_version_add_request)
