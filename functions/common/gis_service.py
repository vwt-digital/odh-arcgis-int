import json
import logging
import os
from datetime import datetime
from json.decoder import JSONDecodeError

from requests.exceptions import ConnectionError, HTTPError
from requests_retry_session import get_requests_session
from retry import retry
from typing import Optional
from utils import get_secret


class GISService:

    _REQUEST_SESSION = get_requests_session(
        retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
    )

    def __init__(self, arcgis_auth, arcgis_url, arcgis_name, disable_updated_at):
        """
        Initiates the GISService

        :param arcgis_auth: The ArcGIS authentication object
        :param arcgis_url: The ArcGIS feature layer URL
        :type arcgis_url: str
        :param arcgis_name: The ArcGIS feature name
        :type arcgis_name: str
        :param disable_updated_at: Disabled the addition of 'updated_at' field
        :type disable_updated_at: boolean
        """

        self._arcgis_auth = arcgis_auth
        self._arcgis_url = arcgis_url
        self._arcgis_name = arcgis_name
        self._disable_updated_at = disable_updated_at

        success, response = self._request_token(
            arcgis_auth.username,
            arcgis_auth.secret,
            arcgis_url,
            arcgis_auth.referer,
            arcgis_auth.request
        )
        if success:
            self.token = response
        else:
            logging.error(f"Something went wrong when requesting GIS token: {response}")

    def update_feature_layer(
            self,
            layer_id: int,
            to_update: list,
            to_create: list,
            to_delete: list
    ) -> (Optional[list], Optional[list], Optional[list]):
        """
        Update feature layer

        :param layer_id: Layer ID
        :type layer_id: int
        :param to_update: Features to update
        :type to_update: list
        :param to_create: Features to create
        :type to_create: list
        :param to_delete: Features to delete
        :type to_delete: list

        :return: (
            List of update results.,
            List of add results.,
            List of delete results.
        )
        :rtype: (list | None, list | None, list | None)
        """

        if not to_update and not to_create and not to_delete:
            return None, None, None

        data = self._create_update_data_object(to_create, to_delete, to_update)
        success, response = self._make_arcgis_request(
            action="applyEdits",
            feature_layer=layer_id,
            data=data
        )

        if success:
            return (
                response["updateResults"],
                response["addResults"],
                response["deleteResults"],
            )
        else:
            logging.error(
                f"Something went wrong while sending updates to GIS: {response}"
            )

        return None, None, None

    @retry(
        (ConnectionError, HTTPError, JSONDecodeError),
        tries=3,
        delay=5,
        logger=None,
        backoff=2,
    )
    def _create_update_data_object(self, to_create: list, to_delete: list, to_update: list):
        """
        Create a data object used for updating the Feature layer

        :param to_update: Features to update
        :type to_update: list
        :param to_create: Features to create
        :type to_create: list
        :param to_delete: Features to delete
        :type to_delete: list

        :return: Request data object
        :rtype: dict
        """

        # Set data object
        data = {
            "f": "json",
            "token": self.token,
        }

        # Set batch timestamp
        batch_timestamp = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        # Append create features if existing
        if to_create:
            data_adds = [obj["object"] for obj in to_create]

            if not self._disable_updated_at:
                for obj in data_adds:
                    obj["attributes"]["updated_at"] = batch_timestamp

            data["adds"] = json.dumps(data_adds)

        # Append update features if existing
        if to_update:
            data_updates = [obj["object"] for obj in to_update]

            if not self._disable_updated_at:
                for obj in data_updates:
                    obj["attributes"]["updated_at"] = batch_timestamp

            data["updates"] = json.dumps(data_updates)

        # Append delete features if existing
        if to_delete:
            data_deletes = [int(obj["objectId"]) for obj in to_delete]
            data["deletes"] = json.dumps(data_deletes)

        return data

    def delete_attachments(self, layer_id: int, feature_id: int, attachment_ids: list) -> Optional[list]:
        success, response = self._make_arcgis_request(
            action="deleteAttachments",
            feature_layer=layer_id,
            feature_id=feature_id,
            data={
                "attachmentIds": ",".join(attachment_ids)
            }
        )

        if success:
            return response["deleteAttachmentResults"]
        else:
            logging.error(
                f"Something went wrong while deleting attachments from GIS: {response}"
            )

        return None

    def get_attachments(self, layer_id: int, feature_id: int) -> Optional[list]:
        success, response = self._make_arcgis_request(
            action="attachments",
            feature_layer=layer_id,
            feature_id=feature_id
        )

        if success:
            return response["attachmentInfos"]
        else:
            logging.error(
                f"Something went wrong while getting attachments from GIS: {response}"
            )

        return None

    def upload_attachment(
            self,
            layer_id,
            feature_id,
            file_type,
            file_name,
            file_content
    ) -> Optional[int]:
        """
        Upload an attachment to a feature

        :param layer_id: Layer ID
        :type layer_id: int
        :param feature_id: Feature ID
        :type feature_id: int
        :param file_type: File content type
        :type file_type: str
        :param file_name: File name
        :type file_name: str
        :param file_content: File binary content
        :type file_content: str

        :return: Attachment ID
        :rtype: int
        """

        files = [("attachment", (file_name, file_content, file_type))]

        success, response = self._make_arcgis_request(
            action="addAttachment",
            feature_layer=layer_id,
            feature_id=feature_id,
            files=files
        )

        if success:
            attachment_id = response["addAttachmentResult"]["objectId"]
            logging.debug(
                f"Uploaded attachment '{attachment_id}' to feature with ID '{feature_id}'."
            )
            return int(attachment_id)
        else:
            logging.error(
                f"Something went wrong while uploading attachment to GIS: {response}"
            )

        return None

    @classmethod
    @retry(
        (ConnectionError, HTTPError, JSONDecodeError),
        tries=3,
        delay=5,
        logger=None,
        backoff=2,
    )
    def _request_token(
            cls,
            username: str,
            secret_key: str,
            auth_url: str,
            referer: str,
            request: str = "gettoken",
    ) -> (bool, str):
        request_data = {
            "f": "json",
            "username": username,
            "password": get_secret(os.environ["PROJECT_ID"], secret_key),
            "request": request,
            "referer": referer
        }

        try:
            response = cls._REQUEST_SESSION.post(auth_url, data=request_data)
            data = response.json()

            if "error" in data:
                return False, str(data["error"]["message"])

        except Exception as e:
            return False, str(e)

        return True, str(data["token"])

    def _query_features(self, feature_layer: int, query: str, out_fields: list) -> Optional[list]:
        success, response = self._make_arcgis_request(
            action="query",
            feature_layer=feature_layer,
            data={
                "where": query,
                "outFields": ",".join(out_fields)
            }
        )

        if success:
            return response["features"]
        else:
            logging.error(
                f"Something went wrong while making a query to GIS: {response}"
            )

        return None

    @retry(
        (ConnectionError, HTTPError, JSONDecodeError),
        tries=3,
        delay=5,
        logger=None,
        backoff=2,
    )
    def _make_arcgis_request(
            self,
            action: str,
            feature_layer: int,
            feature_id: int = None,
            data: dict = None,
            files: list = None
    ) -> (bool, dict):
        request_data = {"f": "json", "token": self.token}
        if data:
            request_data.update(data)

        url = f"{self._arcgis_url}/{feature_layer}"
        if feature_id is not None:
            url = f"{url}/{feature_id}"

        url = f"{url}/{action}"

        try:
            response = self._REQUEST_SESSION.post(url, data=request_data, files=files)
            data = response.json()

            if "error" in data:
                return False, data["error"]

        except Exception as e:
            return False, dict(message=str(e))

        return True, data
