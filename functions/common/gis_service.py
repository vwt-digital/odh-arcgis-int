import json
import logging
import os
from datetime import datetime
from json.decoder import JSONDecodeError

from requests.exceptions import ConnectionError, HTTPError
from retry import retry
from typing import Optional

from . import configuration
from . import requests_retry_session as request_session
from . import utils


class GISService:

    _REQUEST_SESSION = request_session.get_requests_session(
        retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
    )

    def __init__(self, token: str, feature_server_url: str, disable_updated_at: bool = False):
        """
        Creates a new GIS service.

        :param token: Authentication token.
        :type token: str
        :param feature_server_url: URL of the feature server.
        :type feature_server_url: str
        :param disable_updated_at: Disable timestamping of updates.
        :type disable_updated_at: bool
        """
        self._token = token
        self._feature_server_url = feature_server_url
        self._disable_updated_at = disable_updated_at

    @classmethod
    def from_configuration(cls, config: configuration.Configuration):
        """
        Creates a GISService from configuration.

        :param config: The configuration to get the GIS (authorization-)settings from.
        :type config: Configuration

        :return: A GISService based on the specified configuration, or none if authentication failed.
        :rtype: GISService | None
        """
        success, response = cls.request_token(
            username=config.arcgis_auth.username,
            password=utils.get_secret(os.environ["PROJECT_ID"], config.arcgis_auth.secret),
            auth_url=config.arcgis_auth.url,
            referer=config.arcgis_auth.referer,
            request=config.arcgis_auth.request
        )

        if success:
            return cls(
                response,
                config.arcgis_feature_service.url,
                config.mapping.disable_updated_at  # Can be deprecated, never configured.
            )
        else:
            logging.error(f"Could not login to ArcGIS: {response}")
            return None

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
            "token": self._token,
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

    def delete_attachments(self, feature_layer: int, feature_id: int, attachment_ids: list) -> Optional[list]:
        """
        Deletes the attachments from the feature.

        :param feature_layer: Feature layer id.
        :type feature_layer: int
        :param feature_id: Feature id.
        :type feature_id: int
        :param attachment_ids: List of attachment ids.
        :type attachment_ids: list[int]

        :return: A list of delete results, or none if request failed.
        :rtype: list | None
        """
        success, response = self._make_arcgis_request(
            action="deleteAttachments",
            feature_layer=feature_layer,
            feature_id=feature_id,
            data={
                "attachmentIds": ",".join(map(str, attachment_ids))
            }
        )

        if success:
            return response["deleteAttachmentResults"]
        else:
            logging.error(
                f"Something went wrong while deleting attachments from GIS: {response}"
            )

        return None

    def delete_features(self, feature_layer: int, feature_ids: list) -> Optional[list]:
        """
        Deletes the features and its attachments from the layer.

        :param feature_layer: Feature layer id.
        :type feature_layer: int
        :param feature_ids: List of feature ids.
        :type feature_ids: list[int]

        :return: A list of delete results, or none if request failed.
        :rtype: list | None
        """
        for feature_id in feature_ids:
            attachments = self.get_attachments(feature_layer, feature_id)
            if attachments:
                attachment_ids = [int(attachment["id"]) for attachment in attachments]
                self.delete_attachments(feature_layer, feature_id, attachment_ids)

        success, response = self._make_arcgis_request(
            action="deleteFeatures",
            feature_layer=feature_layer,
            data={
                "objectIds": ", ".join(map(str, feature_ids))
            }
        )

        if success:
            return response["deleteResults"]
        else:
            logging.error(
                f"Something went wrong while deleting features from GIS: {response}"
            )

        return None

    def get_attachments(self, layer_id: int, feature_id: int) -> Optional[list]:
        """
        Returns a list of information on each attachment linked to the feature.

        :param layer_id: Feature layer id.
        :type layer_id: int
        :param feature_id: Feature id.
        :type feature_id: int

        :return: A list of information on each attachment linked to the feature, or not if request failed.
        :rtype: list | None
        """
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

    def get_feature_object_id_map(self, feature_layer: int, id_field: str, id_values: list) -> dict:
        """
        Finds all features which 'id_field' value is in the 'id_values' list.
        Then returns a map of each feature's 'id_field' and it's corresponding 'objectid'.

        :param feature_layer: Feature layer id.
        :type feature_layer: int
        :param id_field: The field representing an id.
        :type id_field: str
        :param id_values: A list of values to be matched with the features' 'id_field' value.

        :return: A map of each feature's 'id_field' and it's corresponding 'objectid'.
        :rtype: dict
        """
        id_values_string = ",".join(f"'{key}'" for key in id_values)
        features = self.query_features(
            feature_layer=feature_layer,
            out_fields=["objectid", id_field],
            query=f"{id_field} in ({id_values_string})"
        )

        feature_map = {}
        if features:
            for feature in features:
                feature_id = feature["attributes"][id_field]
                object_id = feature["attributes"]["objectid"]

                feature_map[feature_id] = object_id

        return feature_map

    @classmethod
    @retry(
        (ConnectionError, HTTPError, JSONDecodeError),
        tries=3,
        delay=5,
        logger=None,
        backoff=2,
    )
    def request_token(
            cls,
            username: str,
            password: str,
            auth_url: str = "https://geoportaal.vwinfra.nl/portal/sharing/rest/generateToken",
            referer: str = "https://geoportaal.vwinfra.nl/portal",
            request: str = "gettoken",
    ) -> (bool, str):
        request_data = {
            "f": "json",
            "username": username,
            "password": password,
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

    def query_features(self, feature_layer: int, query: str, out_fields: list) -> Optional[list]:
        """
        Queries the feature layer.

        :param feature_layer: Feature layer id.
        :type feature_layer: int
        :param query: The query to use on the feature layer.
        :type query: str
        :param out_fields: The fields to be present in the returned features attributes.
        :type out_fields: list[str]

        :return: A list of features that matched the query, or none when the query request fails.
        :rtype: list | None
        """
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
        request_data = {"f": "json", "token": self._token}
        if data:
            request_data.update(data)

        url = f"{self._feature_server_url}/{feature_layer}"
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
