import json
import logging
import os
from datetime import datetime

import requests
from requests_retry_session import get_requests_session
from utils import get_secret


class GISService:
    def __init__(self, arcgis_auth, arcgis_url, arcgis_name):
        """
        Initiates the GISService

        :param arcgis_auth: The ArcGIS authentication object
        :param arcgis_url: The ArcGIS feature layer URL
        :type arcgis_url: str
        :param arcgis_name: The ArcGIS feature name
        :type arcgis_name: str
        """

        self.arcgis_auth = arcgis_auth
        self.arcgis_url = arcgis_url
        self.arcgis_name = arcgis_name

        self.requests_session = get_requests_session(
            retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
        )
        self.token = self._get_feature_service_token()

    def _get_feature_service_token(self):
        """
        Request a new feature service token

        :return: Token
        :rtype: str
        """

        gis_r = None

        try:
            request_data = {
                "f": "json",
                "username": self.arcgis_auth.username,
                "password": get_secret(
                    os.environ["PROJECT_ID"], self.arcgis_auth.secret
                ),
                "request": self.arcgis_auth.request,
                "referer": self.arcgis_auth.referer,
            }

            gis_r = self.requests_session.post(self.arcgis_auth.url, request_data)
            gis_r.raise_for_status()

            r_json = gis_r.json()

            if "token" in r_json:
                return r_json["token"]

            logging.error(
                f"An error occurred when retrieving ArcGIS token: {r_json.get('error', gis_r.content)}"
            )
            return None
        except KeyError as e:
            logging.error(
                f"Function is missing authentication configuration for retrieving ArcGIS token: {str(e)}"
            )
            return None
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            json.decoder.JSONDecodeError,
        ) as e:
            logging.error(
                f"An error occurred when retrieving ArcGIS token: {str(e)} ({gis_r.content})"
            )
            return None

    def get_objectids_in_feature_layer(self, layer_id, id_field, id_values):
        """
        Check if features already exist within ArcGIS Feature Layer

        :param layer_id: Layer ID
        :type layer_id: int
        :param id_field: ID field
        :type id_field: str
        :param id_values: ID value
        :type id_values: list

        :return: Feature's object IDs
        :rtype: dict
        """

        params = {
            "where": "{} in ({})".format(
                id_field, ",".join(f"'{key}'" for key in id_values)
            ),
            "outFields": f"{id_field},objectid",
            "f": "json",
            "token": self.token,
        }

        r = self.requests_session.get(
            f"{self.arcgis_url}/{layer_id}/query", params=params
        )

        try:
            response = r.json()
        except json.decoder.JSONDecodeError as e:
            logging.error(
                f"Error when searching for features in GIS server layer {layer_id}: {str(e)}"
            )
            logging.info(r.content)
            return {}
        else:
            if "error" in response:
                logging.error(
                    f"Searching for existing features in map layer '{layer_id}' resulted in an error: "
                    + json.dumps(response["error"])
                )
                return {}

            feature_ids = {}

            for feature in response.get("features", []):
                feature_id = feature["attributes"][id_field]
                object_id = int(feature["attributes"]["objectid"])

                feature_ids[feature_id] = object_id

            return feature_ids

    def update_feature_layer(self, layer_id, to_update, to_create):
        """
        Update feature layer

        :param layer_id: Layer ID
        :type layer_id: int
        :param to_update: Features to update
        :type to_update: list
        :param to_create: Features to create
        :type to_create: list

        :return: Feature ID
        :rtype: int
        """

        # Create list with entities to update
        data_adds = [obj["object"] for obj in to_create]
        data_updates = [obj["object"] for obj in to_update]

        # Set update_at field for each entity
        batch_timestamp = (
            datetime.utcnow().isoformat(timespec="seconds") + "Z"
        )  # Set batch timestamp

        for obj in data_adds:
            obj["attributes"]["updated_at"] = batch_timestamp

        for obj in data_updates:
            obj["attributes"]["updated_at"] = batch_timestamp

        data = {
            "adds": json.dumps(data_adds),
            "updates": json.dumps(data_updates),
            "f": "json",
            "token": self.token,
        }

        try:
            r = self.requests_session.post(
                f"{self.arcgis_url}/{layer_id}/applyEdits", data=data
            )

            response = r.json()
            if response.get("error", False):
                logging.error(
                    f"Error when updating GIS server layer {layer_id} - server responded with status "
                    f"{response['error']['code']}: {response['error']['message']}"
                )
                return None, None

            return response["updateResults"], response["addResults"]
        except requests.exceptions.ConnectionError as e:
            logging.error(
                f"Connection error when updating GIS server layer {layer_id}: {str(e)}"
            )
            return None, None
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when updating GIS server layer {layer_id}: {str(e)}")
            logging.info(r.content)
            return None, None

    def upload_attachment_to_feature_layer(
        self, layer_id, feature_id, file_type, file_name, file_content
    ):
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

        data = {"f": "json", "token": self.token}

        files = [("attachment", (file_name, file_content, file_type))]

        try:
            r = self.requests_session.post(
                f"{self.arcgis_url}/{layer_id}/{feature_id}/addAttachment",
                data=data,
                files=files,
            )

            response = r.json()
            if response.get("error", False):
                logging.error(
                    f"Error when uploading attachment to GIS server layer {layer_id}  - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
                return None

            attachment_id = response["addAttachmentResult"]["objectId"]
            logging.debug(
                f"Uploaded attachment {attachment_id} to feature with ID {feature_id}"
            )

            return int(attachment_id)
        except requests.exceptions.ConnectionError as e:
            logging.error(
                f"Connection error when uploading attachment to GIS server layer {layer_id}: {str(e)}"
            )
            return None
        except json.decoder.JSONDecodeError as e:
            logging.error(
                f"Error when uploading attachment to GIS server layer {layer_id}: {str(e)}"
            )
            logging.info(r.content)
            return None
