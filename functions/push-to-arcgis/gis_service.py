import json
import logging
import os

import requests
from requests_retry_session import get_requests_session
from utils import get_secret


class GISService:
    def __init__(self, arcgis_auth, arcgis_url, arcgis_name):
        """
        Initiates the GISService

        :param arcgis_auth: The ArcGIS authentication object
        :type arcgis_auth: dict
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

        try:
            request_data = {
                "f": "json",
                "username": self.arcgis_auth["username"],
                "password": get_secret(
                    os.environ["PROJECT_ID"], self.arcgis_auth["secret"]
                ),
                "request": self.arcgis_auth["request"],
                "referer": self.arcgis_auth["referer"],
            }

            data = self.requests_session.post(
                self.arcgis_auth["url"], request_data
            ).json()
        except KeyError as e:
            logging.error(
                f"Function is missing authentication configuration for retrieving ArcGIS token: {str(e)}"
            )
            return None
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as e:
            logging.error(f"An error occurred when retrieving ArcGIS token: {str(e)}")
            return None
        else:
            return data["token"]

    def get_object_id_in_feature_layer(self, id_field, id_value):
        """
        Check if feature already exist within ArcGIS Feature Layer

        :param id_field: ID field
        :type id_field: str
        :param id_value: ID value

        :return: Feature ID
        :rtype: int
        """

        params = {
            "where": f"{id_field}='{id_value}'",
            "returnIdsOnly": True,
            "f": "json",
            "token": self.token,
        }

        r = self.requests_session.get(f"{self.arcgis_url}/query", params=params)

        try:
            response = r.json()

            if len(response.get("objectIds", [])) > 0:
                feature_id = response["objectIds"][-1]
                logging.debug(
                    f"Found existing feature for '{id_value}' in map with ID {feature_id}"
                )

                return feature_id

            if "error" in response:
                logging.error(
                    f"Searching for existing feature in map resulted in an error: {json.dumps(response['error'])}"
                )
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when searching for feature in GIS server: {str(e)}")
            logging.info(r.content)
            return None
        else:
            return None

    def update_feature_layer(self, to_update, to_create):
        """
        Update feature layer

        :param to_update: Features to update
        :type to_update: list
        :param to_create: Features to create
        :type to_create: list

        :return: Feature ID
        :rtype: int
        """

        data_adds = [obj["object"] for obj in to_create]
        data_updates = [obj["object"] for obj in to_update]

        data = {
            "adds": json.dumps(data_adds),
            "updates": json.dumps(data_updates),
            "f": "json",
            "token": self.token,
        }

        try:
            r = self.requests_session.post(f"{self.arcgis_url}/applyEdits", data=data)

            response = r.json()
            if response.get("error", False):
                logging.error(
                    f"Error when updating GIS server - server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
                return None

            if len(response["addResults"]) > 0:
                logging.info(f"Added {len(response['addResults'])} new feature(s)")

            if len(response["updateResults"]) > 0:
                logging.info(
                    f"Updated {len(response['updateResults'])} existing feature(s)"
                )

            return response["updateResults"], response["addResults"]
        except requests.exceptions.ConnectionError as e:
            logging.error(f"Connection error when updating GIS server: {str(e)}")
            return None
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when updating GIS server: {str(e)}")
            logging.info(r.content)
            return None

    def upload_attachment_to_feature_layer(
        self, feature_id, file_type, file_name, file_content
    ):
        """
        Upload an attachment to a feature

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
                f"{self.arcgis_url}/{feature_id}/addAttachment", data=data, files=files
            )

            response = r.json()
            if response.get("error", False):
                logging.error(
                    f"Error when uploading attachment to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
                return None

            attachment_id = response["addAttachmentResult"]["objectId"]
            logging.debug(
                f"Uploaded attachment {attachment_id} to feature with ID {feature_id}"
            )

            return attachment_id
        except requests.exceptions.ConnectionError as e:
            logging.error(
                f"Connection error when uploading attachment to GIS server: {str(e)}"
            )
            return None
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when uploading attachment to GIS server: {str(e)}")
            logging.info(r.content)
            return None
