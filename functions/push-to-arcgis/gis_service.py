import json
import logging
import os
import sys

import requests
from requests_retry_session import get_requests_session
from utils import get_secret


class GISService:
    def __init__(self, arcgis_auth, arcgis_url, arcgis_name, firestore_service):
        """
        Initiates the GISService

        :param arcgis_auth: The ArcGIS authentication object
        :type arcgis_auth: dict
        :param arcgis_url: The ArcGIS feature layer URL
        :type arcgis_url: str
        :param arcgis_name: The ArcGIS feature name
        :type arcgis_name: str
        :param firestore_service: The Firestore service
        """

        self.arcgis_auth = arcgis_auth
        self.arcgis_url = arcgis_url
        self.arcgis_name = arcgis_name

        self.requests_session = get_requests_session(
            retries=3, backoff=15, status_forcelist=(404, 500, 502, 503, 504)
        )
        self.token = self._get_feature_service_token()

        self.firestore_client = firestore_service

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
            sys.exit(1)
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as e:
            logging.error(f"An error occurred when retrieving ArcGIS token: {str(e)}")
            sys.exit(1)
        else:
            return data["token"]

    def get_existing_object_id(self, existence_check, id_field, id_value):
        """
        Check if feature already exist

        :param existence_check: Existence check type
        :type existence_check: str
        :param id_field: ID field
        :type id_field: str
        :param id_value: ID value

        :return: Feature ID
        :rtype: int
        """

        if existence_check == "arcgis":
            return self.get_existing_object_id_in_feature_layer(id_field, id_value)

        if existence_check == "firestore":
            return self.get_existing_object_id_in_firestore(id_value)

        if existence_check:
            logging.error(
                f"The existence check value '{existence_check}' is not supported, "
                "supported types: 'arcgis', 'firestore'"
            )

        return None

    def get_existing_object_id_in_feature_layer(self, id_field, id_value):
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
                logging.info(f"Found existing feature in map with ID {feature_id}")

                return feature_id

            if "error" in response:
                logging.error(
                    f"Searching for existing feature in map resulted in an error: {json.dumps(response['error'])}"
                )
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when searching for feature in GIS server: {str(e)}")
            logging.info(json.dumps(r.content))
            return None
        else:
            return None

    def get_existing_object_id_in_firestore(self, id_value):
        """
        Check if feature already exist within Firestore database

        :param id_value: ID value

        :return: Feature ID
        :rtype: int
        """

        entity = self.firestore_client.get_entity(id_value)

        if entity and "objectId" in entity:
            return entity["objectId"]

        return None

    def add_object_to_feature_layer(self, gis_object, object_id):
        """
        Add a new GIS object to feature layer

        :param gis_object: GIS object
        :type gis_object: dict
        :param object_id: Object ID
        :type object_id: str

        :return: Feature ID
        :rtype: int
        """

        data = {"adds": json.dumps([gis_object]), "f": "json", "token": self.token}

        r = self.requests_session.post(f"{self.arcgis_url}/applyEdits", data=data)

        try:
            response = r.json()
            if response.get("error", False):
                logging.error(
                    f"Error when adding feature to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
                return None

            feature_id = response["addResults"][0]["objectId"]
            logging.info(f"Added new feature to map with ID {feature_id}")

            # Save new Feature if Firestore is enabled
            if self.firestore_client:
                logging.info(f"Adding feature to Firestore with ID {feature_id}")
                self.firestore_client.set_entity(
                    object_id, {"objectId": feature_id, "entityId": object_id}
                )

            return feature_id
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when adding feature to GIS server: {str(e)}")
            logging.info(json.dumps(r.content))
            return None

    def update_object_to_feature_layer(self, gis_object, feature_id):
        """
        Update an existing GIS object to feature layer

        :param gis_object: GIS object
        :type gis_object: dict
        :param feature_id: Feature ID
        :type feature_id: int
        """

        gis_object["attributes"]["objectid"] = int(feature_id)

        data = {"updates": json.dumps([gis_object]), "f": "json", "token": self.token}

        r = self.requests_session.post(f"{self.arcgis_url}/applyEdits", data=data)

        try:
            response = r.json()
            if response.get("error", False):
                logging.error(
                    f"Error when updating feature to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
                return None

            logging.info(f"Updated feature with ID {feature_id}")
            return feature_id
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when updating feature to GIS server: {str(e)}")
            logging.info(json.dumps(r.content))
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

        r = self.requests_session.post(
            f"{self.arcgis_url}/{feature_id}/addAttachment", data=data, files=files
        )

        try:
            response = r.json()
            if response.get("error", False):
                logging.error(
                    f"Error when uploading attachment to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )
                return None

            attachment_id = response["addAttachmentResult"]["objectId"]
            logging.info(
                f"Uploaded attachment {attachment_id} to feature with ID {feature_id}"
            )

            return attachment_id
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Error when uploading attachment to GIS server: {str(e)}")
            logging.info(json.dumps(r.content))
            return None
