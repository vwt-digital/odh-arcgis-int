import json
import logging
import os

from config import GIS_FEATURE_SERVICE, GIS_FEATURE_SERVICE_AUTHENTICATION
from requests_retry_session import get_requests_session
from utils import get_secret


class GISService:
    def __init__(self):
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

        data = {
            "f": "json",
            "username": GIS_FEATURE_SERVICE_AUTHENTICATION["username"],
            "password": get_secret(
                os.environ["PROJECT_ID"], GIS_FEATURE_SERVICE_AUTHENTICATION["secret"]
            ),
            "request": GIS_FEATURE_SERVICE_AUTHENTICATION["request"],
            "referer": GIS_FEATURE_SERVICE_AUTHENTICATION["referer"],
        }

        data = self.requests_session.post(
            GIS_FEATURE_SERVICE_AUTHENTICATION["url"], data
        ).json()
        return data["token"]

    def add_object_to_feature_layer(self, gis_object):
        """
        Add a new GIS object to feature layer

        :param gis_object: GIS object
        :type gis_object: dict

        :return: Feature ID
        :rtype: int
        """

        data = {"adds": json.dumps([gis_object]), "f": "json", "token": self.token}

        r = self.requests_session.post(f"{GIS_FEATURE_SERVICE}/applyEdits", data=data)

        try:
            response = r.json()
            if response.get("error", False):
                raise Exception(
                    f"Error when adding feature to GIS server - "
                    f"server responded with status {response['error']['code']}: "
                    f"{response['error']['message']}"
                )

            feature_id = response["addResults"][0]["objectId"]
            logging.info(f"Added new feature to map with ID {feature_id}")

            return feature_id
        except json.decoder.JSONDecodeError as e:
            logging.error(f"Status-code: {r.status_code}")
            logging.error(f"Output:\n{r.text}")
            logging.exception(e)