import unittest
from unittest import mock

from configuration import Configuration
from gis_service import GISService

config = Configuration()


class MockResponse:
    def __init__(self):
        self.json_data = None
        self.text = None
        self.status_code = None
        self.raise_for_status_status = None

    def json(self):
        return self.json_data

    def raise_for_status(self):
        return self.raise_for_status_status

    def mock_response(
        self, status=200, content="", json_data=None, raise_for_status=None
    ):

        self.raise_for_status_status = raise_for_status
        self.status_code = status
        self.text = content
        self.json_data = json_data

        return self


class TestGISService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.gis_service = GISService(
            config.arcgis_auth,
            config.arcgis_feature_service.url,
            config.arcgis_feature_service.id,
            config.mapping.disable_updated_at,
        )
        cls.mock_res = MockResponse()

    @mock.patch("requests.Session.post")
    def test_get_auth_token(self, mock_post):
        res = {
            "token": "6hrFDATxrG9w14QY9wwnmVhLE0Wg6LIvwOwUaxz761m1JfRp4rs8Mzozk5xhSkw0_MQz6bpcJnrFUDwp5lPPFC157dHxbk"
            "KlDiQ9XY3ZIP8zAGCsS8ruN2uKjIaIargX",
            "expires": 1582930261424,
            "ssl": True,
        }
        self.mock_res.mock_response(200, json_data=res)

        mock_post.return_value = self.mock_res

        gis_service = GISService(
            config.arcgis_auth,
            config.arcgis_feature_service.url,
            config.arcgis_feature_service.id,
            config.mapping.disable_updated_at,
        )

        self.assertEqual(res["token"], gis_service.token)

    @mock.patch("requests.Session.post")
    def test_create_feature_success(self, mock_post):
        res = {
            "id": 0,
            "addResults": [
                {
                    "objectId": 1000,
                    "globalId": "{74100804-E229-49b8-8CDC-9B5D3EF03EDA}",
                    "success": True,
                }
            ],
            "updateResults": [],
            "deleteResults": [],
        }

        self.mock_res.mock_response(200, json_data=res)

        mock_post.return_value = self.mock_res

        (
            updateResults,
            addResults,
            deleteResults,
        ) = self.gis_service.update_feature_layer(
            layer_id=1, to_create=[], to_update=[], to_delete=[]
        )

        self.assertEqual(res["addResults"], addResults)
        self.assertEqual([], updateResults)
        self.assertEqual([], deleteResults)

    @mock.patch("requests.Session.post")
    def test_create_feature_fail(self, mock_post):
        res = {"error": {"code": "testcode", "message": "unittest description"}}

        self.mock_res.mock_response(200, json_data=res)

        mock_post.return_value = self.mock_res

        (
            updateResults,
            addResults,
            deleteResults,
        ) = self.gis_service.update_feature_layer(
            layer_id=1, to_create=[], to_update=[], to_delete=[]
        )

        self.assertEqual(None, updateResults)
        self.assertEqual(None, addResults)
        self.assertEqual(None, deleteResults)


if __name__ == "__main__":
    unittest.main()
