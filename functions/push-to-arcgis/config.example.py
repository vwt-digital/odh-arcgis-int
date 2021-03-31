ARCGIS_AUTHENTICATION = {
    "url": "example.com",
    "username": "user_1",
    "secret": "gcp-secret-name",
    "request": "gettoken",
    "referer": "example.com",
}
ARCGIS_FEATURE_URL = "example.com"
ARCGIS_FEATURE_ID = "example"

EXISTENCE_CHECK = "firestore"

MAPPING_ID_FIELD = "attributes/id"
MAPPING_DATA_SOURCE = "nested/data"
MAPPING_ATTACHMENT_FIELDS = ["attributes/profile_picture"]
MAPPING_FIELD_CONFIG = {
    "attributes": {
        "_items": {
            "id": {"field": "properties/id", "required": True},
            "name": {"field": "properties/user/name", "required": False},
            "profile_picture": {
                "field": "properties/user/photo",
                "required": True,
            },
        }
    }
}
