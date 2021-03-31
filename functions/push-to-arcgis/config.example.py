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
MESSAGE_DATA_SOURCE = "nested/data"

MAPPING_ID_FIELD = "id"
MAPPING_ATTACHMENTS = ["profile_picture"]
MAPPING_COORDINATES = "geometry/coordinates"
MAPPING_ATTRIBUTES = {
    "id": {"field": "properties/id", "required": True},
    "name": {"field": "properties/user/name", "required": False},
    "profile_picture": {
        "field": "properties/user/photo",
        "required": True,
    },
}
