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
HIGH_WORKLOAD = True
MESSAGE_DATA_SOURCE = "nested/data"

MAPPING_ID_FIELD = "id"
MAPPING_ATTACHMENTS = ["profile_picture"]
MAPPING_COORDINATES_LON = "geometry/coordinates/0"
MAPPING_COORDINATES_LAT = "geometry/coordinates/1"
MAPPING_ATTRIBUTES = {
    "id": {"field": "properties/id", "required": True},
    "name": {"field": "properties/user/name", "required": False},
    "profile_picture": {
        "field": "properties/user/photo",
        "required": True,
    },
}
