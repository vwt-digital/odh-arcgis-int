GIS_FEATURE_SERVICE_AUTHENTICATION = {
    "url": "",
    "username": "",
    "secret": "",
    "request": "",
    "referer": "",
}
GIS_FEATURE_SERVICE = "https://example.com/"

MAPPING_DATA_SOURCE = "nested/data"
MAPPING_ATTACHMENTS = ["attributes/profile_picture"]
FIELD_MAPPING = {
    "attributes": {
        "_items": {
            "id": {"field": "properties/id", "required": True},
            "name": {"field": "properties/user/name", "required": False},
            "profile_picture": {"field": "properties/user/photo", "required": True},
        }
    }
}
