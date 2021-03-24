GIS_FEATURE_SERVICE_AUTHENTICATION = {
    "url": "",
    "username": "",
    "secret": "",
    "request": "",
    "referer": "",
}
GIS_FEATURE_SERVICE = "https://example.com/"

MAPPING = {
    "pubsub-subscription-1": {
        "data_source": "nested/data",
        "attachment_fields": ["attributes/profile_picture"],
        "mapping": {
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
        },
    },
    "pubsub-subscription-2": {
        "data_source": "nested/data",
        "mapping": {
            "attributes": {
                "_items": {
                    "id": {"field": "properties/id", "required": True},
                    "name": {"field": "properties/user/name", "required": False},
                }
            }
        },
    },
}
