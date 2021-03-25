MAPPING = {
    "pubsub-subscription-1": {
        "arcgis": {
            "authentication": {
                "url": "example.com",
                "username": "user_1",
                "secret": "gcp-secret-name",
                "request": "gettoken",
                "referer": "example.com",
            },
            "feature_url": "example.com",
        },
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
        "arcgis": {
            "authentication": {
                "url": "example.com",
                "username": "user_2",
                "secret": "gcp-secret-name",
                "request": "gettoken",
                "referer": "example.com",
            },
            "feature_url": "example.com",
        },
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
