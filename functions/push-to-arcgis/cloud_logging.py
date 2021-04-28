import os

from google.cloud import logging_v2 as cloudlogging


def setup_cloud_logging():
    log_name = "cloudfunctions.googleapis.com%2Fcloud-functions"
    res = cloudlogging.Resource(
        type="cloud_function",
        labels={
            "function_name": os.environ.get("K_SERVICE", None),
            "project_id": os.environ.get("PROJECT_ID", None),
            "region": os.environ.get("REGION", None),
        },
    )

    client = cloudlogging.Client()
    handler = cloudlogging.handlers.CloudLoggingHandler(
        client,
        name=log_name.format(res.labels["function_name"]),
        resource=res,
        labels=res.labels,
    )
    cloudlogging.handlers.setup_logging(handler)
