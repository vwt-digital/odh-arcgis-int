import base64
import json
import logging

import config
from cloud_logging import setup_cloud_logging
from message_service import MessageService

setup_cloud_logging()  # Setup Cloud Logging integration
logging.getLogger().setLevel(
    logging.DEBUG
    if hasattr(config, "DEBUG_LOGGING") and config.DEBUG_LOGGING
    else logging.INFO
)
message_service = MessageService()


def push_to_arcgis(request):
    """
    Unpack Pub/Sub request and process message.

    :param request: Request object

    :return: Request response
    :rtype: (str, int)
    """

    try:
        envelope = json.loads(request.data.decode("utf-8"))
        logging.debug(envelope)
        _bytes = base64.b64decode(envelope["message"]["data"])
        _message = json.loads(_bytes)
    except Exception as e:
        logging.error(f"Extraction of subscription failed: {str(e)}")
        return "Service Unavailable", 503
    else:
        resp = message_service.process(data=_message)
        return resp


if __name__ == "__main__":
    push_to_arcgis(None)
