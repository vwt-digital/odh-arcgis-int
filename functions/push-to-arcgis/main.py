import base64
import json
import logging

from functions.common.configuration import Configuration
from message_service import MessageService

config = Configuration()

logging.getLogger().setLevel(logging.DEBUG if config.debug_logging else logging.INFO)
message_service = MessageService(config)


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
