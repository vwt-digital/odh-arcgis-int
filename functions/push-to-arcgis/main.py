import base64
import json
import logging

from google.cloud import logging as cloudlogging
from message_service import MessageService

lg_client = cloudlogging.Client()
lg_client.get_default_handler()
lg_client.setup_logging(log_level=logging.INFO)

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
