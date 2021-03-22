import base64
import json
import logging

import config
from field_mapper import FieldMapperService

# from gis_service import GISService

logging.getLogger().setLevel(logging.INFO)


def push_to_arcgis(request):
    """
    Unpack Pub/Sub request and process message.

    :param request: Request object

    :return: Request response
    :rtype: (str, int)
    """

    try:
        # envelope = json.loads(request.data.decode("utf-8"))
        # logging.info(envelope)
        # _bytes = base64.b64decode(envelope["message"]["data"])
        # _message = json.loads(_bytes)
        _message = json.loads(open("payload.json", "r").read())
    except Exception as e:
        logging.error(f"Extraction of subscription failed: {str(e)}")
        return "Service Unavailable", 503
    else:
        publish_message(data=_message)

    return "No Content", 204


def publish_message(data):
    """
    Publish the unpacked message towards an ArcGIS feature service.

    :param data: Data object
    :type data: dict
    """

    if not hasattr(config, "MAPPING_FIELDS"):
        logging.error("Function is missing required 'MAPPING_FIELDS' configuration")
        return

    # Create a list of mapped data
    formatted_data = FieldMapperService().get_mapped_data(data_object=data)

    if not formatted_data:
        logging.info("No data to be published towards ArcGIS")
        return

    print(json.dumps(formatted_data, indent=4))

    # # Publish data to GIS server
    # if len(formatted_data) > 0:
    #     gis_service = GISService()
    #
    #     for item in formatted_data:
    #         gis_service.add_object_to_feature_layer(item)


if __name__ == "__main__":
    push_to_arcgis(None)
