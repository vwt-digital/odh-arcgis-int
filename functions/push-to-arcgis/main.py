import base64
import json
import logging

import config
from attachment_service import AttachmentService
from field_mapper import FieldMapperService
from gis_service import GISService

logging.getLogger().setLevel(logging.INFO)
attachment_service = AttachmentService()


def push_to_arcgis(request):
    """
    Unpack Pub/Sub request and process message.

    :param request: Request object

    :return: Request response
    :rtype: (str, int)
    """

    try:
        envelope = json.loads(request.data.decode("utf-8"))
        logging.info(envelope)
        _subscription = envelope["subscription"].split("/")[-1]
        _bytes = base64.b64decode(envelope["message"]["data"])
        _message = json.loads(_bytes)
    except Exception as e:
        logging.error(f"Extraction of subscription failed: {str(e)}")
        return "Service Unavailable", 503
    else:
        resp = process(data=_message, subscription=_subscription)
        return resp


def process(data, subscription):
    """
    Process the message.

    :param data: Data object
    :type data: dict
    :param subscription: Pub/Sub Subscription name
    :type subscription: str
    """

    if (
        not hasattr(config, "MAPPING_FIELD_CONFIG")
        or not hasattr(config, "ARCGIS_AUTHENTICATION")
        or not hasattr(config, "ARCGIS_FEATURE_URL")
    ):
        logging.error(
            f"Function is missing required configuration for subscription '{subscription}'"
        )
        return "Bad Gateway", 502

    # Retrieve current mapping configuration
    mapping_fields = config.MAPPING_FIELD_CONFIG  # Required configuration
    mapping_data_source = (
        config.MAPPING_DATA_SOURCE if hasattr(config, "MAPPING_DATA_SOURCE") else None
    )
    mapping_attachments = (
        config.MAPPING_ATTACHMENT_FIELDS
        if hasattr(config, "MAPPING_ATTACHMENT_FIELDS")
        else None
    )
    arcgis_auth = config.ARCGIS_AUTHENTICATION  # Required configuration
    arcgis_url = config.ARCGIS_FEATURE_URL  # Required configuration

    # Create a list of mapped data
    mapping_service = FieldMapperService(
        mapping_fields, mapping_data_source, mapping_attachments
    )
    formatted_data = mapping_service.get_mapped_data(data_object=data)

    if not formatted_data:
        logging.info("No data to be published towards ArcGIS")
        return "No Content", 204

    gis_service = GISService(arcgis_auth, arcgis_url)

    # Publish data to GIS server
    for item in formatted_data:
        # Extract attachments from object
        item, item_attachments = mapping_service.extract_attachments(data_object=item)

        # # Upload feature to ArcGIS
        feature_id = gis_service.add_object_to_feature_layer(item)

        # Upload attachments and update feature
        if len(item_attachments) > 0:
            logging.info(f"Found {len(item_attachments)} attachments to upload")
            updated_attachment = False

            for field in item_attachments:
                # Get attachment content
                file_type, file_name, file_content = attachment_service.get(
                    item_attachments[field]
                )

                if not file_content:
                    continue

                # Upload attachment to feature object
                attachment_id = gis_service.upload_attachment_to_feature_layer(
                    feature_id, file_type, file_name, file_content
                )

                # Add attachment ID to correct field
                item = mapping_service.set_in_dict(
                    data=item, map_list=field.split("/"), value=int(attachment_id)
                )
                updated_attachment = True

            # Update feature object with attachment IDs
            if updated_attachment:
                gis_service.update_object_to_feature_layer(item, feature_id)

    return "No Content", 204


if __name__ == "__main__":
    push_to_arcgis(None)
