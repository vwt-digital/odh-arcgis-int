import logging
import sys

import config
from attachment_service import AttachmentService
from field_mapper import FieldMapperService
from firestore_service import FirestoreService
from gis_service import GISService


class MessageService:
    def __init__(self):
        """Initiate function configuration"""

        if (
            not hasattr(config, "MAPPING_ATTRIBUTES")
            or not hasattr(config, "MAPPING_COORDINATES")
            or not hasattr(config, "MAPPING_ID_FIELD")
            or not hasattr(config, "ARCGIS_AUTHENTICATION")
            or not hasattr(config, "ARCGIS_FEATURE_URL")
            or not hasattr(config, "ARCGIS_FEATURE_ID")
        ):
            logging.error("Function is missing required configuration")
            sys.exit(1)

        # Retrieve current mapping configuration
        self.mapping_attributes = config.MAPPING_ATTRIBUTES  # Required configuration
        self.mapping_coordinates = config.MAPPING_COORDINATES  # Required configuration
        self.mapping_id_field = config.MAPPING_ID_FIELD  # Required configuration
        mapping_attachments = (
            config.MAPPING_ATTACHMENTS
            if hasattr(config, "MAPPING_ATTACHMENTS")
            else None
        )

        # Retrieve ArcGIS configuration
        self.arcgis_auth = config.ARCGIS_AUTHENTICATION  # Required configuration
        self.arcgis_url = config.ARCGIS_FEATURE_URL  # Required configuration
        self.arcgis_name = config.ARCGIS_FEATURE_ID  # Required configuration

        # Retrieve other configuration
        message_data_source = (
            config.MESSAGE_DATA_SOURCE
            if hasattr(config, "MESSAGE_DATA_SOURCE")
            else None
        )
        self.existence_check = (
            config.EXISTENCE_CHECK if hasattr(config, "EXISTENCE_CHECK") else None
        )
        high_workload = (
            config.HIGH_WORKLOAD if hasattr(config, "HIGH_WORKLOAD") else False
        )

        self.firestore_service = (
            FirestoreService(high_workload=high_workload, kind=self.arcgis_name)
            if self.existence_check == "firestore"
            else None
        )
        self.attachment_service = AttachmentService()  # Initiate attachment service
        self.mapping_service = FieldMapperService(
            message_data_source, mapping_attachments
        )  # Initiate mapper service

    def process(self, data):
        """
        Processes data from incoming message

        :param data: Incoming data
        :type data: dict

        :return: HTTP response
        :rtype: (str, int)
        """

        # Create mapping service and retrieve ArcGIS object mapping
        mapping_fields = self.mapping_service.get_mapping(
            self.mapping_attributes, self.mapping_coordinates
        )

        # Retrieve mapped data
        formatted_data = self.mapping_service.get_mapped_data(
            data_object=data, mapping_fields=mapping_fields
        )
        if not formatted_data:
            logging.info("No data to be published towards ArcGIS")
            return "No Content", 204

        # Create ArcGIS service
        gis_service = GISService(
            self.arcgis_auth, self.arcgis_url, self.arcgis_name, self.firestore_service
        )
        item_processor = self.ItemProcessor(outer=self, gis_service=gis_service)

        for item in formatted_data:
            if not item_processor.process(
                item
            ):  # Break execution if feature could not be processed
                return "Service Unavailable", 503

        gis_service.close_service()

        return "No Content", 204

    class ItemProcessor:
        def __init__(self, outer, gis_service):
            """
            Initiates item processor

            :param outer: The outer service
            :param gis_service: GIS Service
            """

            self.outer = outer
            self.gis_service = gis_service

        def process(self, item):
            """
            Processes single item

            :param item: Item
            :type item: dict

            :return: Feature is successfully processed
            :rtype: boolean
            """

            # Extract attachments from object
            item, item_attachments = self.outer.mapping_service.extract_attachments(
                data_object=item
            )
            item_id = self.outer.mapping_service.get_from_dict(
                data=item,
                map_list=["attributes", self.outer.mapping_id_field],
                field_config={},
            )
            # Check if feature already exists
            feature_id = self.gis_service.get_existing_object_id(
                self.outer.existence_check, self.outer.mapping_id_field, item_id
            )

            # If feature exists update this, otherwise add new feature
            if feature_id:
                feature_id = self.gis_service.update_object_to_feature_layer(
                    item, feature_id
                )
            else:
                feature_id = self.gis_service.add_object_to_feature_layer(item, item_id)

            if not feature_id:
                return False

            # Upload attachments and update feature
            if len(item_attachments) > 0:
                self.process_attachments(feature_id, item, item_attachments)

            return True

        def process_attachments(self, feature_id, item, item_attachments):
            """
            Process item attachments
            """

            logging.info(f"Found {len(item_attachments)} attachments to upload")
            updated_attachment = False

            for field in item_attachments:
                # Get attachment content
                file_type, file_name, file_content = self.outer.attachment_service.get(
                    item_attachments[field]
                )

                if not file_content:
                    continue

                # Upload attachment to feature object
                attachment_id = self.gis_service.upload_attachment_to_feature_layer(
                    feature_id, file_type, file_name, file_content
                )

                if not attachment_id:
                    continue

                # Add attachment ID to correct field
                item = self.outer.mapping_service.set_in_dict(
                    data=item, map_list=field.split("/"), value=int(attachment_id)
                )
                updated_attachment = True

            # Update feature object with attachment IDs
            if updated_attachment:
                self.gis_service.update_object_to_feature_layer(item, feature_id)
