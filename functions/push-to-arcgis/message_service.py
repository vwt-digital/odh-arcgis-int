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

        self.item_processor = None

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

        if not gis_service.token:
            return "Service Unavailable", 503

        # Publish data to ArcGIS
        self.publish_data_to_arcgis(formatted_data, gis_service)

        return "No Content", 204

    def publish_data_to_arcgis(self, formatted_data, gis_service):
        """
        Publish formatted data to ArcGIS

        :param formatted_data: Formatted data
        :type formatted_data: list
        :param gis_service: GIS Service
        """

        # Create Item Processor
        self.item_processor = self.ItemProcessor(outer=self, gis_service=gis_service)

        # Divide data and publish to ArcGIS
        edits_to_update, edits_to_create, edits_with_attachment = self.divide_data(
            formatted_data
        )

        features_updated, features_created = gis_service.update_feature_layer(
            edits_to_update, edits_to_create
        )

        # Check for attachments
        if edits_with_attachment:
            # Join lists
            edits_updated = self.right_join(
                edits_to_update, features_updated, "updated"
            )
            edits_created = self.right_join(
                edits_to_create, features_created, "created"
            )
            edits_done = {**edits_updated, **edits_created}

            # Publish attachments
            edits_to_update = self.publish_attachments_to_arcgis(
                edits_done, edits_with_attachment
            )

            # Update features with new attachments
            if edits_to_update:
                attachment_count = self.count_total_uploaded_attachments(
                    edits_to_update
                )
                logging.info(f"Uploaded {attachment_count} attachments")

                gis_service.update_feature_layer(edits_to_update, [])

        gis_service.close_service()

    def divide_data(self, formatted_data):
        """
        Divide data into edits to update, to create and with attachments

        :param formatted_data: Formatted data
        :type formatted_data: list

        :return: Edits to update, to create and with attachments
        :rtype: (list, list, dict)
        """

        edits_to_update = []
        edits_to_create = []
        edits_with_attachment = {}

        for item in formatted_data:
            (
                feature_id,
                item_obj,
                item_attachments,
                item_id,
            ) = self.item_processor.process(item)

            if feature_id:
                item_obj["attributes"]["objectid"] = feature_id
                edits_to_update.append({"item_id": item_id, "object": item_obj})
            else:
                edits_to_create.append({"item_id": item_id, "object": item_obj})

            if item_attachments:
                edits_with_attachment[item_id] = item_attachments

        return edits_to_update, edits_to_create, edits_with_attachment

    def publish_attachments_to_arcgis(self, edits_done, edits_with_attachment):
        """
        Publish attachments to ArcGIS

        :param edits_done: Edits done
        :type edits_done: dict
        :param edits_with_attachment: Edits with attachments
        :type edits_with_attachment: dict

        :return: Edits to update
        :rtype: list
        """

        edits_to_update = []

        for item_id in edits_with_attachment:
            if item_id in edits_done:
                feature_id = edits_done[item_id]["id"]
                feature_data = edits_done[item_id]["data"]
                feature_attachments = edits_with_attachment[item_id]

                feature_data_updated = self.item_processor.process_attachments(
                    feature_id, feature_data, feature_attachments
                )

                if feature_data_updated:
                    edits_to_update.append(feature_data_updated)

        return edits_to_update

    @staticmethod
    def count_total_uploaded_attachments(edits_to_update):
        """
        Count total attachments within edits

        :param edits_to_update: Edits to update
        :type edits_to_update: list

        :return: Total attachments
        :rtype: int
        """
        attachments_uploaded = 0

        for edit in edits_to_update:
            attachments_uploaded += edit["attachment_count"]

        return attachments_uploaded

    @staticmethod
    def right_join(list_1, list_2, list_type):
        """
        Right joint two lists to one dict

        :param list_1: List 1
        :type list_1: list
        :param list_2: List 2
        :type list_2: list
        :param list_type: List type
        :type list_type: str

        :return: Joined dict
        :rtype: dict
        """

        list_to_dict = {}

        for index, item in enumerate(list_2):
            list_to_dict[list_1[index]["item_id"]] = {
                "data": list_1[index]["object"],
                "id": item["objectId"],
                "type": list_type,
            }

        return list_to_dict

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

            return feature_id, item, item_attachments, item_id

        def process_attachments(self, feature_id, item, item_attachments):
            """
            Process item attachments

            :return: Updated item
            :rtype: dict
            """

            logging.debug(
                f"Found {len(item_attachments)} attachments to upload for feature {feature_id}"
            )
            attachment_count = 0

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
                attachment_count += 1

            if attachment_count > 0:
                return {
                    "id": feature_id,
                    "attachment_count": attachment_count,
                    "object": item,
                }

            return None
