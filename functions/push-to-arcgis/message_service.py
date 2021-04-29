import logging
import sys

from attachment_service import AttachmentService
from field_mapper import FieldMapperService
from firestore_service import FirestoreService
from gis_service import GISService


class MessageService:
    def __init__(self, config):
        """Initiate function configuration"""

        self.config = config

        if (
            not self.config.mapping.fields
            or not self.config.mapping.coordinates.longitude
            or not self.config.mapping.coordinates.latitude
            or not self.config.mapping.id_field
            or not self.config.arcgis_auth
            or not self.config.arcgis_feature_service.url
            or not self.config.arcgis_feature_service.id
        ):
            logging.error("Function is missing required configuration")
            sys.exit(1)

        # Initiate import services
        self.attachment_service = AttachmentService()
        self.firestore_service = (
            FirestoreService(
                high_workload=self.config.high_workload,
                kind=self.config.arcgis_feature_service.id,
            )
            if self.config.existence_check.firestore
            else None
        )
        self.mapping_service = FieldMapperService(
            self.config.data_source, self.config.mapping.attachments
        )

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
            self.config.mapping.fields, self.config.mapping.coordinates
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
            self.config.arcgis_auth,
            self.config.arcgis_feature_service.url,
            self.config.arcgis_feature_service.id,
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

        if not features_updated and not features_created:
            return

        # Join lists
        edits_updated = self.right_join(edits_to_update, features_updated, "updated")
        edits_created = self.right_join(edits_to_create, features_created, "created")

        # Append entities to Firestore service
        if self.firestore_service:
            for entity_id in edits_created:
                self.firestore_service.set_entity(
                    entity_id,
                    {"objectId": edits_created[entity_id]["id"], "entityId": entity_id},
                )

        # Check for attachments
        if edits_with_attachment:
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
                logging.info(f"Uploaded {attachment_count} attachment(s)")

                gis_service.update_feature_layer(edits_to_update, [])

        if self.firestore_service:
            self.firestore_service.close()

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
                map_list=["attributes"].extend(
                    self.outer.config.mapping.id_field.split("/")
                ),
                field_config={},
            )
            # Check if feature already exists
            feature_id = self.get_existing_object_id(item_id)

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

                field_mapping = ["attributes"]
                field_mapping.extend(field.split("/"))

                # Add attachment ID to correct field
                item = self.outer.mapping_service.set_in_dict(
                    data=item, map_list=field_mapping, value=int(attachment_id)
                )
                attachment_count += 1

            if attachment_count > 0:
                return {
                    "id": feature_id,
                    "attachment_count": attachment_count,
                    "object": item,
                }

            return None

        def get_existing_object_id(self, id_value):
            """
            Check if feature already exist

            :param id_value: ID value

            :return: Feature ID
            :rtype: int
            """

            if self.outer.config.existence_check.arcgis:
                return self.gis_service.get_object_id_in_feature_layer(
                    self.outer.config.mapping.id_field, id_value
                )

            if self.outer.config.existence_check.firestore:
                return self.get_existing_object_id_in_firestore(id_value)

            if self.outer.config.existence_check.value:
                logging.error(
                    f"The existence check value '{self.outer.config.existence_check.value}' is not supported, "
                    "supported types: 'arcgis', 'firestore'"
                )

            return None

        def get_existing_object_id_in_firestore(self, id_value):
            """
            Check if feature already exist within Firestore database

            :param id_value: ID value

            :return: Feature ID
            :rtype: int
            """

            entity = self.outer.firestore_service.get_entity(id_value)

            if entity and "objectId" in entity:
                return entity["objectId"]

            return None
