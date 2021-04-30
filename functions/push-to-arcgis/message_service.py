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
            data_object=data,
            mapping_fields=mapping_fields,
            layer_field=self.config.mapping.layer_field,
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

        # Create Item Processor
        self.item_processor = self.ItemProcessor(outer=self, gis_service=gis_service)

        # Divide data
        edits, edits_with_attachment = self.divide_data(formatted_data)

        # Publish data to ArcGIS
        edits_updated, edits_created = self.publish_data_to_arcgis(edits, gis_service)

        # Check for attachments and publish them
        if edits_with_attachment:
            self.publish_attachments_to_arcgis(
                edits_done={**edits_updated, **edits_created},
                edits_with_attachment=edits_with_attachment,
                gis_service=gis_service,
            )

        if self.firestore_service:  # Close Firestore Service is available
            self.firestore_service.close()

        return "No Content", 204

    def publish_data_to_arcgis(self, edits, gis_service):
        """
        Publish formatted data to ArcGIS

        :param edits: Edits to be made
        :type edits: dict
        :param gis_service: GIS Service

        :return: Edits updated and created
        :rtype: (dict, dict)
        """

        edits_updated = {}
        edits_created = {}

        updated_layer_count = {}
        created_layer_count = {}

        for layer_id in edits:
            features_updated, features_created = gis_service.update_feature_layer(
                layer_id, edits[layer_id]["to_update"], edits[layer_id]["to_create"]
            )

            if features_updated and len(features_updated) > 0:
                updated_layer_count[layer_id] = len(features_updated)
                edits_updated = {
                    **edits_updated,
                    **self.right_join(
                        edits[layer_id]["to_update"], features_updated, "updated"
                    ),
                }  # Join data

            if features_created and len(features_created) > 0:
                created_layer_count[layer_id] = len(features_created)
                edits_created = {
                    **edits_created,
                    **self.right_join(
                        edits[layer_id]["to_create"], features_created, "created"
                    ),
                }  # Join data

        if not edits_updated and not edits_created:
            return

        if updated_layer_count:
            logging.info(
                f"Updated existing features in layers: {dict_to_string(updated_layer_count)}"
            )

        if created_layer_count:
            logging.info(
                f"Created new features in layers: {dict_to_string(created_layer_count)}"
            )

        # Append entities to Firestore service
        if self.firestore_service:
            for entity_id in edits_created:
                self.firestore_service.set_entity(
                    entity_id,
                    {
                        "entityId": entity_id,
                        "objectId": edits_created[entity_id]["id"],
                    },
                )

        return edits_updated, edits_created

    def divide_data(self, formatted_data):
        """
        Divide data into edits to update, to create and with attachments

        :param formatted_data: Formatted data
        :type formatted_data: list

        :return: Edits to update and create, Edits with attachments
        :rtype: (dict, dict)
        """

        edits = {}
        edits_with_attachment = {}

        for item in formatted_data:
            (
                layer_id,
                feature_id,
                item_obj,
                item_attachments,
                item_id,
            ) = self.item_processor.process(item)

            if layer_id is None:  # Skip message if it does not have a valid layer ID
                continue

            if layer_id not in edits:
                edits[layer_id] = {"to_update": [], "to_create": []}

            item_dict = {"item_id": item_id, "layer_id": layer_id, "object": item_obj}

            if feature_id:
                item_dict["object"]["attributes"]["objectid"] = feature_id
                edits[layer_id]["to_update"].append(item_dict)
            else:
                edits[layer_id]["to_create"].append(item_dict)

            if item_attachments:
                edits_with_attachment[item_id] = item_attachments

        return edits, edits_with_attachment

    def publish_attachments_to_arcgis(
        self, edits_done, edits_with_attachment, gis_service
    ):
        """
        Publish attachments to ArcGIS

        :param edits_done: Edits done
        :type edits_done: dict
        :param edits_with_attachment: Edits with attachments
        :type edits_with_attachment: dict
        :param gis_service: GIS Service
        """

        edits_to_update = {}

        for item_id in edits_with_attachment:
            if item_id in edits_done:
                layer_id = edits_done[item_id]["layer_id"]
                feature_id = edits_done[item_id]["id"]
                feature_data = edits_done[item_id]["data"]
                feature_attachments = edits_with_attachment[item_id]

                feature_data_updated = self.item_processor.process_attachments(
                    layer_id, feature_id, feature_data, feature_attachments
                )

                if feature_data_updated:
                    if layer_id not in edits_to_update:
                        edits_to_update[layer_id] = []

                    edits_to_update[layer_id].append(feature_data_updated)

        # Update features with new attachments
        if edits_to_update:
            attachment_count = self.count_total_uploaded_attachments(edits_to_update)
            logging.info(f"Uploaded {attachment_count} attachment(s)")

            for layer_id in edits_to_update:
                gis_service.update_feature_layer(
                    layer_id, edits_to_update[layer_id], []
                )

    @staticmethod
    def count_total_uploaded_attachments(edits_to_update):
        """
        Count total attachments within edits

        :param edits_to_update: Edits to update
        :type edits_to_update: dict

        :return: Total attachments
        :rtype: int
        """

        attachments_uploaded = 0

        for layer_id in edits_to_update:
            for edit in edits_to_update[layer_id]:
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
                "layer_id": list_1[index]["layer_id"],
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

            :return: Layer ID, feature ID, item data, item attachments, item ID
            :rtype: (int, int, dict, dict, int)
            """

            field_mapping = ["attributes"]
            field_mapping.extend(self.outer.config.mapping.id_field.split("/"))

            # Extract attachments from object
            (
                item_data,
                item_attachments,
            ) = self.outer.mapping_service.extract_attachments(data_object=item["data"])
            item_id = self.outer.mapping_service.get_from_dict(
                data=item_data,
                map_list=field_mapping,
                field_config={},
            )

            # Retrieve feature's layer ID
            layer_id = self.get_layer_id(layer_id=item["layer_id"], item_id=item_id)

            # Check if feature already exists
            feature_id = self.get_existing_object_id(layer_id, item_id)

            return layer_id, feature_id, item_data, item_attachments, item_id

        def process_attachments(self, layer_id, feature_id, item, item_attachments):
            """
            Process item attachments

            :param layer_id: Layer ID
            :type layer_id: int
            :param feature_id: Feature ID
            :type feature_id: int
            :param item: Item data
            :type item: dict
            :param item_attachments: Item's attachments
            :type item_attachments: dict

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
                    layer_id, feature_id, file_type, file_name, file_content
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

        def get_layer_id(self, layer_id, item_id):
            """
            Return item's layer ID

            :param layer_id: Item layer ID
            :type layer_id: int
            :param item_id: Item ID
            :type item_id: str

            :return: Layer ID
            :rtype: int
            """

            # Retrieve layer configuration
            layer_config = self.outer.config.arcgis_feature_service.layers

            # Return the item layer ID if existing
            if layer_id:
                if layer_config and layer_id not in layer_config:
                    logging.error(
                        f"Message '{item_id}' contains a not defined layer ID '{layer_id}', skipping this"
                    )
                    return None

                return int(layer_id)

            # Return first configuration layer ID if existing
            if layer_config:
                return int(layer_config[0])

            # Return default layer ID '0'
            return 0

        def get_existing_object_id(self, layer_id, id_value):
            """
            Check if feature already exist

            :param layer_id: Layer ID
            :type layer_id: int
            :param id_value: ID value

            :return: Feature ID
            :rtype: int
            """

            if self.outer.config.existence_check.arcgis:
                return self.gis_service.get_object_id_in_feature_layer(
                    layer_id, self.outer.config.mapping.id_field, id_value
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


def dict_to_string(data):
    """
    Transform dict to simple string

    :param data: Dictionary
    :type data: dict

    :return: Data in string format
    :rtype: str
    """

    string = []

    for item, value in data.items():
        string.append(f"{item} ({value})")

    return ", ".join(string)
