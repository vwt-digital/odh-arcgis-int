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
            or not self.config.mapping.coordinates.conversion
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
            self.config.data_source,
            self.config.mapping.attachments,
            self.config.mapping.coordinates.conversion,
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
            self.config.mapping.disable_updated_at,
        )

        if not gis_service.token:
            return "Service Unavailable", 503

        # Create Item Processor
        self.item_processor = self.ItemProcessor(outer=self, gis_service=gis_service)

        # Prepare data for ArcGIS interaction
        edits, edits_with_attachment = self.format_data_into_edits(formatted_data)
        arcgis_data = self.prepare_edits_for_update(edits)

        # Publish data to ArcGIS
        edits_updated, edits_created, edits_deleted = self.publish_data_to_arcgis(
            arcgis_data, gis_service
        )

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
        Publish formatted data to ArcGIS and merge results

        :param edits: Edits to be made
        :type edits: dict
        :param gis_service: GIS Service

        :return: Edits updated, created and deleted
        :rtype: (dict, dict, dict)
        """

        edits_created, edits_deleted, edits_updated = self.publish_edits(
            edits, gis_service
        )

        if (
            not edits_updated["objects"]
            and not edits_created["objects"]
            and not edits_deleted["objects"]
        ):
            return None, None, None

        if edits_updated["count"]:
            logging.info(
                f"Updated existing features in layers: {dict_to_string(edits_updated['count'])}"
            )

        if edits_created["count"]:
            logging.info(
                f"Created new features in layers: {dict_to_string(edits_created['count'])}"
            )

        if edits_deleted["count"]:
            logging.info(
                f"Deleted existing features in layers: {dict_to_string(edits_deleted['count'])}"
            )

        # Append entities to Firestore service
        if self.firestore_service:
            for entity_id in edits_created["objects"]:
                self.firestore_service.set_entity(
                    entity_id,
                    {
                        "entityId": entity_id,
                        "layerId": edits_created["objects"][entity_id]["layer_id"],
                        "objectId": edits_created["objects"][entity_id]["id"],
                    },
                )

        return (
            edits_updated["objects"],
            edits_created["objects"],
            edits_deleted["objects"],
        )

    def publish_edits(self, edits, gis_service):
        """
        Publish the edits towards ArcGIS

        :param edits: Edits to be made
        :type edits: dict
        :param gis_service: GIS Service

        :return: Edits updated, created and deleted
        :rtype: (dict, dict, dict)
        """

        edits_updated = {"objects": {}, "count": {}}
        edits_created = {"objects": {}, "count": {}}
        edits_deleted = {"objects": {}, "count": {}}

        for layer_id in sorted(edits):
            (
                features_updated,
                features_created,
                features_deleted,
            ) = gis_service.update_feature_layer(
                layer_id,
                edits[layer_id]["to_update"],
                edits[layer_id]["to_create"],
                edits[layer_id]["to_delete"],
            )

            if features_updated and len(features_updated) > 0:
                edits_updated["count"][layer_id] = len(features_updated)
                edits_updated["objects"] = {
                    **edits_updated["objects"],
                    **self.right_join(
                        edits[layer_id]["to_update"], features_updated, "updated"
                    ),
                }  # Join data

            if features_created and len(features_created) > 0:
                edits_created["count"][layer_id] = len(features_created)
                edits_created["objects"] = {
                    **edits_created["objects"],
                    **self.right_join(
                        edits[layer_id]["to_create"], features_created, "created"
                    ),
                }  # Join data

            if features_deleted and len(features_deleted) > 0:
                edits_deleted["count"][layer_id] = len(features_deleted)
                edits_deleted["objects"] = {
                    **edits_deleted["objects"],
                    **self.right_join(
                        edits[layer_id]["to_delete"], features_deleted, "deleted"
                    ),
                }  # Join data

        return edits_created, edits_deleted, edits_updated

    def format_data_into_edits(self, formatted_data):
        """
        Format data into layers

        :param formatted_data: Formatted data
        :type formatted_data: list

        :return: ArcGIS edits, ArcGIS edits with attachments
        :rtype: (list, dict)
        """

        edits = []
        edits_with_attachment = {}

        for item in formatted_data:
            (
                layer_id,
                item_obj,
                item_attachments,
                item_id,
            ) = self.item_processor.extract_data(item)

            if layer_id is None:  # Skip message if it does not have a valid layer ID
                continue

            edits.append({"item_id": item_id, "layer_id": layer_id, "object": item_obj})

            if item_attachments:
                edits_with_attachment[item_id] = item_attachments

        return edits, edits_with_attachment

    def prepare_edits_for_update(self, edits):
        """
        Prepare edits for update

        :param edits: ArcGIS edits
        :type edits: list

        :return: ArcGIS edits
        :rtype: dict
        """

        layers_to_check = (
            self.config.arcgis_feature_service.layers
            if self.config.arcgis_feature_service.layers
            else [0]
        )
        edits_to_match = list(set([edit["item_id"] for edit in edits]))
        layer_edits = {}

        # Match features on multiple layers
        edits_matched = self.match_existing_features(edits_to_match, layers_to_check)

        # Check for each edit if an update, creation or deletion is needed
        for edit in edits:
            edit_item_id = edit["item_id"]
            edit_layer_id = int(edit["layer_id"])
            edit_object_id = None

            edit_update_type = "to_create"  # Default update type is creation

            # Check if existing features are needed for update
            for item in edits_matched.get(edit_item_id, []):
                item_layer_id = int(item["layer_id"])
                item_object_id = int(item["object_id"])

                # Check if item is already in new layer ID and use it for an update
                if item_layer_id == edit_layer_id:
                    edit_object_id = item_object_id
                    edit["object"]["attributes"][
                        "objectid"
                    ] = item_object_id  # Set ObjectID in edit
                    edit_layer_id = item_layer_id
                    edit_update_type = "to_update"
                    continue

                # Check if item is not already used to update
                if edit_object_id != item_object_id:
                    if (
                        item_layer_id not in layer_edits
                    ):  # Check if layer already has edits
                        layer_edits[item_layer_id] = {
                            "to_update": [],
                            "to_create": [],
                            "to_delete": [],
                        }

                    # Append feature to deletion list as it is not needed anymore
                    layer_edits[item_layer_id]["to_delete"].append(
                        {
                            "item_id": edit_item_id,
                            "layer_id": item_layer_id,
                            "object": item,
                            "objectId": item_object_id,
                        }
                    )

            if edit_layer_id not in layer_edits:  # Check if layer already has edits
                layer_edits[edit_layer_id] = {
                    "to_update": [],
                    "to_create": [],
                    "to_delete": [],
                }

            # Append edit to layer with correct update type: 'to_update' or 'to_create'
            layer_edits[edit_layer_id][edit_update_type].append(edit)

        return layer_edits

    def match_existing_features(self, edit_ids, layer_ids):
        """
        Match existing features

        :param edit_ids: Edit IDs
        :type edit_ids: list
        :param layer_ids: ArcGIS layers
        :type layer_ids: list

        :return: Matched edits
        :rtype: dict
        """

        edits_matched = {}

        for layer_id in layer_ids:
            object_ids = self.item_processor.get_existing_object_id(layer_id, edit_ids)

            for item_id in object_ids:
                if item_id not in edits_matched:
                    edits_matched[item_id] = []

                edits_matched[item_id].append(
                    {
                        "layer_id": layer_id,
                        "object_id": object_ids[item_id],
                    }
                )
                edit_ids.remove(item_id)

            if not edit_ids:
                break

        return edits_matched

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
                    layer_id, edits_to_update[layer_id], [], []
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
                "id": item["objectId"] if isinstance(item, dict) else item,
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

        def extract_data(self, item):
            """
            Extract data from item

            :param item: Item
            :type item: dict

            :return: Layer ID, item data, item attachments, item ID
            :rtype: (int, dict, dict, int)
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

            return layer_id, item_data, item_attachments, item_id

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

        def get_existing_object_id(self, layer_id, id_values):
            """
            Check if feature already exist

            :param layer_id: Layer ID
            :type layer_id: int
            :param id_values: ID values
            :type id_values: list

            :return: Feature ID
            :rtype: int
            """

            if self.outer.config.existence_check.arcgis:
                return self.gis_service.get_objectids_in_feature_layer(
                    layer_id, self.outer.config.mapping.id_field, id_values
                )

            if self.outer.config.existence_check.firestore:
                return self.get_existing_objectids_in_firestore(id_values)

            if self.outer.config.existence_check.value:
                logging.error(
                    f"The existence check value '{self.outer.config.existence_check.value}' is not supported, "
                    "supported types: 'arcgis', 'firestore'"
                )

            return None

        def get_existing_objectids_in_firestore(self, id_values):
            """
            Check if feature already exist within Firestore database

            :param id_values: ID values
            :type id_values: list

            :return: Feature ID
            :rtype: int
            """

            entities = self.outer.firestore_service.get_entities(id_values)

            feature_ids = {}
            for entity in entities:
                feature_id = entity["entityId"]
                object_id = entity["objectId"]

                feature_ids[feature_id] = object_id

            return feature_ids


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
