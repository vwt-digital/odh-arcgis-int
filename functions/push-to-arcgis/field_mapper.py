import logging
import operator
from functools import reduce

import config


class FieldMapperService:
    @staticmethod
    def transform_value(field_name, field_config, value):
        """
        Transform a value based on configuration

        :param field_name: Name of the current field
        :type field_name: str
        :param field_config: Configuration for transformation
        :type field_config: dict
        :param value: Value to transform

        :return: Transformed value
        """

        try:
            if "list_item" in field_config:
                value = value[int(field_config["list_item"])]

            if (
                "character_set" in field_config
                and len(field_config["character_set"]) == 2
            ):
                character_start = field_config["character_set"][0]
                character_end = field_config["character_set"][1]

                if character_start is None:
                    value = value[: int(character_end)]
                elif character_end is None:
                    value = value[int(character_start) :]
                else:
                    value = value[int(character_start) : int(character_end)]
        except (KeyError, AttributeError, TypeError, IndexError):
            value = None
        finally:
            if field_config.get("required", False) and not value:
                raise ValueError(f"Required field '{field_name}' is empty")

            return value

    def get_from_dict(self, data, map_list, field_config):
        """
        Returns a dictionary based on a mapping

        :param data: Data
        :type data: dict
        :param map_list: List of mapping fields
        :type map_list: list
        :param field_config: Configuration for transformation
        :type field_config: dict

        :return: Value
        """

        try:
            value = reduce(operator.getitem, map_list, data)
        except (KeyError, AttributeError, TypeError):
            return None
        else:
            return self.transform_value(
                field_name="/".join(map_list), field_config=field_config, value=value
            )

    @staticmethod
    def set_in_dict(data, map_list, value):
        """
        Set item in nested dictionary

        :param data: Data
        :type data: dict
        :param map_list: List of mapping fields
        :type map_list: list
        :param value: The value to update with

        :return: Data
        :rtype: dict
        """

        try:
            reduce(operator.getitem, map_list[:-1], data)[map_list[-1]] = value
        except (KeyError, AttributeError, TypeError):
            pass
        finally:
            return data

    def map_data(self, mapping, data):
        """
        Map data to a new dictionary


        :param mapping: Field mapping
        :type mapping: dict
        :param data: Data object
        :type data: dict

        :return: Mapped data
        :rtype: dict
        """

        formatted_dict = {}

        for field in mapping:
            field_config = mapping[field]

            if "_items" in field_config:
                formatted_dict[field] = self.map_data(
                    mapping=mapping[field]["_items"], data=data
                )
                continue

            if "field" in field_config:
                field_mapping = field_config["field"].split("/")
                formatted_dict[field] = self.get_from_dict(
                    data=data, map_list=field_mapping, field_config=field_config
                )
                continue

            logging.error(
                f"Mapping for field '{field}' is incorrect, skipping this field"
            )

        return formatted_dict

    def get_mapped_data(self, data_object):
        """
        Transform data to a list of mapped data

        :param data_object: Data object
        :type data_object: dict

        :return: List of mapped data
        :rtype: list
        """

        # Get nested data object if configured
        try:
            if hasattr(config, "MAPPING_DATA_SOURCE"):
                data_object = self.get_from_dict(
                    data=data_object,
                    map_list=config.MAPPING_DATA_SOURCE.split("/"),
                    field_config={},
                )
        except (ValueError, KeyError) as e:
            logging.info(f"An error occurred during retrieval of data: {str(e)}")
            return None

        # Check if data is of type list, otherwise create a list
        if not isinstance(data_object, list):
            data_object = [data_object]

        # Create mapped data objects based on the configuration
        formatted_data = []
        for data in data_object:
            try:
                mapped_data = self.map_data(config.MAPPING_FIELDS, data)
            except (ValueError, KeyError) as e:
                logging.info(f"An error occurred during formatting data: {str(e)}")
                continue
            else:
                formatted_data.append(mapped_data)

        return formatted_data

    def extract_attachments(self, data_object):
        """
        Extract the attachments from the mapped data

        :param data_object: Data object
        :type data_object: dict

        :return: Data and extracted attachments
        :rtype: (dict, dict)
        """

        item_attachments = {}

        if (
            hasattr(config, "MAPPING_ATTACHMENTS")
            and len(config.MAPPING_ATTACHMENTS) > 0
        ):
            for field in config.MAPPING_ATTACHMENTS:
                field_mapping = field.split("/")

                # Get current attachment value
                item_attachments[field] = self.get_from_dict(
                    data=data_object, map_list=field_mapping, field_config={}
                )

                # Remove current attachment value before update
                data_object = self.set_in_dict(
                    data=data_object, map_list=field_mapping, value=None
                )

        return data_object, item_attachments
