import json
import logging
import operator
from functools import reduce

import pyproj


class FieldMapperService:
    def __init__(
        self, mapping_data_source, mapping_attachments, coordination_conversion_type
    ):
        """
        Initiates the FieldMapperService

        :param mapping_data_source: The data source field
        :type mapping_data_source: str
        :param mapping_attachments: A list of attachment fields
        :type mapping_attachments: list
        :param coordination_conversion_type: The coordination conversion type
        :type coordination_conversion_type: str
        """

        self.mapping_data_source = mapping_data_source
        self.mapping_attachments = mapping_attachments
        self.coordination_conversion_type = coordination_conversion_type

    @staticmethod
    def transform_value(field_mapping, field_config, value):
        """
        Transform a value based on configuration

        :param field_mapping: Mapping of the current field
        :type field_mapping: str
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
        except (KeyError, AttributeError, TypeError, IndexError) as e:
            logging.info(
                f"Value transformation for field '{field_mapping}' failed due to: {str(e)}"
            )
            value = None
        finally:
            if field_config.get("required", False) and not value:
                raise ValueError(f"Required field '{field_mapping}' is empty")

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

        value = None

        try:
            value = reduce(operator.getitem, map_list, data)
        except (KeyError, AttributeError, TypeError):
            pass
        finally:
            return self.transform_value(
                field_mapping="/".join(map_list), field_config=field_config, value=value
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

    def get_mapping(self, attribute_mapping, coordinate_mapping):
        return {
            "geometry": {
                "_items": {
                    "latitude": self.get_coordinate_mapping(
                        coordinate_mapping.latitude
                    ),
                    "longitude": self.get_coordinate_mapping(
                        coordinate_mapping.longitude
                    ),
                },
            },
            "attributes": {"_items": attribute_mapping},
        }

    @staticmethod
    def get_coordinate_mapping(field):
        """
        Get coordinate mapping based on field

        :param field: Mapping field
        :type field: str

        :return: Coordinate mapping
        :rtype: dict
        """

        field_mapping = field.split("/")

        if _is_int(field_mapping[-1]):
            return {
                "field": "/".join(field_mapping[:-1]),
                "list_item": int(field_mapping[-1]),
                "required": True,
            }

        return {
            "field": field,
            "required": True,
        }

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

            if isinstance(field_config, str):
                field_mapping = field_config.split("/")
                formatted_dict[field] = self.get_from_dict(
                    data=data, map_list=field_mapping, field_config={}
                )
                continue

            logging.error(
                f"Mapping for field '{field}' is incorrect, skipping this field"
            )

        return formatted_dict

    def get_mapped_data(self, data_object, mapping_fields, layer_field):
        """
        Transform data to a list of mapped data

        :param data_object: Data object
        :type data_object: dict
        :param mapping_fields: The field mapping for this instance
        :type mapping_fields: dict
        :param layer_field: The field where the ArcGIS layer is specified
        :type layer_field: str

        :return: List of mapped data
        :rtype: list
        """

        # Get nested data object if configured
        if self.mapping_data_source:
            data_object = self.get_from_dict(
                data=data_object,
                map_list=self.mapping_data_source.split("/"),
                field_config={},
            )

        if not data_object:
            return None

        # Check if data is of type list, otherwise create a list
        if not isinstance(data_object, list):
            data_object = [data_object]

        # Create mapped data objects based on the configuration
        formatted_data = []
        for data in data_object:
            try:
                mapped_data = self.map_data(mapping_fields, data)
                mapped_layer = (
                    self.get_from_dict(
                        data=data, map_list=layer_field.split("/"), field_config={}
                    )
                    if layer_field
                    else None
                )

                mapped_data["geometry"] = self.convert_lonlat_to_geometry(
                    mapped_data["geometry"]
                )
            except (ValueError, KeyError) as e:
                logging.info(f"An error occurred during formatting data: {str(e)}")
                logging.debug(json.dumps(data))
                continue
            else:
                formatted_data.append({"data": mapped_data, "layer_id": mapped_layer})

        return formatted_data

    def convert_lonlat_to_geometry(self, coordinates):
        """
        Convert longitude and latitude to geometry

        :param coordinates: Coordinates
        :type coordinates: dict

        :return: Geometry
        :rtype: dict
        """

        coordinate_y = coordinates["longitude"]
        coordinate_x = coordinates["latitude"]

        if (
            self.coordination_conversion_type == "wgs84-web_mercator"
        ):  # WGS 84 Web Mercator projection
            input_grid = pyproj.Proj(projparams="epsg:3857")
            wgs84 = pyproj.Proj(projparams="epsg:4326")
            converted_geometry = pyproj.transform(
                wgs84, input_grid, coordinate_y, coordinate_x
            )

            coordinate_y = converted_geometry[0]
            coordinate_x = converted_geometry[1]

        return {"x": coordinate_x, "y": coordinate_y}

    def extract_attachments(self, data_object):
        """
        Extract the attachments from the mapped data

        :param data_object: Data object
        :type data_object: dict

        :return: Data and extracted attachments
        :rtype: (dict, dict)
        """

        item_attachments = {}

        if self.mapping_attachments and len(self.mapping_attachments) > 0:
            for field in self.mapping_attachments:
                field_mapping = field.split("/")
                field_mapping.insert(0, "attributes")

                # Get current attachment value
                item_attachments[field] = self.get_from_dict(
                    data=data_object, map_list=field_mapping, field_config={}
                )

                # Remove current attachment value before update
                data_object = self.set_in_dict(
                    data=data_object, map_list=field_mapping, value=None
                )

        return data_object, item_attachments


def _is_int(x) -> bool:
    """
    Returns true if parameter is an integer.

    :param x: Integer or float to validate.
    """

    try:
        a = float(x)
        b = int(a)
    except ValueError:
        return False
    else:
        return a == b
