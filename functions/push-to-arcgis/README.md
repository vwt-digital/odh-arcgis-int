# Push to ArcGIS

This function is used as an interface between the Operational Data Hub and an ArcGIS server. The function will process
Pub/Sub messages, create a new object based on mapping configuration and publishes them towards a feature server.

## Configuration
These variables have to be defined within the environment of the function:
- `GIS_FEATURE_SERVICE_AUTHENTICATION` `required` `[dict]`: A dictionary containing the authentication values for the 
  GIS feature service;
- `GIS_FEATURE_SERVICE` `required` `[string]`: The URL of the GIS feature service;
- `DATA_FIELD` `[string]`: The mapping of the nested data object;
- `FIELD_MAPPING` `required` `[dict]`: The field mapping (see [Field Mapping](#field-mapping)).

### Field mapping
This function supports a field mapping for transforming data into GIS objects. Within each field the following 
attributes can be used to create nested fields, retrieve a value from a list or get part of a string.

#### Field `required`
The most important part of the mapping is the `field` attribute. This will specify where in the data the value must be
retrieved from. Nested fields can be specified by seperating it with a `/`.

###### Data
~~~json
{
  "properties": {
    "id": 123456,
    "user": {
      "name": "Test"
    }
  }
}
~~~

###### Mapping
~~~json
{
  "id": {
    "field": "properties/id"
  },
  "name": {
    "field": "properties/user/name"
  }
}
~~~

###### Result
~~~json
{
  "id": 123456,
  "name": "Test"
}
~~~

#### Required `optional`
When needed, the `required` attribute can be used to enforce the presence of a specific field. If this value can not be
found within the data or is empty, the message won't be published towards the GIS server

###### Mapping
~~~json
{
  "id": {
    "field": "properties/id",
    "required": true
  },
  "name": {
    "field": "properties/user/name"
  }
}
~~~

> The default value of the `required` attribute is `False`

#### List item `optional`
When it is needed to retrieve a certain value from a list, the `list_item` attribute can be used. With this the specific
list-item can be specified.

###### Data
~~~json
{
  "coordinates": [
    52.1326,
    5.2913
  ]
}
~~~

###### Mapping
~~~json
{
  "latitude": {
    "field": "coordinates",
    "list_item": 1
  }
}
~~~

###### Result
~~~json
{
  "latitude": 5.2913
}
~~~

#### Part of value `optional`
When just a part of the value must be included within the data, there are two fields that can be specified:
`characters_to` and `characters_from`. The `characters_to` attribute will retrieve the characters from the beginning of
the value until the specified amount. The `characters_from` will retrieve the characters from the specified amount 
until the end of the string.

###### Data
~~~json
{
  "address": "1234AB Amsterdam"
}
~~~

###### Mapping
~~~json
{
  "postcode": {
    "field": "address",
    "character_to": 5
  },
  "city": {
    "field": "address",
    "character_from": 6
  }
}
~~~

###### Result
~~~json
{
  "postcode": "1234AB",
  "city": "Amsterdam"
}
~~~

#### Nested object `optional`
When a nested object is needed, the field `_items` can be used. This ensure the function will create a nested object and
fill this with the fields specified within the `_items` list.

##### Data
~~~json
{
  "properties": {
    "id": 123456,
    "user": {
      "name": "Test"
    }
  }
}
~~~

###### Mapping
~~~json
{
  "attributes": {
    "_items": {
      "id": {
        "field": "properties/id",
        "required": true
      },
      "name": {
        "field": "properties/user/name",
        "required": false
      }
    }
  }
}
~~~

###### Result
~~~json
{
  "attributes": {
    "id": 123456,
    "name": "Test"
  }
}
~~~

## Invoking
The function can be invoked by creating a Pub/Sub Push Subscription towards the HTTP-endpoint of the function. Don't
forget to ensure the Pub/Sub instances has Function Invoking permission.

Function entrypoint: `push_to_arcgis`

## License
[GPL-3](https://www.gnu.org/licenses/gpl-3.0.en.html)