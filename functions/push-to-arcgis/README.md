# Push to ArcGIS

This function is used as an interface between the Operational Data Hub and an ArcGIS server. The function will process
Pub/Sub messages, create a new object based on mapping configuration and publishes them towards a feature server.

### Configuration
This function supports a field mapping for transforming data into GIS objects. The mapping is based on the incoming
Pub/Sub Subscription name, that is sent within the incoming message. To allow multiple Pub/Sub Subscriptions to use this
function, each of them must have a valid configuration (as shown below, see [config.example.py](config.example.py) for
an example).

~~~python
MAPPING_CONFIG = {
    "pubsub-subscription-1": {
        "arcgis": {...},
        "mapping": {...}
    },
    "pubsub-subscription-2": {
        "arcgis": {...},
        "mapping": {...}
    }
}
~~~

The following attributes can (and some must) be defined for each incoming subscription:

- `arcgis` `required` `[dict]`: A dictionary containing the authentication values for the GIS feature service 
  (see [ArcGIS authentication](#arcgis-authentication));
- `attachment_fields` `[list]`: A list of fields containing an attachment that will be sent towards ArcGIS
  (format: `field/sub-field/sub-sub-field`);
- `data_source` `[string]`: The (nested) data field (format: `field/sub-field/sub-sub-field`);
- `mapping` `required` `[dict]`: The field mapping for the transformation of an incoming message towards an ArcGIS
  object (see [field mapping](#field-mapping)).

### ArcGIS authentication
Before this function can post new data towards ArcGIS, first a token has to be retrieved. This is done based on some
ArcGIS configuration. The `arcgis` attribute within the mapping config of each subscription must be contain the
following data:

~~~json
{
  "authentication": {
    "url": "The url for the token request",
    "username": "The username for the request",
    "secret": "The GCP Secret Manager secret name containing the secret value for the request",
    "request": "The name of the request for retrieving a token",
    "referer": "The referer of the request"
  },
  "feature_url": "The URL towards the correct feature service"
}
~~~

### Field mapping

For the field mapping, the attributes below can be used.

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

#### Character set `optional`

When just a part of the value must be included within the data, the attribute `character_set` can be used. This will
retrieve the characters between the defined character positions. The attribute consists of a list with two integers; the
starting character position, and the ending character position:

~~~text
[character_start, character_end]
~~~

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
    "character_set": [
      null,
      5
    ]
  },
  "city": {
    "field": "address",
    "character_set": [
      6,
      null
    ]
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

> When the start or end of a value has to be specified, the value `None` or `null` can be used.

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