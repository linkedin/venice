---
layout: default
title: Router REST Spec
parent: Developer Guides
permalink: /docs/dev_guide/router_rest_spec
---
# Router REST Spec
This page describes how the router's REST endpoints work for reading data out of Venice with the thin client. Currently,
there exists a reference implementation of the thin client in Java, but if clients in additional languages are to be 
built, they could reference this spec to do so.

Note that although this part of the architecture is RESTful, Venice in general has a bias for performance, so the
implementation may not be fully up to the standards of a dogmatic RESTafarian. For example, because of URL length
limitations, batch gets are implemented with a POST rather than a GET, even though they carry no side effects.

The following sections describe the endpoints in the order they are invoked by the client during initialization, in
order to prime its internal state.

As with any client/server interaction, the developer of a new client implementation should consider details such as load
balancing, connection pooling, authentication and authorization. Since these tend to be generic concerns, rather than 
Venice-specific ones, they are omitted from this document.

## Cluster Discovery Endpoint
The first endpoint the client invokes is to discover which cluster the store of interest belongs to:

```
GET https://<host>:<port>/discover_cluster/<store_name>
```

This will return a JSON response in the following format:

```json
{
  "cluster": "cluster_name",
  "name": "store_name",
  "error": null,
  "errorType": null,
  "d2Service": "D2_SERVICE_NAME_62513b39957e_af278fc8",
  "serverD2Service": "SERVER_D2_SERVICE_NAME_62513b3d13a5_4dc8aa80",
  "zkAddress": "localhost:63359",
  "kafkaBootstrapServers": "localhost:1234",
  "exceptionType": null
}
```

For all JSON responses returned by Venice, it is advisable to validate that the `error` field is `null`. If it is not
`null`, then it should be a string containing some description of the error. In addition to that, the `errorType` field
is a means to categorize errors programmatically according to the values in the 
[ErrorType](https://github.com/linkedin/venice/blob/main/internal/venice-client-common/src/main/java/com/linkedin/venice/exceptions/ErrorType.java) 
enum.

The client then uses the information in the `d2Service` field of this response to connect to the correct cluster.

A few important details:

1. The cluster discovery mechanism currently makes certain assumptions that 
   [D2](https://linkedin.github.io/rest.li/Dynamic_Discovery) is used. This assumption could be loosened in the future 
   in order to integrate with other service discovery mechanisms.
2. This step can be skipped if not leveraging multi-cluster deployments, or if the operator wishes to let clients 
   hardcode some cluster addresses.
3. If using cluster migration, the mapping between a store and a cluster can change. In that case, there is a way for 
   the client to find out about it and automatically switch to the new cluster assignment. The switchover process needs 
   to be documented further.

## Schema Endpoints
After connecting to the correct cluster, the client primes its cache of 
[Avro](https://avro.apache.org/docs/1.11.1/specification/) schemas, by calling the endpoints below.

### Key Schema Endpoint
The key schema must be used to serialize keys to be queried. In Venice, the key schema cannot evolve, so the client can 
rely on this assumption. The key schema is retrieved via:
```
GET https://<host>:<port>/key_schema/<store_name>
```

This returns a JSON response in the following format:

```json
{
  "cluster": "cluster_name",
  "name": "store_name",
  "error": null,
  "errorType": null,
  "id": 1,
  "derivedSchemaId": -1,
  "schemaStr": "\"string\"",
  "exceptionType": null
}
```
The significant field here is `schemaStr`, which is the Avro schema in AVSC format, with the double quotes escaped so 
that they do not interfere with the surrounding JSON envelope.

### All Value Schemas Endpoint
In addition to the key schema, a Venice store is also associated with one or many value schemas, which are guaranteed to
be fully compatible with one another. A Venice store can contain records encoded with various versions of the schema, so
it is important for the client to know all registered schemas, so that it can perform 
[schema evolution](https://avro.apache.org/docs/1.11.1/specification/#schema-resolution).
```
GET https://<host>:<port>/value_schema/<store_name>
```
This returns the following JSON response:
```json
{
  "cluster": "cluster_name",
  "name": "store_name",
  "error": null,
  "errorType": null,
  "superSetSchemaId": 2,
  "schemas": [
    {
      "id": 1,
      "derivedSchemaId": -1,
      "rmdValueSchemaId": -1,
      "schemaStr": "{\"type\":\"record\",\"name\":\"ValueRecord\",\"namespace\":\"com.foo\",\"fields\":[{\"name\":\"intField\",\"type\":\"int\",\"default\":0}]}"
    }, {
      "id": 2,
      "derivedSchemaId": -1,
      "rmdValueSchemaId": -1,
      "schemaStr": "{\"type\":\"record\",\"name\":\"ValueRecord\",\"namespace\":\"com.foo\",\"fields\":[{\"name\":\"intField\",\"type\":\"int\",\"default\":0},{\"name\":\"stringField\",\"type\":\"string\",\"default\":\"\"}]}"
    }
  ],
  "exceptionType": null
}
```
### Individual Value Schema Endpoint
After initialization, it is possible that the client encounters new schemas that did not exist yet at the time it was
initialized. In those cases, the client can fetch those unknown schemas specifically via:
```
GET https://<host>:<port>/value_schema/<store_name>/<id>
```
This returns a payload identical to the key schema endpoint.

## Storage Endpoints
After initialization, the client can begin querying the routers for data. There are three read operations supported in
Venice:

- Single Get
- Batch Get
- Read Compute

All storage endpoints should be queried with the following request header:

```
X-VENICE-API-VERSION = 1
```

Furthermore, the following header is optional:

```
# Indicates that the client is capable of decompressing GZIP responses (if omitted, GZIP compressed stores will be 
# decompressed by the router, on behalf of the client).
X-VENICE-SUPPORTED-COMPRESSION-STRATEGY = 1
```

### Single Get Endpoint
Querying the value for a single key can be achieved by sending an HTTP GET request to:
```
GET https://<host>:<port>/storage/<store_name>/<key_in_base64>?f=b64
```

Alternatively, for stores with a simple string key schema, the value can be gotten via:
```
GET https://<host>:<port>/storage/<store_name>/<string_key>
```

The response code should be 200, and the value is encoded in Avro binary in the body of the response, with the following 
response headers:

```
# The Avro writer schema ID to lookup in the client's schema cache
X-VENICE-SCHEMA-ID = 1

# Valid values for the thin client include: 0 -> NO_OP, 1 -> GZIP
X-VENICE-COMPRESSION-STRATEGY = 0
```

### Batch Get
Batch gets are performed as an HTTP POST request to:
```
POST https://<host>:<port>/storage/<store_name>
```

The request should be accompanied by the following headers:

```
# Not supplying the streaming header results in a legacy non-streaming mode which is less performant, and which may be 
# removed in the future
X-VENICE-STREAMING = 1

# This header makes the quota system more efficient (technically optional, but highly recommended)
X-VENICE-KEY-COUNT = <number of keys queried>
```

The body of the POST request is a concatenation of all the queried keys, serialized in Avro binary. Note that the order
of the keys matter (see details below).

The response code will always be 200 since the router starts returning results incrementally before knowing if all parts
of the request can be fulfilled successfully (but see the footer details below for the real status code). Furthermore,
the values are laid out one after the other in the body of the response, packaged inside envelopes encoded in the 
following Avro schema:

```json
{
  "name": "MultiGetResponseRecordV1",
  "namespace": "com.linkedin.venice.read.protocol.response",
  "doc": "This field will store all the related info for one record",
  "type": "record",
  "fields": [
    {
      "name": "keyIndex",
      "doc": "The corresponding key index for each record. Venice Client/Router is maintaining a mapping between a unique index and the corresponding key, so that Venice backend doesn't need to return the full key bytes to reduce network overhead",
      "type": "int"
    },
    {
      "name": "value",
      "doc": "Avro serialized value",
      "type": "bytes"
    },
    {
      "name": "schemaId",
      "doc": "Schema id of current store being used when serializing this record",
      "type": "int"
    }
  ]
}
```

A few important details:

1. The client needs to keep track of the order in which it wrote the keys in the request body, because the response does
   not include the requested keys, but only a `keyIndex` corresponding to the key's position in the request body.
2. The response envelopes are not coming in the same order as they were requested.
3. When a requested key has no associated value, the envelope will contain the following sentinel values:
   1. The `keyIndex` is negative. 
   2. The `value` is empty. 
   3. The `schemaId` is `-1000`.
4. If any errors occurred as part of handling this request, then a special envelope at the end of the response body is 
included, enclosing a "footer record", with the following characteristics:
   1. The `keyIndex` is `-1000000`. 
   2. The `value` is encoded using the Avro schema below.
   3. The `schemaId` is `-1001`.

```json
{
  "name": "StreamingFooterRecordV1",
  "namespace": "com.linkedin.venice.read.protocol.response.streaming",
  "doc": "This record will store all the additional info after sending out streaming response",
  "type": "record",
  "fields": [
    {
      "name": "status",
      "doc": "Final HTTP status code (non-200) after processing the streaming request completely",
      "type": "int"
    },
    {
      "name": "detail",
      "doc": "Error detail",
      "type": "bytes"
    },
    {
      "name": "trailerHeaders",
      "doc": "Additional headers after sending out response headers",
      "type": {
        "type": "map",
        "values": "string"
      }
    }
  ]
}
```

### Read Compute

TODO