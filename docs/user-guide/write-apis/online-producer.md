# Online Producer

The Online Producer enables online applications to write data to a Venice store directly. All writes are still
asynchronous, and data is only eventually consistent. However, the APIs guarantee durability of the data if the
operation is successful.

## Prerequisites

To use the Online Producer, the store must meet some prerequisites:

1. It must not have writes disabled
2. It must be a hybrid store
3. It must have a current version.

In addition to the store-level prerequisites, the current version must meet the following prerequisites:

1. It must be configured as hybrid; aka capable of receiving near-line writes
2. It must specify either `ACTIVE_ACTIVE` or `NON_AGGREGATE` data-replication policies
3. It must specify a partitioner that the writer application knows how to use

## API

Detailed Javadocs for the Online Producer API can be accessed
[here](https://venicedb.org/javadoc/com/linkedin/venice/producer/VeniceProducer.html). All of these APIs have at least
two versions - one that accepts a logical timestamp and one that doesn't.

1. Logical timestamps (in ms) are what Venice backend will use to resolve conflicts in case multiple writes modify the
   same record. An update to Venice could be triggered due to some trigger that can be attributed to a specific point in
   time. In such cases, it might be beneficial for applications to mark their updates to Venice with that timestamp and
   Venice will persist the record as if it had been received at that point in time - either by applying the update,
   dropping the update if a newer update has already been persisted, or applying an update partially only to fields that
   have not received an update with a newer timestamp yet.
2. In case the write requests are made without specifying the logical timestamp, then the time at which the message was
   produced is used as the logical timestamp during conflict resolution.

## Usage

To create an instance of the producer, `OnlineProducerFactory` should be used since the interface for
`OnlineVeniceProducer` is not yet considered stable and can introduce backward incompatible changes.

```java
String storeName = "<store_name>";
ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(storeName)
    .setVeniceURL("http://router.host:7777")
    ...;
VeniceProperties producerConfig = VeniceProperties.empty();
OnlineVeniceProducer producer = OnlineProducerFactory.createProducer(clientConfig, producerConfig, null);

producer.asyncPut(key, value).get();
producer.asyncDelete(key).get();
producer.asyncUpdate(key, builder -> {
    ((UpdateBuilder) updateBuilderObj)
        .setNewFieldValue(FIELD_NUMBER, 10L);
        .setNewFieldValue(FIELD_COMPANY, "LinkedIn")
        ...;
}).get();
```

## Optional Configs

The online Venice producer can be configured by specifying the following optional configs:

To specify the number of threads dedicated for the online Venice producer. This also controls the number of concurrent
write operations in each producer:

```
client.producer.thread.num = 10
```

To specify the interval at which the client will refresh its schema cache:

```
client.producer.schema.refresh.interval.seconds = 300
```
