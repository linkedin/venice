
# VIP-5: Facet Counting

* **Status**: _Accepted_
* **Author(s)**: _Felix GV_
* **Pull Request**: [PR 1612](https://github.com/linkedin/venice/pull/1612)
* **Release**: _N/A_

## Introduction

Facet Counting is a type of aggregation query popular in the search domain. It provides information about the 
cardinality of values for the fields of documents of interest.

Given that search is one of the types of derived data workloads in which Venice already participates (i.e., the Da Vinci
Client can serve as a search solution's forward index component), there is demand from Venice users to increase the 
scope of capabilities such that Facet Counting can also be performed natively by Venice.

This document introduces the domain, and suggests a roadmap for implementing this capability in various phases, 
including on the client-side and server-side.

## Problem Statement 

This section dives deeper in defining what Facet Counting is, and how to integrate it into Venice.

Before diving into specifics, it may be useful to provide an analogy in order to make a general observation: _the 
proposal in this VIP has similarities and differences compared to Read Compute_. Read Compute, as it exists today, is a
record-wise (or we could say: row-wise) operation, meaning that for a given input record there is exactly one output
record. This type of workload is very natural to push down into the server-side, in such way that the work is split 
across many servers and the client can retrieve the various output records individually. The use cases presented here
also have a portion of work which is executable on a per-record basis, and therefore has the potential of being pushed
down to the server-side, however, given that they are "aggregation queries", there is also a final processing step which
must be performed in some central location (e.g., in the client). The work can therefore only be partially pushed down.

### Facet Counting Use Cases

Let's define what Facet Counting is, with a series of examples from simple to more complex, using SQL to explain it 
(although SQL is just a convenient way to express query semantics, but is not part of this proposal).

These are functioning SQL queries that have been run on a DuckDB database populated by Venice's own Push Job Details 
[system store](../../operations/data-management/system-stores.md). This system store's Avro schema can be seen here: [key](https://github.com/linkedin/venice/blob/main/internal/venice-common/src/main/resources/avro/PushJobStatusRecordKey/v1/PushJobStatusRecordKey.avsc),
[value](https://github.com/linkedin/venice/blob/main/internal/venice-common/src/main/resources/avro/PushJobDetails/v4/PushJobDetails.avsc).
The SQL schema for the table is also included below, though only a subset of these columns are used in the examples:

```sql
SELECT column_name, column_type FROM (DESCRIBE current_version);
┌───────────────────────────────────────┬─────────────┐
│              column_name              │ column_type │
│                varchar                │   varchar   │
├───────────────────────────────────────┼─────────────┤
│ storeName                             │ VARCHAR     │
│ versionNumber                         │ INTEGER     │
│ clusterName                           │ VARCHAR     │
│ reportTimestamp                       │ BIGINT      │
│ pushId                                │ VARCHAR     │
│ partitionCount                        │ INTEGER     │
│ valueCompressionStrategy              │ INTEGER     │
│ chunkingEnabled                       │ BOOLEAN     │
│ jobDurationInMs                       │ BIGINT      │
│ totalNumberOfRecords                  │ BIGINT      │
│ totalKeyBytes                         │ BIGINT      │
│ totalRawValueBytes                    │ BIGINT      │
│ totalCompressedValueBytes             │ BIGINT      │
│ totalGzipCompressedValueBytes         │ BIGINT      │
│ totalZstdWithDictCompressedValueBytes │ BIGINT      │
│ pushJobLatestCheckpoint               │ INTEGER     │
│ failureDetails                        │ VARCHAR     │
│ sendLivenessHeartbeatFailureDetails   │ VARCHAR     │
├───────────────────────────────────────┴─────────────┤
│ 18 rows                                   2 columns │
└─────────────────────────────────────────────────────┘
```

#### Facet Counting by Values of a Single Column

The simplest example of Facet Counting would be to count how many times each distinct value appears in a given column,
and we would typically want to limit the amount of results returned, as there could be quite a few:

```sql
SELECT   storeName,
         COUNT(storeName) AS cnt
FROM     current_version
GROUP BY storeName
ORDER BY cnt DESC
LIMIT    5;
┌─────────────────────────────┬───────┐
│          storeName          │  cnt  │
│           varchar           │ int64 │
├─────────────────────────────┼───────┤
│ HB_VPJtarget_prod-venice-0  │  2539 │
│ HB_VPJtarget_prod-venice-13 │  2340 │
│ HB_VPJtarget_prod-venice-8  │  2337 │
│ HB_VPJtarget_prod-venice-7  │  2335 │
│ HB_VPJtarget_prod-venice-3  │  2334 │
└─────────────────────────────┴───────┘
```

#### Facet Counting by Buckets Within a Column

Another example would be to group not by values, but by buckets. An example of this would be to have buckets for 
"last 24h", "last week", "last 30 days".

```sql
SET VARIABLE most_recent_job_time = (SELECT MAX(reportTimestamp) FROM current_version);
SET VARIABLE ms_per_day = (SELECT 24 * 60 * 60 * 1000);
SET VARIABLE ms_per_week = (SELECT GETVARIABLE('ms_per_day') * 7);
SET VARIABLE ms_per_30_days (SELECT CAST(GETVARIABLE('ms_per_day') AS BIGINT) * 30);

SELECT * FROM (
    SELECT   'last_24h' as value_or_bucket,
             COUNT(reportTimestamp) as cnt
    FROM     current_version
    WHERE    reportTimestamp > GETVARIABLE('most_recent_job_time') - GETVARIABLE('ms_per_day'))
UNION
SELECT * FROM (
    SELECT   'last_week' as value_or_bucket,
             COUNT(reportTimestamp) as cnt
    FROM     current_version
    WHERE    reportTimestamp > GETVARIABLE('most_recent_job_time') - GETVARIABLE('ms_per_week'))
UNION
SELECT * FROM (
    SELECT   'last_30_days' as value_or_bucket,
             COUNT(reportTimestamp) as cnt
    FROM     current_version
    WHERE    reportTimestamp > GETVARIABLE('most_recent_job_time') - GETVARIABLE('ms_per_30_days'))
ORDER BY cnt;

┌─────────────────┬───────┐
│ value_or_bucket │  cnt  │
│     varchar     │ int64 │
├─────────────────┼───────┤
│ last_24h        │  2150 │
│ last_week       │ 14811 │
│ last_30_days    │ 61991 │
└─────────────────┴───────┘
```

#### Facet Counting on Multiple Columns

Finally, a common example of Facet Counting would be to perform the same as above, but for multiple columns, all at 
once. In real scenarios, there could be hundreds of columns included, but to keep things simple we will do only two 
columns counted by values and one column counted by bucket:

```sql
SELECT * FROM (
    SELECT   'clusterName' AS col,
             clusterName AS value_or_bucket, 
             COUNT(clusterName) AS cnt 
    FROM     current_version 
    GROUP BY clusterName 
    ORDER BY cnt 
    DESC     LIMIT 5) 
UNION
SELECT * FROM (
    SELECT   'storeName' AS col, 
             storeName AS value_or_bucket, 
             COUNT(storeName) AS cnt 
    FROM     current_version 
    GROUP BY storeName 
    ORDER BY cnt 
    DESC     LIMIT 5)
UNION
SELECT * FROM (
    SELECT   'reportTimestamp' AS col,
             'last_24h' as value_or_bucket,
             COUNT(reportTimestamp) as cnt
    FROM     current_version
    WHERE    reportTimestamp > GETVARIABLE('most_recent_job_time') - GETVARIABLE('ms_per_day'))
UNION
SELECT * FROM (
    SELECT   'reportTimestamp' AS col,
             'last_week' as value_or_bucket,
             COUNT(reportTimestamp) as cnt
    FROM     current_version
    WHERE    reportTimestamp > GETVARIABLE('most_recent_job_time') - GETVARIABLE('ms_per_week'))
UNION
SELECT * FROM (
    SELECT   'reportTimestamp' AS col,
             'last_30_days' as value_or_bucket,
             COUNT(reportTimestamp) as cnt
    FROM     current_version
    WHERE    reportTimestamp > GETVARIABLE('most_recent_job_time') - GETVARIABLE('ms_per_30_days'))
ORDER BY col, cnt DESC;
┌─────────────────┬─────────────────────────────┬───────┐
│       col       │       value_or_bucket       │  cnt  │
│     varchar     │           varchar           │ int64 │
├─────────────────┼─────────────────────────────┼───────┤
│ clusterName     │ venice-1                    │ 51060 │
│ clusterName     │ venice-3                    │ 33422 │
│ clusterName     │ venice-5                    │ 27961 │
│ clusterName     │ venice-0                    │ 18471 │
│ clusterName     │ venice-4                    │ 18350 │
│ reportTimestamp │ last_30_days                │ 61991 │
│ reportTimestamp │ last_week                   │ 14811 │
│ reportTimestamp │ last_24h                    │  2150 │
│ storeName       │ HB_VPJtarget_prod-venice-0  │  2539 │
│ storeName       │ HB_VPJtarget_prod-venice-13 │  2340 │
│ storeName       │ HB_VPJtarget_prod-venice-8  │  2337 │
│ storeName       │ HB_VPJtarget_prod-venice-7  │  2335 │
│ storeName       │ HB_VPJtarget_prod-venice-3  │  2334 │
├─────────────────┴─────────────────────────────┴───────┤
│ 13 rows                                     3 columns │
└───────────────────────────────────────────────────────┘
```

#### Filtering

The above examples all demonstrate Facet Counting being performed on a full table. And while that is feasible, it can be
a costly proposition. In practice, a more common scenario is to perform Facet Counting on a subset of rows of the 
dataset. In the case of this proposal, the goal is to provide the ability to perform Facet Counting on some pre-defined
keys within the Venice dataset.

### Where to Perform the Counting?

There are a variety of locations where the counting could be performed: client, router, server. Ultimately, it is 
probably ideal to have the ability to perform it in all three locations, and to decide by configuration what mode the
system will operate in, so that we have the most operational flexibility. That, however, does not mean that we need to
implement all of these in order to start getting value from the project (see Development Milestones).

#### Client-side Computation

There is already support for performing Read Compute on the client-side, which is useful as a fallback in cases where
server-side Read Compute is disabled for the queried store. Similarly, the ability to perform server-side Facet Counting 
should be enabled via a store config, and the client should be capable of gracefully degrading to client-side compute
if the server-side is disabled. This is important so that users can make use of the API no matter what the server-side
settings are, and configs then become a lever for shifting work across components, rather than a functional blocker.

#### Server-side Computation

The appeal of supporting server-side computations for Facet Counting is the same as for Read Compute:

1. The response sizes should be much smaller, thus saving on network bandwidth and improving the overall end-to-end 
   performance.
2. Moreover, the work can be partitioned across many servers, each of which would need to do just a fraction of the 
   total.

The difference with currently supported Read Compute queries is that, given that Facet Counting is an aggregation query,
there still needs to be some amount of work performed in the component which receives the subset of results from each
server. In the case of the Fast Client, this final step must be performed on the FC itself. In the case of the Thin 
Client, it could be performed either on the TC or on the Router.

#### Router-side Computation

As mentioned above, for Thin Clients performing Facet Counting queries on a store where server-side computation is 
enabled, the final step of the query processing could be done either on the client-side or router-side. From a 
functional standpoint, either way works, but from an efficiency standpoint, it may be better to do it on the router-side
to further reduce network bandwidth (between router and client). That being said, given that latency-sensitive 
applications ought to onboard to the Fast Client anyway, there may be diminishing returns in expending effort to support
router-side computation. Therefore, this milestone can be scheduled last, and it may be acceptable to not implement it 
at all.

## Scope

The following is in-scope:

1. Count by value and count by bucket, both of which would apply to specified keys only.
2. The ability to perform counting on multiple columns as part of the same query, including the ability to specify the
   same column for the sake of both count by value and count by bucket.
3. Client-side (TC, FC, DVC) and server-side computation.

The following is out of scope:

1. Full table scans.
2. Router-side computation.
3. Perform both Read Compute and Facet Counting on the same keys as part of the same query.

## Project Justification

The goal of this project is to make Venice more amenable to leverage in a variety of derived data use cases. This can
help users minimize the complexity of juggling multiple independent systems, leading to productivity and operability 
improvements for them.

While it is already possible for users to manually implement Facet Counting on data retrieved via Batch Get (which would
be equivalent to the client-side support described above, from a performance standpoint), this approach does not scale
well for larger workloads. Having the ability to code against a Venice-provided API and let the infrastructure decide
if/when to shift the workload to the server-side provides better cost and scalability options.

## Functional Specification

The Facet Counting API would be a DSL similar to, but distinct from, the Read Compute API. Queries could be specified
using the following builder pattern:

```java
public interface ComputeAggregationRequestBuilder<K> extends ExecutableRequestBuilder<K, ComputeAggregationResponse> {
  /**
   * Aggregation query where the content of specified fields are going to be grouped by their value, then the occurrence
   * of each distinct value counted, after which the top K highest counts along with their associated values are going
   * to be returned.
   *
   * @param topK The max number of distinct values for which to return a count.
   * @param fieldNames The names of fields for which to perform the facet counting.
   * @return The same builder instance, to chain additional operations onto.
   */
  ComputeAggregationRequestBuilder<K> countGroupByValue(int topK, String... fieldNames);

  /**
   * Aggregation query where the content of specified fields are going to be grouped by buckets, with each bucket being
   * defined by some {@link Predicate}, and each matching occurrence incrementing a count for its associated bucket,
   * after which the bucket names to counts are going to be returned.
   *
   * @param bucketNameToPredicate A map of predicate name -> {@link Predicate} to define the buckets to group values by.
   * @param fieldNames The names of fields for which to perform the facet counting.
   * @return The same builder instance, to chain additional operations onto.
   * @param <T> The type of the fields to apply the bucket predicates to.
   */
  <T> ComputeAggregationRequestBuilder<K> countGroupByBucket(
          Map<String, Predicate<T>> bucketNameToPredicate,
          String... fieldNames);
}
```

Responses could be accessed using the following container:

```java
public interface ComputeAggregationResponse {
  /**
   * @param fieldName for which to get the value -> count map
   * @return the value counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByValue(int, String...)}
   */
  <T> Map<T, Integer> getValueToCount(String fieldName);

  /**
   * @param fieldName for which to get the bucket -> count map
   * @return the bucket counts as defined by {@link ComputeAggregationRequestBuilder#countGroupByBucket(Map, String...)}
   */
  Map<String, Integer> getBucketNameToCount(String fieldName);
}
```

Using the proposed API to achieve the `Facet Counting on Multiple Columns` use case above would look like this:

```java
public class VIP5Example {
  public void vip(AvroGenericReadComputeStoreClient<GenericRecord, GenericRecord> client, Set<GenericRecord> keySet) {
    // Note that the computeAggregation() API is not added in this PR, and will be added once initial support is built
     ComputeAggregationRequestBuilder<GenericRecord> requestBuilder = client.computeAggregation();

    // Using the Predicate API to define the filtering criteria of each bucket
    long currentTime = System.currentTimeMillis();
    Map<String, Predicate<Long>> bucketByTimeRanges = new HashMap<>();
    bucketByTimeRanges.put("last_24h", LongPredicate.greaterThan(currentTime - 1 * Time.MS_PER_DAY));
    bucketByTimeRanges.put("last_week", LongPredicate.greaterThan(currentTime - 7 * Time.MS_PER_DAY));
    bucketByTimeRanges.put("last_30_days", LongPredicate.greaterThan(currentTime - 30 * Time.MS_PER_DAY));
    
    // Facet count for multiple columns, including both grouped by value and grouped by buckets
     ComputeAggregationRequestBuilder<GenericRecord> requestBuilder = requestBuilder
            .countGroupByValue(5, "clusterName", "storeName")
            .countGroupByBucket(bucketByTimeRanges, "reportTimestamp");

    // Specify filter to specific keys in order to execute the query
    CompletableFuture<ComputeAggregationResponse> facetCountResponse = requestBuilder.execute(keySet);
  }
}
```

As we can see, the proposed DSL is significantly more succinct than the equivalent SQL. That is because although SQL can 
be made to do nearly anything, Facet Counting of multiple columns in a single query is not a common use case.

## Proposed Design

The client-side computation is very similar to Read Compute and therefore fairly straightforward. Essentially, the 
client would retrieve the values for the keys of interest via Batch Get, then perform the computation locally.

Regarding server-side computation, it could be achieved either by extending the current `/compute` endpoint on servers,
or as a new endpoint. Given that there are significant differences between the two, it may be tedious to cram the two of
them into the same endpoint. Moreover, since there is no need for the time being to combine Read Compute and Facet 
Counting for the same set of keys as part of a single query, there is no incentive for now to incur this complexity 
cost. If, in the future, it becomes necessary to combine these types of queries together, then the architecture can be
further evolved at that point.

The Facet Counting endpoint would need to have its own wire protocol, similar to the Read Compute wire protocol:

1. Start by listing the computation details (encoded from what was specified in `ComputeAggregationRequestBuilder`).
2. Then list all the keys to query and compute on.

It is necessary to support protocol evolution, but being separate from Read Compute, these two can evolve on separate 
tracks.

## Development Milestones

1. Start with client-side support, so that users can start using the API as soon as possible, and that it works 
   regardless of store and server settings.
2. Then implement the wire protocol and server-side support.

### Future Work

Although this is out of scope from the current proposal, it is interesting to note that having the scaffolding in place 
to perform aggregation queries opens up the door to performing other kinds of aggregations besides counting, e.g., min,
max, average, sum, or other functions...

## Test Plan

This functionality requires the full scope of test methodologies: unit and integration tests, followed by collaboration
with early adopter users to integrate and gradually ramp.

## References 

1. [Facet Counting in Lucene](https://lucene.apache.org/core/4_1_0/facet/org/apache/lucene/facet/doc-files/userguide.html#facet_features)

## Appendix

See the code attached to [PR 1612](https://github.com/linkedin/venice/pull/1612).