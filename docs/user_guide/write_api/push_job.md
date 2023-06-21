---
layout: default
title: Push Job
parent: Write APIs
grand_parent: User Guides
permalink: /docs/user_guide/write_api/push_job
---
# Push Job
The Push Job takes data from a Hadoop grid and writes it to Venice. 

## Use Cases
There are two modes the Push Job can run in:

- Full Push (default)
- Incremental Push

### Full Push
When performing a Full Push, the user takes advantage of the fact that Venice's datasets are versioned. A Full Push
triggers the dynamic creation of a new dataset version, and then loads data into it in the background. The new dataset 
version is called a "future" version as long as data is still loading, and during that time, no online read traffic will
be served from it. When the loading is determined to have successfully completed in a given datacenter, the new dataset 
version transitions from "future" to "current", whereas the old dataset version transitions from "current" to "backup". 
When a dataset version becomes current, online read traffic gets routed to it.

### Incremental Push
When performing an Incremental Push, no new dataset versions are created, and data gets added into all existing versions 
of the dataset. This leverages the same mechanism as Streaming Writes, and requires that the store be configured as 
Hybrid.

### Targeted Region Push
Technically, targeted region push is an option of full push _(hence not a new mode)_, but it allows writing data into a 
subset of global regions/data centers, whereas full push writes globally at once.

By default, it automatically pushes data to the rest of unspecified regions after the targeted region push is completed.
We are working on to implement more validations in between to ensure the targeted regions are healthy after the first push. 
Users may turn off this automation and perform validations and chain it with another full push/targeted region push to 
achieve the same effect as full push in terms of data integrity, but the store versions across different regions might be 
not the same depending on the exact setup.

## Usage
The Push Job is designed to require as few configs as possible. The following mandatory configs should be unique to each
use case, and set by the user:

- `venice.store.name`: The name of the Venice store to push into.
- `input.path`: The HDFS path containing the data to be pushed, populated with one or many Avro files, where each file 
  contains a sequence of records, and where each record has a `key` field and a `value` field.

In addition to use case-specific configs, there are also some necessary configs that would typically be the same for all 
use cases in a given environment (e.g., one value for production, another value for staging, etc.). The following can 
therefore be configured globally by the operator, in order to make the Push Job even easier to leverage by users:

- `venice.discover.urls`: The URL of the Venice controller.

### Optional Configs
The user may choose to specify the following configs:

- `incremental.push`: Whether to run the job in incremental mode. Default: `false`
- `key.field`: The name of the key field within the input records. Default: `key`
- `value.field`: The name of the value field within the input records. Default: `value`
- `allow.duplicate.key`: Whether to let the Push Job proceed even if it detects that the input contains multiple records 
  having the same key but distinct values. If set to `true`, then the Push Job picks one of the values to be written in
  a non-deterministic fashion. Default: `false`
- `extended.schema.validity.check.enabled`: Whether to perform extended schema validation on the input (equivalent to
  the `STRICT` mode in avro-util's [SchemaParseConfiguration](https://github.com/linkedin/avro-util/blob/master/helper/helper-common/src/main/java/com/linkedin/avroutil1/compatibility/SchemaParseConfiguration.java)). 
  If set to `false`, it becomes equivalent to avro-util's `LOOSE` mode. Default: `true`
- `targeted.region.push.enabled`: Whether to perform targeted region push. Default: `false`
- `targeted.region.push.list`: This config takes effect only when targeted region push flag is enabled. 
  Optionally specify a list of target region(s) to push data into. See full details at 
  [TARGETED_REGION_PUSH_LIST](https://venicedb.org/javadoc/com/linkedin/venice/hadoop/VenicePushJob.html#TARGETED_REGION_PUSH_LIST).
- `post.validation.consumption`: Whether to perform post validation consumption after targeted region push is finished. 
   Default: `true`. Set this to `false` if you want to achieve a true single colo push.

The push job also supports using D2 URLs for automated controller service discovery. To use this, the user or operator
must specify the following configs:

- `multi.region`: `true` if the Venice cluster is deployed in a multi-region mode; `false` otherwise
- `venice.discover.urls`: The D2 URL of the Venice controller. It must be of the form `d2://<D2 service name of the controller>`
- `d2.zk.hosts.<region name>`: The Zookeeper addresses where the components in the specified region are announcing themselves to D2
- If `multi.region` is `true`:
  - `venice.discover.urls` must use the D2 service name of the parent controller
  - `parent.controller.region.name` must denote the name of the datacenter where the parent controller is deployed
  - `d2.zk.hosts.<parent controller region>` is a mandatory config
- If `multi.region` is `false`
  - `venice.discover.urls` must use the D2 service name of the child controller
  - `source.grid.fabric` must denote the name of the datacenter where Venice is deployed
  - `d2.zk.hosts.<source grid fabric>` is a mandatory config

The user or operator may want to specify the following security-related configs:

- `venice.ssl.enable`. Default: `false`
- `ssl.configurator.class`. Default: `com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator`
- `ssl.key.store.property.name`
- `ssl.trust.store.property.name`
- `ssl.key.store.password.property.name`
- `ssl.key.password.property.name`