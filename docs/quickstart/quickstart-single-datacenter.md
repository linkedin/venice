---
layout: default
title: Venice Single-Datacenter Docker Quickstart
parent: Quickstart
permalink: /docs/quickstart/quickstart-single-datacenter
---


# Venice Single-Datacenter Docker Quickstart



Follow this guide to set up a simple venice cluster using docker images
provided by Venice team.


#### Step 1: Install and set up Docker Engine and docker-compose
    Follow https://docs.docker.com/engine/install/ to install docker and start docker engine


#### Step 2: Download Venice quickstart Docker compose file
```
wget https://raw.githubusercontent.com/linkedin/venice/main/docker/docker-compose-single-dc-setup.yaml
```

#### Step 3: Run docker compose
This will download and start containers for kafka, zookeeper, venice-controller, venice-router,
venice-server, and venice-client.
Once containers are up and running, it will create a test cluster, namely, `venice-cluster`.

Note: Make sure the `docker-compose-single-dc-setup.yaml` downloaded in step 2 is in the same directory from which you will run the following command.
```
docker-compose -f docker-compose-single-dc-setup.yaml up -d
```

#### Step 4: Access `venice-client` container's bash shell
From this container, we will create a store in `venice-cluster`, which was created in step 3, push
data to it and run queries against it.
```
docker exec -it venice-client bash
```

#### Step 5: Create a venice store
The below script uses `venice-admin-tool` to create a new store: `venice-store`.
We will use the following key and value schema for store creation.

key schema:
```bash
{
    "name": "key",
    "type": "string"
}
```
value schema:
```bash
{
   "name": "value",
   "type": "string"
}
```

Let's create a venice store:
```bash
./create-store.sh http://venice-controller:5555 venice-cluster0 test-store sample-data/schema/keySchema.avsc sample-data/schema/valueSchema.avsc
```

#### Step 6: Push data to the store
Venice supports multiple ways to write data to the store. For more details, please refer to [Write Path](../README.md#write-path) section in [README](../README.md).
In this example, we will use batch push mode and push 100 records.
```
key: 1 to 100
value: val1 to val100
```

##### Print dataset
```bash
./avro-to-json.sh sample-data/batch-push-data/kv_records.avro 
```

##### Run a push job

Let's push the data:
```bash
./run-vpj.sh sample-data/single-dc-configs/batch-push-job.properties
```

#### Step 7: Read data from the store
Venice provides two client types for reading data. Both are included in the venice-client container image.

##### Option A: Thin client (routes through the router)
The thin client sends all read requests through the Venice Router.
```
./fetch.sh <router> <store> <key>
```
For example:
```bash
$ ./fetch.sh http://venice-router:7777 test-store 1
key=1
value=val1

$ ./fetch.sh http://venice-router:7777 test-store 100
key=100
value=val100

# Now if we do get on non-existing key, venice will return `null`
$ ./fetch.sh http://venice-router:7777 test-store 101
key=101
value=null
```

##### Option B: Fast client (D2 service discovery, reads directly from servers)
The fast client uses D2 service discovery for initial cluster discovery via the router, then
fetches metadata and routes data reads directly to the storage servers, bypassing the router.
This requires enabling storage node read quota on the store:
```bash
java -jar /opt/venice/bin/venice-admin-tool-all.jar --update-store \
  --url http://venice-controller:5555 --cluster venice-cluster0 \
  --store test-store --storage-node-read-quota-enabled true
```

Then query using the fast client:
```
./fast-client-fetch.sh <store> <key>
```
For example:
```bash
$ ./fast-client-fetch.sh test-store 1
key=1
value=val1

$ ./fast-client-fetch.sh test-store 100
key=100
value=val100

# Non-existing key returns null
$ ./fast-client-fetch.sh test-store 101
key=101
value=null
```


#### Step 8: Update and add some new records using Incremental Push
Venice supports incremental push which allows us to update values of existing rows or to add new rows in an existing store.
In this example, we will
1. update values for keys from `91-100`. For example, the new value of `100` will be `val100_v1`
2. add new rows (key: `101-110`)

##### Print records to be updated and added to the existing dataset in the store
```bash
./avro-to-json.sh sample-data/inc-push-data/kv_records_v1.avro 
```

##### Run incremental push job
```bash
./run-vpj.sh sample-data/single-dc-configs/inc-push-job.properties
```

#### Step 9: Read data from the store after Incremental Push
Incremental Push updated the values of keys 91-100 and added new rows 101-110.
Let's read the data once again.

```bash
# Value of 1 changed remains unchanged
$ ./fetch.sh http://venice-router:7777 test-store 1
key=1
value=val1

# Value of 100 changed from test_name_100 to test_name_100_v1
$ ./fetch.sh http://venice-router:7777 test-store 100
key=100
value=val100_v1

# Incremental Push added value for previously non-existing key 101
$ ./fetch.sh http://venice-router:7777 test-store 101
key=101
value=val101
```


#### Step 10: Exit `venice-client`
```bash
# type exit command on the terminal or use ctrl + c
exit
```

#### Step 11: Stop docker
Tear down the venice cluster
```bash
docker-compose -f docker-compose-single-dc-setup.yaml down
```

## Next steps
Venice is a feature rich derived data store. It offers features such as write-compute, read-compute, streaming ingestion, multi data center active-active replication,
deterministic conflict resolution, etc. To know more about such features please refer [README](../README.md) and reach out to
the [Venice team](../README.md#community-resources).
