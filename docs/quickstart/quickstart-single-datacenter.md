---
layout: default
title: Venice Single-Datacenter Docker Quickstart
parent: Quickstart
permalink: /docs/quickstart/quickstart-single-datacenter
---


# Venice Single-Datacenter Docker Quickstart



Follow this guide to set up a simple venice cluster using docker images
provided by Venice team.


#### Step 1: Install and set up Docker Engine
    Follow https://docs.docker.com/engine/install/ to install docker and start docker engine


#### Step 2: Download Venice quickstart Docker compose file
```
wget https://raw.githubusercontent.com/linkedin/venice/main/quickstart/docker-compose.yaml
```

#### Step 3: Run docker compose
This will download and start containers for kafka, zookeeper, venice-controller, venice-router,
venice-server, and venice-client.
Once containers are up and running, it will create a test cluster, namely, `venice-cluster`.

Note: Make sure the `docker-compose.yaml` downloaded in step 2 is in the same directory from which you will run the following command.
```
docker compose up -d
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
    "name": "id",
    "type": "string"
}
```
value schema:
```bash
{
   "name": "name",
   "type": "string"
}
```

Let's create a venice store:
```
bash create_store.sh
```

#### Step 6: Push data to the store
Venice supports multiple ways to write data to the store. For more details, please refer to [Write Path](../README.md#write-path) section in [README](../README.md).
In this example, we will use batch push mode and push 100 records.
```
key: 1 to 100
value: test_name_1 to test_name_100
```

Let's push the data:
```
bash batch_push_data.sh
```

#### Step 7: Read data from the store
Let's query some data from `venice-store`, using `venice-thin-client` which is included in venice container images. (key: `1 to 100`)
```
bash query_data.sh <key>
```
For example:
```bash
$ bash query_data.sh 1
key=1
value=test_name_1

$ bash query_data.sh 100
key=100
value=test_name_100

# Now if we do get on non-existing key, venice will return `null`
$ bash query_data.sh 101
key=101
value=null
```


#### Step 8: Update and add some new records using Incremental Push
Venice supports incremental push which allows us to update values of existing rows or to add new rows in an existing store.
In this example, we will
1. update values for keys from `51-100`. For example, the new value of `100` will be `test_name_100_v1`
2. add new rows (key: `101-150`)

```
bash inc_push_data.sh
```

#### Step 9: Read data from the store after Incremental Push
Incremental Push updated the values of keys 51-100 and added new rows 101-150.
Let's read the data once again.

```bash
# Value of 1 changed remains unchanged
$ bash query_data.sh 1
key=1
value=test_name_1

# Value of 100 changed from test_name_100 to test_name_100_v1
$ bash query_data.sh 100
key=100
value=test_name_100_v1

# Incremental Push added value for previously non-existing key 101
$ bash query_data.sh 101
key=101
value=test_name_101_v1
```


#### Step 10: Exit `venice-client`
```
exit
```

#### Step 11: Stop docker
Tear down the venice cluster
```
docker compose down
```

## Next steps
Venice is a feature rich derived data store. It offers features such as write-compute, read-compute, streaming ingestion, multi data center active-active replication,
deterministic conflict resolution, etc. To know more about such features please refer [README](../README.md) and reach out to
the [Venice team](../README.md#community-resources).
