---
layout: default
title: Venice Multi-Datacenter Docker Quickstart
parent: Quickstart
permalink: /docs/quickstart/quickstart-multi-datacenter
---


# Venice Multi-Datacenter Docker Quickstart


Follow this guide to set up a multi-datacenter venice cluster using docker images
provided by Venice team.


#### Step 1: Install and set up Docker Engine
    Follow https://docs.docker.com/engine/install/ to install docker and start docker engine



#### Step 2: Download docker-compose-multi-dc-setup.yaml file
```
wget https://raw.githubusercontent.com/linkedin/venice/main/docker/docker-compose-multi-dc-setup.yaml
```


#### Step 3: Run docker compose to bring up Venice multi-colo setup
```bash
docker-compose -f docker-compose-multi-dc-setup.yaml up -d
```


#### Step 4: Access `venice-client` container's bash shell
```bash
docker exec -it venice-client /bin/bash
```

#### Step 5: Make sure that you're in /opt/venice directory
```bash
# pwd
/opt/venice
# if not change current working directory to /opt/venice
cd /opt/venice
```

#### Step 6: Create a store 
Note: If you change the store name from `test-store` to something else, you will have to modify `/opt/venice/sample-data/multi-dc-configs/batch-push-job.properties` and `/opt/venice/sample-data/multi-dc-configs/inc-push-job.properties` to use the provided store name. 

```bash
./create-store.sh http://venice-controller.dc-parent.venicedb.io:5555 venice-cluster0 test-store sample-data/schema/keySchema.avsc sample-data/schema/valueSchema.avsc 
```

#### Step 7: Let's add a dataset to the store using batch push

##### Print dataset
```bash
./avro-to-json.sh sample-data/batch-push-data/kv_records.avro 
```

##### Run a push job

```bash
./run-vpj.sh sample-data/multi-dc-configs/batch-push-job.properties 
```

##### Fetch data from dc-0
```bash
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 90 # should return a value
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 100 # should return a value
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 110 # should return null
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 120 # should return null
```

##### Fetch data from dc-1
```bash
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 90 # should return a value
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 100 # should return a value
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 110 # should return null
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 120 # should return null
```


#### Step 8: Let's update some existing records in the dataset and add few new records using incremental push

##### Print records to be updated and added to the existing dataset in the store
```bash
./avro-to-json.sh sample-data/inc-push-data/kv_records_v1.avro 
```

##### Run incremental push job
```bash
./run-vpj.sh sample-data/multi-dc-configs/inc-push-job.properties 
```

##### Fetch data from dc-0
```bash
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 90 # should return an unchanged value
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 100 # should return an updated value
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 110 # should return inserted value
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 120 # should return null
```

##### Fetch data from dc-1
```bash
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 90 # should return an unchanged value
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 100 # should return an updated value
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 110 # should return inserted value
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 120 # should return null
```

#### Step 9: Exit from the venice-client container
```bash
# type exit command on the terminal or use cntrl + c
exit
```

#### Step 10: Teardown the cluster
```bash
docker-compose -f docker-compose-single-dc-setup.yaml down
```
