# Venice Multi-Datacenter Docker Quickstart

#### Download docker-compose-multi-dc-setup.yaml
```bash
wget https://raw.githubusercontent.com/linkedin/venice/main/docker/docker-compose-multi-dc-setup.yaml
```

#### Run docker-compose-multi-dc-setup.yaml to bring up the venice cluster: venice-cluster0
```bash
docker-compose -f docker-compose-multi-dc-setup.yaml up -d
```

#### Login to venice-client container
```bash
docker exec -it venice-client /bin/bash

```

#### Make sure that you're in /opt/venice directory
```bash
# pwd
/opt/venice

# if not then change directory

cd /opt/venice
````

### Create a store 
Note: If you change the store name from test-store to something else, you'll have to modify `/opt/venice/sample-data/multi-dc-configs/batch-push-job.properties` and `/opt/venice/sample-data/multi-dc-configs/inc-push-job.properties` to use the provided store name. 

```bash
./create-store.sh http://venice-controller.dc-parent.venicedb.io:5555 venice-cluster0 test-store sample-data/schema/keySchema.avsc sample-data/schema/valueSchema.avsc 
```

#### Let's add a dataset to the store using batch push

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
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 90
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 100
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 110
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 120
```

##### Fetch data from dc-1
```bash
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 90
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 100
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 110
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 120
```


#### Let's update some existing records in the dataset and add few new records using incremental push

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
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 90
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 100
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 110
./fetch.sh http://venice-router.dc-0.venicedb.io:7777 test-store 120
```

##### Fetch data from dc-1
```bash
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 90
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 100
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 110
./fetch.sh http://venice-router.dc-1.venicedb.io:7777 test-store 120
```

#### Exit from the venice-client container
```bash
# type exit command on terminal or use cntrl + c
exit
```

#### Teardown the cluster
```bash
docker-compose -f docker-compose-single-dc-setup.yaml down
```

