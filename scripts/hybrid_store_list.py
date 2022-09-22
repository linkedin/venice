'''
A script that scans the venice ZK metadata to get the number of hybrid store.

prerequisite for Mac: "pip install click" and "pip install kazoo"

prerequisite for Linux(CentOS/RHEL):
1. sudo yum install epel-release
2. sudo yum install python-pip
3. sudo pip install --upgrade pip
4. sudo pip install click
5. sudo pip install kazoo

Before running the script, create a SSH tunnel to the Venice ZK cluster
Assuming ${VENICE_ZK_URL} is the ZK url of Venice ZK cluster, ${VENICE_ZK_PORT}
is the Venice ZK port number, and ${HOSTNAME} is the host that can access the
above ZK cluster and one that your local machine has access to, run this command first:
ssh -nNT -L 2299:${VENICE_ZK_URL}:${VENICE_ZK_PORT} ${HOSTNAME}
'''

import click
import json
import logging
from kazoo.client import KazooClient

system_store_signature = "system_store"


# click is a handy helper library to read params from commandline
# python hybrid_store_list.py --host "localhost:2299" --cluster "cluster-0" --cluster "cluster-1" --zk-path-prefix "/venice"
@click.command()
@click.option('--cluster', '-c', multiple=True)
@click.option('--host', '-H')
@click.option('--zk-path-prefix')
def read_config(cluster, host, zk_path_prefix):
    if host is None:
        host = 'localhost:2299'
    if zk_path_prefix is None:
        zk_path_prefix = '/venice'
    list_hybrid_stores(host, cluster, zk_path_prefix)


# Scan the stores in a cluster to find out all hybrid stores
# cluster: Venice cluster name
# host: ZK host address
# zk: zk client. It's an optional param. If it's none, it create a new one
def hybrid_store_finder(cluster, zk, zk_path_prefix):
    logging.basicConfig()

    stores = zk.get_children("%s/%s/Stores" % (zk_path_prefix, cluster))
    print("There are %s stores in the cluster: %s" % (len(stores), cluster))

    regular_hybrid_store_num = 0
    pure_streaming_store_num = 0
    system_store_num = 0
    for store in stores:
        data, stat = zk.get("%s/%s/Stores/%s" % (zk_path_prefix, cluster, store))
        store_metadata = json.loads(data)
        if (store_metadata['hybridStoreConfig'] != None):
            store_name = store_metadata['name']
            if system_store_signature in store_name:
                system_store_num += 1
                print("system store: " + store_name)
            else:
                # this is a rough estimate. If a store has less than 10 versions, it's likely that it's a streaming-only store
                if (store_metadata['currentVersion'] < 10):
                    pure_streaming_store_num += 1
                    print("pure streaming store: " + store_name)
                else:
                    regular_hybrid_store_num += 1
                    print("regular hybrid store: " + store_name)

    print("Hybrid store number: %d\nPure streaming store number: %d\nSystem store number: %d" % (
    regular_hybrid_store_num + pure_streaming_store_num, pure_streaming_store_num, system_store_num))

    return len(stores), regular_hybrid_store_num + pure_streaming_store_num


# iterate the cluster list and scan all the stores under these clusters
def list_hybrid_stores(zk_host, venice_clusters, zk_path_prefix):
    total_store_num = 0
    total_hybrid_store_num = 0
    zk = KazooClient(hosts=zk_host, read_only=True)
    zk.start()
    for cluster in venice_clusters:
        # construct zk client
        store_num, hybrid_store_num = hybrid_store_finder(cluster, zk, zk_path_prefix)
        total_store_num += store_num
        total_hybrid_store_num += hybrid_store_num

    print("Total store number:%d, hybrid store number: %d" % (total_store_num, total_hybrid_store_num))

    zk.stop()
    zk.close()


if __name__ == '__main__':
    read_config()
