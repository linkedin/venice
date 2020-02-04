'''
A script that scans the venice ZK metadata to get the number of hybrid store.
Currently, it scans all Prod clusters (0-11, venice-p is excluded). If you want
to query other clusters, you could either change "zk_host" and "venice_clusters",
or use "readConfig(cluster, host)" to pass them from command line.

It's can be also used as a template for any Venice ZK lookup operations.

prerequisite: "pip install click" and "pip install kazoo"
TODO: move it SRE's tool kit MP
'''

import click
import json
from kazoo.client import KazooClient
import logging

system_store_signature = "system_store"

#SSH tunnel port
#Since there is firewall between biz and Prod, a SSH
#tunnel might be needed if it's scraping Prod ZK.
#e.g. ssh -nNT -L 2299:zk-lsg1-venice.prod.linkedin.com:2622 ${prod_host}
zk_host = 'localhost:2299'

#a list of Venice cluster in Prod from venice-0 to venice-11. venice-p is excluded.
venice_clusters = ['venice-' + str(x) for x in range(12)]


#unused
#click is a handy helper library to read params from commandline
#e.g. python store.py --host=localhost:2299 --cluster=venice-0
@click.command()
@click.option("--cluster")
@click.option("--host")
def readConfig(cluster, host):
    return cluster, host

#Scan the stores in a cluster to find out all hybrid stores
#cluster: Venice cluster name
#host: ZK host address
#zk: zk client. It's an optional param. If it's none, it create a new one
def hybrid_store_finder(cluster, host=None, zk=None):
    logging.basicConfig()

    if (zk == None):
        zk = KazooClient(hosts=host, read_only=True)
    zk.start()

    stores = zk.get_children("/venice/%s/Stores" % cluster)
    print("There are %s stores in the cluster: %s" % (len(stores), cluster))

    regular_hybrid_store_num = 0
    pure_streaming_store_num = 0
    system_store_num = 0
    for store in stores:
        data, stat = zk.get("/venice/%s/Stores/%s" %(cluster, store))
        store_metadata = json.loads(data)
        if (store_metadata['hybridStoreConfig'] != None):
            store_name = store_metadata['name']
            if system_store_signature in store_name:
                system_store_num += 1
                print("stystem store: " + store_name)
            else:
                #this is a rough estimate. If a store has less than 10 versions, it's likely that it's a streaming-only store
                if (store_metadata['currentVersion'] < 10):
                    pure_streaming_store_num += 1
                    print("pure streaming store: " + store_name)
                else:
                    regular_hybrid_store_num += 1
                    print("regular hybrid store: " + store_name)



    print("Hybrid store number: %d\nPure streaming store number: %d\nSystem store number: %d" % (regular_hybrid_store_num + pure_streaming_store_num, pure_streaming_store_num, system_store_num))

    zk.stop()

    return len(stores), regular_hybrid_store_num + pure_streaming_store_num

#iterate the cluster list and scan all the stores under these clusters
def main():
    total_store_num = 0
    total_hybrid_store_num = 0
    for cluster in venice_clusters:
        #construct zk client
        zk = KazooClient(hosts=zk_host, read_only=True)

        store_num, hybrid_store_num = hybrid_store_finder(cluster, None, zk)
        total_store_num += store_num
        total_hybrid_store_num += hybrid_store_num

    print("Total store number:%d, hybrid store number: %d" % (total_store_num, total_hybrid_store_num))

if __name__ == '__main__':
    main()