'''
A script that scans the Venice ZK metadata to get the distributions of znodes
in different Venice clusters.

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
import logging
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

# a list of Venice clusters
common_zk_path = ['AdminTopicOffsetRecord', 'CONFIGS', 'CONTROLLER', 'EXTERNALVIEW', 'IDEALSTATES', 'INSTANCES',
                  'LIVEINSTANCES', 'OfflineJobs', 'OfflinePushes', 'PROPERTYSTORE', 'StoreGraveyard', 'Stores',
                  'adminTopicMetadata', 'executionids', 'routers']


# click is a handy helper library to read params from commandline
# python zk_summary.py --host "localhost:2299" --cluster "cluster-0" --cluster "cluster-1" --zk-path-prefix "/venice"
@click.command()
@click.option('--cluster', '-c', multiple=True)
@click.option('--host', '-H')
@click.option('--zk-path-prefix')
def read_config(cluster, host, zk_path_prefix):
    if host is None:
        host = 'localhost:2299'
    if zk_path_prefix is None:
        zk_path_prefix = '/venice'
    get_zk_summary(host, cluster, zk_path_prefix)


# A helper function to count all znode within "current_path".
# "depth" is an extra parameter that can improve the speed of this function;
# if the function caller knows that max recursion depth of a zk path, a positive
# max "depth" number can be used to speed up the recursion.
def get_children_zk_count(current_path, zk=None, depth=0):
    logging.basicConfig()

    try:
        children = zk.get_children(current_path)
    except NoNodeError:
        print("ZK path %s doesn't exist." % (current_path))
        return 0
    counter = len(children)
    if depth == 0:
        return counter

    last_logged_number = 0
    for child in children:
        counter += get_children_zk_count(current_path + "/" + child, zk, depth - 1)
        if counter - last_logged_number > 5000:
            last_logged_number = counter
            print("Scanned %d znodes at ZK path %s" % (counter, current_path))
    return counter


def get_cluster_detailed_zk_count(cluster, zk=None):
    count_map = {}
    print("Scanning zkpath: " + cluster + "/AdminTopicOffsetRecord")
    AdminTopicOffsetRecord = get_children_zk_count(cluster + "/AdminTopicOffsetRecord", zk, -1)
    count_map['AdminTopicOffsetRecord'] = AdminTopicOffsetRecord

    print("Scanning zkpath: " + cluster + "/CONFIGS")
    CONFIGS = get_children_zk_count(cluster + "/CONFIGS", zk, -1)
    count_map['CONFIGS'] = CONFIGS

    print("Scanning zkpath: " + cluster + "/CONTROLLER")
    CONTROLLER = get_children_zk_count(cluster + "/CONTROLLER", zk, -1)
    count_map['CONTROLLER'] = CONTROLLER

    print("Scanning zkpath: " + cluster + "/EXTERNALVIEW")
    EXTERNALVIEW = get_children_zk_count(cluster + "/EXTERNALVIEW", zk, 0)
    count_map['EXTERNALVIEW'] = EXTERNALVIEW

    print("Scanning zkpath: " + cluster + "/IDEALSTATES")
    IDEALSTATES = get_children_zk_count(cluster + "/IDEALSTATES", zk, 0)
    count_map['IDEALSTATES'] = IDEALSTATES

    print("Scanning zkpath: " + cluster + "/INSTANCES")
    INSTANCES = get_children_zk_count(cluster + "/INSTANCES", zk, 2)
    count_map['INSTANCES'] = INSTANCES

    print("Scanning zkpath: " + cluster + "/LIVEINSTANCES")
    LIVEINSTANCES = get_children_zk_count(cluster + "/LIVEINSTANCES", zk, -1)
    count_map['LIVEINSTANCES'] = LIVEINSTANCES

    print("Scanning zkpath: " + cluster + "/OfflineJobs")
    OfflineJobs = get_children_zk_count(cluster + "/OfflineJobs", zk, 1)
    count_map['OfflineJobs'] = OfflineJobs

    print("Scanning zkpath: " + cluster + "/OfflinePushes")
    OfflinePushes = get_children_zk_count(cluster + "/OfflinePushes", zk, 1)
    count_map['OfflinePushes'] = OfflinePushes

    print("Scanning zkpath: " + cluster + "/PROPERTYSTORE")
    PROPERTYSTORE = get_children_zk_count(cluster + "/PROPERTYSTORE", zk, -1)
    count_map['PROPERTYSTORE'] = PROPERTYSTORE

    print("Scanning zkpath: " + cluster + "/StoreGraveyard")
    StoreGraveyard = get_children_zk_count(cluster + "/StoreGraveyard", zk, -1)
    count_map['StoreGraveyard'] = StoreGraveyard

    print("Scanning zkpath: " + cluster + "/Stores")
    Stores = get_children_zk_count(cluster + "/Stores", zk, 2)
    count_map['Stores'] = Stores

    print("Scanning zkpath: " + cluster + "/adminTopicMetadata")
    adminTopicMetadata = get_children_zk_count(cluster + "/adminTopicMetadata", zk, -1)
    count_map['adminTopicMetadata'] = adminTopicMetadata

    print("Scanning zkpath: " + cluster + "/executionids")
    executionids = get_children_zk_count(cluster + "/executionids", zk, -1)
    count_map['executionids'] = executionids

    print("Scanning zkpath: " + cluster + "/routers")
    routers = get_children_zk_count(cluster + "/routers", zk, -1)
    count_map['routers'] = routers

    return count_map


# iterate the cluster list and scan all the znode under those clusters
def get_zk_summary(zk_host, venice_clusters, zk_path_prefix):
    zk = KazooClient(hosts=zk_host, read_only=True)
    zk.start()

    cluster_znode_distributions = {}
    cluster_znode_detailed_distributions = {}
    common_path_znode_distributions = {}
    for zk_path in common_zk_path:
        common_path_znode_distributions[zk_path] = 0

    for cluster in venice_clusters:
        current = zk_path_prefix + "/" + cluster
        cluster_map = get_cluster_detailed_zk_count(current, zk)
        print('\n')
        print("ZK distribution for cluster " + cluster)
        print(cluster_map)
        cluster_znode_detailed_distributions[cluster] = cluster_map
        cluster_count = 0
        for zk_path in common_zk_path:
            cluster_count += cluster_map[zk_path]
            common_path_znode_distributions[zk_path] = common_path_znode_distributions[zk_path] + cluster_map[zk_path]
        print("Total zk count for cluster %s is %d." % (cluster, cluster_count))
        print('\n')
        cluster_znode_distributions[cluster] = cluster_count
    print(cluster_znode_distributions)
    print(common_path_znode_distributions)
    print('\n')

    print('[ZK node distribution summary]')
    print('[1] Cluster level distribution:')
    for k, v in cluster_znode_distributions.items():
        print("%s: %d" % (k, v))
    print('\n[2] Common path distributions among all clusters:')
    for k, v in common_path_znode_distributions.items():
        print("%s: %d" % (k, v))
    print('\n[3] Detailed znode distributions for cluster with most znode:')
    cluster_with_most_znode = ""
    max_count = 0
    for k, v in cluster_znode_detailed_distributions.items():
        if cluster_znode_distributions[k] > max_count:
            max_count = cluster_znode_distributions[k]
            cluster_with_most_znode = k
    print('%s:' % (cluster_with_most_znode))
    print(cluster_znode_detailed_distributions[cluster_with_most_znode])
    print('\n')

    zk.stop()
    zk.close()


if __name__ == '__main__':
    read_config()
