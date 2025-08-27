'''
A script that scans the Venice ZK metadata to get the current version of all Venice stores.

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
import subprocess
import re
from kazoo.client import KazooClient


# click is a handy helper library to read params from commandline
# python current_version_topic_check.py --host "localhost:2299" --cluster "cluster-0" --cluster "cluster-1" --zk-path-prefix "/venice" --fabric "ei4"
@click.command()
@click.option('--cluster', '-c', multiple=True)
@click.option('--host', '-H')
@click.option('--zk-path-prefix')
@click.option('--fabric', '-f', required=True, help='Fabric name for kafka-tool command')
def read_config(cluster, host, zk_path_prefix, fabric):
    if host is None:
        host = 'localhost:2299'
    if zk_path_prefix is None:
        zk_path_prefix = '/venice'
    check_current_version_topics(host, cluster, zk_path_prefix, fabric)


# Scan all stores in a cluster to get their current versions and compose topic names
# cluster: Venice cluster name
# host: ZK host address
# zk: zk client. It's an optional param. If it's none, it create a new one
def get_current_version_topics(cluster, zk, zk_path_prefix):
    logging.basicConfig()

    stores = zk.get_children("%s/%s/Stores" % (zk_path_prefix, cluster))
    print("Cluster: %s - Total stores: %d" % (cluster, len(stores)))
    print("-" * 50)

    current_version_topics = []

    for store in stores:
        data, stat = zk.get("%s/%s/Stores/%s" % (zk_path_prefix, cluster, store))
        store_metadata = json.loads(data)

        store_name = store_metadata['name']
        current_version = store_metadata.get('currentVersion', None)

        if current_version is not None and current_version != 'N/A' and current_version != 0:
            topic_name = "%s_v%s" % (store_name, current_version)
            current_version_topics.append(topic_name)
            print("Store: %s | Current Version: %s | Topic: %s" % (store_name, current_version, topic_name))
        elif current_version == 0:
            print("Store: %s | Current Version: 0 | Topic: Skipped (version 0)" % store_name)
        else:
            print("Store: %s | Current Version: N/A | Topic: N/A" % store_name)

    print("-" * 50)
    print()
    return current_version_topics


# Get list of existing Kafka topics using kafka-tool
def get_kafka_topics(fabric):
    try:
        print("Fetching Kafka topics from fabric: %s" % fabric)
        cmd = ["kafka-tool", "topic", "list", "-c", "venice", "-f", fabric]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        if result.returncode != 0:
            print("Error running kafka-tool command: %s" % result.stderr)
            return []

        # Parse the output to extract topic names
        kafka_topics = []
        for line in result.stdout.split('\n'):
            # Look for lines with format: (name=topic_name, internal=false)
            match = re.search(r'\(name=([^,]+),\s*internal=(true|false)\)', line)
            if match:
                topic_name = match.group(1)
                is_internal = match.group(2) == 'true'
                if not is_internal:  # Only include non-internal topics
                    kafka_topics.append(topic_name)

        print("Found %d non-internal Kafka topics" % len(kafka_topics))
        return kafka_topics

    except subprocess.TimeoutExpired:
        print("Timeout: kafka-tool command took too long to execute")
        return []
    except Exception as e:
        print("Error executing kafka-tool command: %s" % str(e))
        return []


# Main function to check current version topics against Kafka topics
def check_current_version_topics(zk_host, venice_clusters, zk_path_prefix, fabric):
    # Step 1: Get all current version topics from Venice clusters
    all_current_version_topics = []
    zk = KazooClient(hosts=zk_host, read_only=True)
    zk.start()

    for cluster in venice_clusters:
        cluster_topics = get_current_version_topics(cluster, zk, zk_path_prefix)
        all_current_version_topics.extend(cluster_topics)

    zk.stop()
    zk.close()

    print("Total current version topics from Venice: %d" % len(all_current_version_topics))
    print("=" * 70)

    # Step 2: Get existing Kafka topics
    kafka_topics = get_kafka_topics(fabric)
    kafka_topics_set = set(kafka_topics)

    print("=" * 70)

    # Step 3: Find missing topics
    missing_topics = []
    for topic in all_current_version_topics:
        if topic not in kafka_topics_set:
            missing_topics.append(topic)

    # Report results
    if missing_topics:
        print("\nMISSING CURRENT VERSION TOPICS IN KAFKA:")
        print("=" * 50)
        for topic in missing_topics:
            print("Missing: %s" % topic)
        print("\nTotal missing topics: %d" % len(missing_topics))
    else:
        print("\nAll current version topics exist in Kafka!")

    print("\nSummary:")
    print("- Venice current version topics: %d" % len(all_current_version_topics))
    print("- Kafka topics (non-internal): %d" % len(kafka_topics))
    print("- Missing topics: %d" % len(missing_topics))


if __name__ == '__main__':
    read_config()
