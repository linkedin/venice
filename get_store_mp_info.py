#!/usr/bin/env python3

"""To use:

$ python3 -m venv myvenv
$ source myvenv/bin/activate

Then run this script from the virtual environment
"""
import argparse
import json
import logging
import subprocess
import sys
import textwrap


from dns.exception import DNSException
from kazoo.client import KazooClient


def store_finder(cluster, host=None, zk=None, is_hybrid=True):
    system_store_signature = "system_store"
    logging.basicConfig()

    if (zk == None):
        zk = KazooClient(hosts=host, read_only=True)
    zk.start()

    stores = zk.get_children("/venice/%s/Stores" % cluster)

    store_infos = []
    for store in stores:
        data, stat = zk.get("/venice/%s/Stores/%s" %(cluster, store))
        store_metadata = json.loads(data)
        store_name = store_metadata['name']
        if is_hybrid:
            if hybrid_store_checker(store_metadata):
                store_infos.append(store_name + "," + cluster)
        else:
            store_infos.append(store_name + "," + cluster)
    zk.stop()
    print("There are %s stores in the cluster: %s, %s stores found as required." % (len(stores), cluster, len(store_infos)))
    return len(stores), store_infos

def hybrid_store_checker(store_metadata):
    system_store_signature = "system_store"
    if (store_metadata['hybridStoreConfig'] != None):
        store_name = store_metadata['name']
        if system_store_signature not in store_name:
            return True
    return False

def get_stores_by_requirement(store_list_filename, is_hybrid):
    total_store_num = 0
    total_hybrid_store_num = 0
    venice_clusters = ['venice-' + str(x) for x in range(12)]
    all_hybrid_stores = []
    for cluster in venice_clusters:
        # Construct zk client
        zk = KazooClient(hosts="localhost:2299", read_only=True)
        store_num, hybrid_stores = store_finder(cluster, None, zk, is_hybrid)
        total_store_num += store_num
        total_hybrid_store_num += len(hybrid_stores)
        all_hybrid_stores.extend(hybrid_stores)
    with open(store_list_filename, 'w') as f:
        for store in all_hybrid_stores:
            f.write(store + '\n')
    print("Total store number:%d, hybrid store number: %d" % (total_store_num, total_hybrid_store_num))


def get_store_mps_from_list(list_file_name, remote_server, remote_parent_controller, local_tool_path, store_mp_map_path, is_write):
    store_mps_info = {}
    admin_tool_setup_for_remote_server(remote_server, local_tool_path)
    with open(list_file_name, 'r') as f:
        lines = f.readlines()
        for line in lines:
            elements = line.split(',')
            store_name, cluster_name = elements[0].strip(), elements[1].strip()
            mp_names, acls  = get_store_mp_name(store_name, cluster_name, remote_server, remote_parent_controller, is_write)
            store_mps_info[store_name] = (cluster_name, mp_names, acls)
            if is_write:
                print(store_name + "," + cluster_name + "\n\tMPs pushing to it:" + str(mp_names) + "\n\tWrite acls: " + str(acls))
            else:
                print(store_name + "," + cluster_name + "\n\tMPs reading from it:" + str(mp_names) + "\n\tRead acls: " + str(acls))
    with open(store_mp_map_path, 'w') as f:
        json.dump(store_mps_info, f)


def print_store_without_acl(list_file_name, mp_json_file_name):
    with open(mp_json_file_name) as json_file:
        store_mps_info = json.load(json_file)
        with open(list_file_name, 'r') as f:
            lines = f.readlines()
            for line in lines:
                elements = line.split(',')
                store_name, cluster_name = elements[0].strip(), elements[1].strip()
                cluster_name, mp_names, write_acls = store_mps_info[store_name]
                if len(write_acls) == 0:
                    print(store_name, "No acls found.")


def admin_tool_setup_for_remote_server(remote_server, local_tool_path):
    # Add logic here to check the jar file in the controller server or not. If not copy jar file to it.
    check_file_cmd = f"ssh {remote_server} 'test -f /var/tmp/venice-admin-tool-0.1-fat.jar && echo 'exist''"
    output = run_command(check_file_cmd)
    if "exist" not in output:
        # copy file to remote controller server
        print("jar file does not exist, so copy from local.")
        copy_file_cmd = f"scp {local_tool_path} {remote_server}:/var/tmp/venice-admin-tool-0.1-fat.jar"
        output = run_command(copy_file_cmd)

def get_store_mp_name(store_name, cluster_name, remote_server, remote_parent_controller, is_write):
    acl_cmd = f"ssh {remote_server} 'java -jar /var/tmp/venice-admin-tool-0.1-fat.jar --get-store-acl --url http://{remote_parent_controller}:1576 --cluster {cluster_name} --store {store_name}'"
    output = run_command(acl_cmd)
    lines = output.split('\n')
    for i in range(len(lines)):
        if lines[i].startswith('{'):
            break

    json_str = '\n'.join(lines[i:])
    acl_result = json.loads(json_str)
    if not acl_result["accessPermissions"]:
        return [], []
    access_permissions = json.loads(acl_result["accessPermissions"])

    acls = access_permissions["AccessPermissions"]["Write"]
    if is_write:
        acls = access_permissions["AccessPermissions"]["Read"]

    # Filter out application level acl
    mp_names = []
    for acl in acls:
        if acl.startswith('urn:li:servicePrincipal'):
            mp_name = obtain_mp_name_from_acl(acl.split(":")[-1])
            if mp_name not in ['No'] and mp_name not in mp_names:
                mp_names.append(mp_name)
    return mp_names, acls

def run_command(command):
    status, output = subprocess.getstatusoutput(command)
    if status:
        # if raise exception here, following logic after this command will not be executed.
        print("command warning:\n" + output)
    return output

def obtain_mp_name_from_acl(acl):
    cmd_line = "go-status -f prod -a %s --index=p:a | head -n 1 | awk \'{ print $1 }\'" % acl
    output = run_command(cmd_line)
    return output.strip()

if __name__ == "__main__":

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawTextHelpFormatter,
    description=textwrap.dedent(
      """
      Run this tool to get store mulitproduct information.
      For this script we need to run on dev box with python3.
      Since there is firewall between biz and Prod, a SSH tunnel might be needed if it's scraping Prod ZK.
      Command: e.g. ssh -nNT -L 2299:zk-lor1-venice.prod.linkedin.com:2622 ${prod_host}
      Usually choose the prod_host from controller servers.
      This tool can be modified to find other type of stores and corresponding multiproducts.
      python ~/Projects/venice/get_store_mp_info.py  -p "./" --remote "lva1-app19053.prod.linkedin.com"
          --tool "~/Projects/venice/venice-admin-tool/build/libs/venice-admin-tool-0.1-fat.jar" --hybrid true --write false
          --parent "lva1-app28390.prod.linkedin.com"
      """))

  parser.add_argument("-p", "--path", help="Paths for saving store and multiproduct information.")
  parser.add_argument("--hybrid", help="Just search for hybrid stores or not. true: just hybrid stores. false: all stores.")
  parser.add_argument("-w", "--write", help="Check the write ACL or READ ACL. true: Write. false: Read.")
  parser.add_argument("-r", "--remote", help="Remote server to execute admin tool command.")
  parser.add_argument("-pc", "--parent", help="Parent controller server for admin tool command.")
  parser.add_argument("-t", "--tool", help="Local admin tool jar file path.")
  args = vars(parser.parse_args())

  remote_server = args["remote"]
  remote_parent_controller = args["parent"]

  if args["path"]:
    path = args["path"]
    admin_tool_path = args["tool"]
    is_hybrid = 'true' in args["hybrid"]
    is_write = 'true' in args["write"]
    store_list_path = path + "/store_list" # The store list format will be like: store, cluster\n store, cluster\n ...
    get_stores_by_requirement(store_list_path, is_hybrid)
    store_mp_map_path = path + "/store_mp.json"
    get_store_mps_from_list(store_list_path, remote_server, remote_parent_controller, admin_tool_path, store_mp_map_path, is_write)
    print_store_without_acl(store_list_path, store_mp_map_path)