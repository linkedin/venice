#!/usr/bin/env python3

# This file is inspired by the following script in Apache Pulsar (Apache License 2.0)
# https://github.com/apache/pulsar/blob/master/docker/pulsar/scripts/apply-config-from-env-with-prefix.py

############################################################
# Edit a properties config file and replace values based on
# the ENV variables
# export prefix_my-key=new-value
# ./apply-config-from-env-with-prefix prefix_ file.conf
#
# Environment variables that are prefixed with the command
# line prefix will be used to updated file properties if
# they exist and create new ones if they don't.
#
# Environment variables not prefixed will be used only to
# update if they exist and ignored if they don't.
#
# Venice uses variables with dot notation, but k8s and bash
# don't play well with dots, so the script replaces underscores with dots.

# For instance:
# listener.port=7777
# 
# This is mapped to
# prefix_listener_port=7777
#
############################################################

import os
import sys

if len(sys.argv) < 3:
    print('Usage: %s <PREFIX> <FILE> [<FILE>...]' % (sys.argv[0]))
    sys.exit(1)

# Always apply env config to env scripts as well
prefix = sys.argv[1]
conf_files = sys.argv[2:]

PF_ENV_DEBUG = (os.environ.get('PF_ENV_DEBUG','0') == '1')

for conf_filename in conf_files:
    lines = []  # List of config file lines
    keys = {} # Map a key to its line number in the file

    # Load conf file
    for line in open(conf_filename):
        lines.append(line)
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        try:
            k,v = line.split('=', 1)
            keys[k] = len(lines) - 1
        except:
            if PF_ENV_DEBUG:
                print("[%s] skip Processing %s" % (conf_filename, line))

    # Update values from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k].strip()

        # Hide the value in logs if is password.
        if "password" in k.lower():
            displayValue = "********"
        else:
            displayValue = v
        if k.startswith(prefix):
            k = k[len(prefix):].replace("_", ".")
        if k in keys:
            print('[%s] Applying config %s = %s' % (conf_filename, k, displayValue))
            idx = keys[k]
            lines[idx] = '%s=%s\n' % (k, v)


    # Ensure we have a new-line at the end of the file, to avoid issue
    # when appending more lines to the config
    lines.append('\n')

    # Add new keys from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k]
        if not k.startswith(prefix):
            continue

        # Hide the value in logs if is password.
        if "password" in k.lower():
            displayValue = "********"
        else:
            displayValue = v

        k = k[len(prefix):].replace("_", ".")
        if k not in keys:
            print('[%s] Adding config %s = %s' % (conf_filename, k, displayValue))
            lines.append('%s=%s\n' % (k, v))
        else:
            print('[%s] Updating config %s = %s' % (conf_filename, k, displayValue))
            lines[keys[k]] = '%s=%s\n' % (k, v)


    # Store back the updated config in the same file
    f = open(conf_filename, 'w')
    for line in lines:
        f.write(line)
    f.close()

