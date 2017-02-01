#!/usr/bin/env bash

# You must source this file instead of just executing it so that it will properly set the environment variable.
# It will ask for your LDAP password to generate a key, then set an ENV variable so venice can find that key

id-tool grestin sign
export VENICE_KEYSTORE=$(pwd)/identity.p12