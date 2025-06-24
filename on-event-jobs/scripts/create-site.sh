#!/bin/bash 

# Echo all commands before execute
#set -x
# Interrupt and exit on any error
set -e

# Environmental variables required:
#   SITE_CODE
#   SITE_NAME

echo "Running scripts to create the site '${SITE_CODE}'."
echo "SITE_NAME=${SITE_NAME}"

# Add here some command

echo "End of scripts to create the site."
