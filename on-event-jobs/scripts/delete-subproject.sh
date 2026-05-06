#!/bin/bash 

# Echo all commands before execute
#set -x
# Interrupt and exit on any error
set -e

# Environmental variables required:
#   PROJECT_CODE
#   SUBPROJECT_CODE
#   SUBPROJECT_EXTERNAL_ID

echo "Running scripts to delete the subproject '${PROJECT_CODE}-${SUBPROJECT_CODE}'."
echo "SUBPROJECT_EXTERNAL_ID=${SUBPROJECT_EXTERNAL_ID}"

# Add here some command

echo "End of scripts to delete the subproject."
