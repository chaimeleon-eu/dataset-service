#!/bin/bash 

# Echo all commands before execute
#set -x
# Interrupt and exit on any error
set -e

# Environmental variables required:
#   PROJECT_CODE
#   SUBPROJECT_CODE
#   SUBPROJECT_NAME

echo "Running scripts to create the subproject '${PROJECT_CODE}-${SUBPROJECT_CODE}'."
echo "SUBPROJECT_NAME=${SUBPROJECT_NAME}"

# Add here some command

echo "End of scripts to create the subproject."
