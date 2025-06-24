#!/bin/bash 

# Echo all commands before execute
#set -x
# Interrupt and exit on any error
set -e

# Environmental variables required:
#   TENANT_NAME
#   TENANT_ROLES
#   K8S_ENDPOINT
#   K8S_TOKEN


echo "Running scripts to delete the user '${TENANT_NAME}'."
echo "ROLES=${TENANT_ROLES}"

if echo ${TENANT_ROLES} | grep -i "data-scientist" > /dev/null; then
    KUBECTL_CMD="kubectl --server ${K8S_ENDPOINT} --insecure-skip-tls-verify=true --token=${K8S_TOKEN}"
    TENANT_NAMESPACE="user-${TENANT_NAME}"
    echo "NAMESPACE=${TENANT_NAMESPACE}"
    
    echo -e "\n##############################################################################"
    echo "=============== Delete the k8s namespace of the user"
    ${KUBECTL_CMD} delete namespace ${TENANT_NAMESPACE}

fi

echo "End of scripts to delete the user '${TENANT_NAME}'."
