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


echo "Running scripts to create the user '${TENANT_NAME}'."
echo "ROLES=${TENANT_ROLES}"

if echo ${TENANT_ROLES} | grep -i "data-scientist" > /dev/null; then
    KUBECTL_CMD="kubectl --server ${K8S_ENDPOINT} --insecure-skip-tls-verify=true --token=${K8S_TOKEN}"
    TENANT_NAMESPACE="user-${TENANT_NAME}"
    echo "NAMESPACE=${TENANT_NAMESPACE}"
    
    echo -e "\n##############################################################################"
    echo "=============== Create the k8s namespace for the user"
    if ${KUBECTL_CMD} get namespace ${TENANT_NAMESPACE}; then 
        echo "The namespace already exists."
    else
        ${KUBECTL_CMD} create namespace ${TENANT_NAMESPACE}
    fi
    
fi

echo "End of scripts to create the user '${TENANT_NAME}'."
