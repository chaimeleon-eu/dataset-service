
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import logging
import json
from dataset_service.config import CONFIG_ENV_VAR_NAME

DATASET_CREATION_JOB_PREFIX="creating-dataset-"

def _get_deployment_of_dataset_service_backend(namespace):
    API = client.AppsV1Api()
    return API.read_namespaced_deployment("dataset-service-backend", namespace)

def add_dataset_creation_job(datasetId):
    config.load_incluster_config()
    current_namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
    API = client.BatchV1Api()

    deployment = _get_deployment_of_dataset_service_backend(current_namespace)
    volumes = deployment.spec.template.spec.volumes
    container_image = deployment.spec.template.spec.containers[0].image
    container_volume_mounts = deployment.spec.template.spec.containers[0].volume_mounts
    node_selector = deployment.spec.template.spec.node_selector
    priority_class_name = deployment.spec.template.spec.priority_class_name
    job_config = dict()
    for env_var in deployment.spec.template.spec.containers[0].env:
        if env_var.name == CONFIG_ENV_VAR_NAME:
            main_service_config = json.loads(env_var.value)
            job_config["db"] = main_service_config["db"]
            job_config["auth"] = dict()
            job_config["auth"]["client"] = main_service_config["auth"]["client"]
            job_config["tracer"] = main_service_config["tracer"]
            job_config["self"] = main_service_config["self"]
            break

    body = client.V1Job(
        api_version='batch/v1',
        kind='Job',
        metadata = client.V1ObjectMeta(name = DATASET_CREATION_JOB_PREFIX + datasetId),
        spec = client.V1JobSpec(
            backoff_limit = 0,   # no retries
            ttl_seconds_after_finished = 86400,   # 24 hours after finish to remove the job
            template = client.V1PodTemplateSpec(
                spec = client.V1PodSpec(
                    volumes= volumes,
                    containers = [ 
                        client.V1Container(
                            image = container_image,
                            name = "dataset-creation", 
                            volume_mounts = container_volume_mounts,
                            #image_pull_policy = "Always",
                            command = [ "/usr/bin/python3" ], 
                            args = ["start_dataset_creation_job.py", datasetId],
                            env = [
                                client.V1EnvVar(name = CONFIG_ENV_VAR_NAME, value = json.dumps(job_config) )
                            ]
                        )
                    ],
                    restart_policy = "Never",
                    node_selector = node_selector,
                    priority_class_name = priority_class_name
                )
            )
        ) 
    )

    try:
        api_response = API.create_namespaced_job(current_namespace, body) # field_manager=CONFIG.self.name)
        #logging.root.debug(api_response)
    except ApiException as e:
        logging.root.error("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)
        return False
    return True
    
def delete_dataset_creation_job(datasetId):
    config.load_incluster_config()
    current_namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
    API = client.BatchV1Api()

    try:
        # propagation_policy='Foreground' to include the deletion of the pod
        api_response = API.delete_namespaced_job(DATASET_CREATION_JOB_PREFIX + datasetId, current_namespace, propagation_policy='Foreground')
        #logging.root.debug(api_response)
    except ApiException as e:
        if e.status == 404: 
            logging.root.debug("The job is already deleted or finished.")
            return True
        logging.root.error("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
        return False
    return True
