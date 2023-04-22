
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import logging
import json
from dataset_service.config import CONFIG_ENV_VAR_NAME

def _get_deployment_of_dataset_service_backend(namespace):
    API = client.AppsV1Api()
    return API.read_namespaced_deployment("dataset-service-backend", namespace)

def create_job(job_name, datasetId):
    config.load_incluster_config()
    current_namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
    API = client.BatchV1Api()

    deployment = _get_deployment_of_dataset_service_backend(current_namespace)
    volumes = deployment.spec.template.spec.volumes
    container_image = deployment.spec.template.spec.containers[0].image
    container_volume_mounts = deployment.spec.template.spec.containers[0].volume_mounts
    job_config = dict()
    for env_var in deployment.spec.template.spec.containers[0].env:
        if env_var.name == CONFIG_ENV_VAR_NAME:
            main_service_config = json.loads(env_var.value)
            job_config["db"] = main_service_config["db"]
            job_config["tracer"] = main_service_config["tracer"]
            job_config["self"] = main_service_config["self"]
            break

    body = client.V1Job(
        api_version='batch/v1',
        kind='Job',
        metadata = client.V1ObjectMeta(name=job_name),
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
                    restart_policy = "Never"
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
    