
from enum import Enum
import logging.config
from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException
import logging
import json, yaml
import uuid
from dataset_service.config import CONFIG_ENV_VAR_NAME

DATASET_CREATION_JOB_PREFIX="creating-dataset-"
USER_CREATION_JOB_PREFIX="creating-user-"

class Job_state(Enum):
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    UNKNOWN = 'unknown'

class K8sClient:
    def __init__(self):
        config.load_incluster_config()
        self.namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
        logging.getLogger('kubernetes').setLevel(logging.WARN)

    def _get_deployment_of_dataset_service_backend(self):
        API = client.AppsV1Api()
        return API.read_namespaced_deployment("dataset-service-backend", self.namespace)

    def add_dataset_creation_job(self, datasetId):
        API = client.BatchV1Api()
        deployment = self._get_deployment_of_dataset_service_backend()
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
            api_response = API.create_namespaced_job(self.namespace, body) # field_manager=CONFIG.self.name)
            #logging.root.debug(api_response)
        except ApiException as e:
            logging.root.error("Exception when calling BatchV1Api->create_namespaced_job: %s\n" % e)
            return False
        return True

    def exist_dataset_creation_job(self, datasetId):
        API = client.BatchV1Api()
        try:
            job = API.read_namespaced_job(DATASET_CREATION_JOB_PREFIX + datasetId, self.namespace)
        except ApiException as e:
            if e.status == 404: 
                return None
            else: raise e
        return job

    def is_running_job(self, job):
        return not hasattr(job.status, 'succeeded') and not hasattr(job.status, 'failed')

    def _get_state_of_job(self, job) -> Job_state:
        # Assuming it is a job with only one pod, 
        # let's see if exists the attribute with the number of pods in each state to obtain the state of the job.
        if hasattr(job.status, 'active') and job.status.active: return Job_state.RUNNING
        if hasattr(job.status, 'succeeded') and job.status.succeeded: return Job_state.SUCCEEDED
        if hasattr(job.status, 'failed') and job.status.failed: return Job_state.FAILED
        return Job_state.UNKNOWN

    def delete_dataset_creation_job(self, datasetId):
        API = client.BatchV1Api()
        try:
            # propagation_policy='Foreground' to include the deletion of the pod
            api_response = API.delete_namespaced_job(DATASET_CREATION_JOB_PREFIX + datasetId, self.namespace, propagation_policy='Foreground')
            #logging.root.debug(api_response)
        except ApiException as e:
            if e.status == 404: 
                logging.root.debug("The job is already deleted or finished.")
                return True
            logging.root.error("Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
            return False
        return True


    def add_user_creation_job(self, username: str, roles: list[str], site: str, projects: list[str], job_template_file_path: str):
        API = client.ApiClient()

        projects_param = ":".join(projects)
        roles_param = ":".join(roles)
        try:
            with open(job_template_file_path) as f:
                job_template = f.read()
            job_template = job_template.replace("__OPERATION__", "create")
            job_template = job_template.replace("__TENANT_USERNAME__", username)
            job_template = job_template.replace("__TENANT_ROLES__", roles_param)
            job_template = job_template.replace("__TENANT_SITE__", site)
            job_template = job_template.replace("__TENANT_PROJECTS__", projects_param)
            job = yaml.load(job_template, Loader=yaml.SafeLoader)
        except:
            logging.root.error("ERROR: User management job template file not found or cannot be loaded from " + job_template_file_path)
            return False
        logging.root.info("User management job template loaded from file: " + job_template_file_path)
        if not "metadata" in job or not  isinstance(job["metadata"], dict): job["metadata"] = {}
        random_uuid = str(uuid.uuid4())[:8]
        job['metadata']['name'] = USER_CREATION_JOB_PREFIX + username + "-" + random_uuid
        job['metadata']['namespace'] = self.namespace
        job['metadata']['labels'] = {
            'job-type': 'user-creation',
            'username': username
        }
        try:
            api_response = utils.create_from_yaml(API,  yaml_objects=[ job ])
            #logging.root.debug(api_response)
        except ApiException as e:
            logging.root.error("Exception when calling k8s API -> create_from_yaml: %s\n" % e)
            return False
        return True

    def list_user_creation_jobs(self, username: str):
        API = client.BatchV1Api()
        creationJobs = []
        try:
            jobList = API.list_namespaced_job(self.namespace, label_selector="job-type=user-creation, username="+username)
            for j in jobList.items:
                #if str(j.metadata.name).startswith(USER_CREATION_JOB_PREFIX + username + "-"):
                creationJobs.append({
                    "creationDate": j.metadata.creation_timestamp.isoformat(),
                    "name": j.metadata.name,
                    "uid": j.spec.selector.match_labels["batch.kubernetes.io/controller-uid"],  # j.metadata.labels["controller-uid"]
                    "status": str(self._get_state_of_job(j).value)
                })
            creationJobs.sort(key=lambda x:x["creationDate"], reverse=True)
        except ApiException as e:
            logging.root.error("Exception when calling k8s API -> list_user_creation_jobs: %s\n" % e)
            raise e
        return creationJobs
    
    def _get_name_of_first_pod_of_job(self, api, job_selector_controller_uid) -> str | None:
        pod_list = api.list_namespaced_pod(self.namespace, label_selector='controller-uid=' + job_selector_controller_uid)
        if len(pod_list.items) == 0: return None
        return pod_list.items[0].metadata.name

    def get_user_creation_job_logs(self, username, selectorUid) -> str | None:
        API = client.CoreV1Api()
        try:
            pod_name = self._get_name_of_first_pod_of_job(API, selectorUid)
            if pod_name is None or not pod_name.startswith(USER_CREATION_JOB_PREFIX + username + "-"): return None
            pod = API.read_namespaced_pod_status(pod_name, self.namespace)
            if not isinstance(pod, client.V1Pod) or not isinstance(pod.status, client.V1PodStatus): return None
            if pod.status.phase in ['Pending','Unknown']: return None
            limit_bytes = 1024 * 1024  # 1MB
            return API.read_namespaced_pod_log(pod_name, self.namespace, limit_bytes=limit_bytes)
        except ApiException as e:
            logging.root.error("Exception when calling k8s API -> get_user_creation_job_logs: %s\n" % e)
            raise e

