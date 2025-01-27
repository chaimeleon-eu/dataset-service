
# Add scripts to be executed on user creation/deletion

## Custom scripts

You can edit the scripts to be ejecuted on creation and deletion of any user, see in the directory `scripts`.

This feature allows to automate some actions in the platform (configure other services) for the new user. 
As an example, the current scripts create and delete a namespace in kubernetes for the user.

Thas scripts will be executed by a job that will be created by Dataset-service using a job template.

You shoud make a private copy of the template and customize for your needs:  
```
cp job-template.yaml job-template.private.yaml
vim job-template.private.yaml
```

Once you have your own job template and scripts then you must copy them to the `dataset-service-data` persistent volume 
already created for the Dataset-service.
This is an example for a "k8s" ceph volume mounted in "/mnt/cephfs/" containing that persistent volume:
```
mkdir /mnt/cephfs/k8s/volumes/csi/csi-vol-17672da2/b70adcce-1cf7/user-management-jobs
cp -r job-template.private.yaml scripts /mnt/cephfs/k8s/volumes/csi/csi-vol-17672da2/b70adcce-1cf7/user-management-jobs/
```

## (Optional) Build and upload the container image for user management

If your scripts need any aditional requirements you should include them in the files `requirements-apt.txt` or `requirements-pip.txt`.
And then build your container image and upload to any repository you want.
```
set IMAGE_NAME=harbor.chaimeleon-eu.i3m.upv.es/chaimeleon-services/dataset-service-user-management
set /p IMAGE_TAG=< Dockerfile_version
docker build -t %IMAGE_NAME%:%IMAGE_TAG% .
docker push %IMAGE_NAME%:%IMAGE_TAG%
```
Don't forget to adjuts the image to be used in the `job-template.private.yaml` file.

## Enable the feature

Finally, to enable the feature you must tell the Dataset-service where is the job template file.  
Go to the file `k8s/2-dataset-service.private.yaml` and set the config parameter `user_management_scripts.job_template_file_path`.

There is that volumeMount by default:
```
          volumeMounts:
            - [...]
            - mountPath: "/var/user-management-jobs"
              subPath: "user-management-jobs"
              name: dataset-service-data-volume
```
So by default you should configure that way:
```
user_management_scripts:
  job_template_file_path: "/var/user-management-jobs/job-template.private.yaml"
```

