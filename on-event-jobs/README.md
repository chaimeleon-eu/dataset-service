
# Add scripts to be executed on some events

Events: 
  - user creation/deletion
  - site creation/update
  - subproject creation/update

## Custom scripts

You can edit the scripts to be executed on the events, see the directory `scripts`.

This feature allows to automate some actions in the platform (configure other services) according to the event. 
As an example, the current scripts:
 - create and delete a namespace in kubernetes for each user
 - just print the site name whenever a new one is created or any is updated.

That scripts will be executed by a job that will be created by Dataset-service using a job template.
The directory `k8s-templates` contains examples of template for each of the events, 
you can customize for your needs.

Once you have your own job template and scripts, then you must copy them to the `dataset-service-data` persistent volume 
already created for the Dataset-service.
This is an example for a "k8s" ceph volume mounted in "/mnt/cephfs/" containing that persistent volume:
```
mkdir /mnt/cephfs/k8s/volumes/csi/csi-vol-17672da2/b70adcce-1cf7/on-event-jobs
cp -r k8s-templates scripts /mnt/cephfs/k8s/volumes/csi/csi-vol-17672da2/b70adcce-1cf7/on-event-jobs/
```

## (Optional) Build and upload the container image for scripts execution

If your scripts need any aditional requirements you should include them in the files `requirements-apt.txt` or `requirements-pip.txt`.
And then build your container image and upload to any repository you want.
```
set IMAGE_NAME=harbor.chaimeleon-eu.i3m.upv.es/chaimeleon-services/dataset-service-on-event-jobs
set /p IMAGE_TAG=< Dockerfile_version
docker build -t %IMAGE_NAME%:%IMAGE_TAG% .
docker push %IMAGE_NAME%:%IMAGE_TAG%
```
Don't forget to adjust the name and tag of the image to be used in the templates (directory `k8s-templates`).

## Enable the feature

Finally, to enable the feature you must tell the Dataset-service where is the job template file.  
As an example to enable the user management job launching, go to the file `k8s/2-dataset-service.private.yaml` and set the config parameter `on_event_scripts.user_management_job_template_file_path`.

There is that volumeMount by default:
```
          volumeMounts:
            - [...]
            - mountPath: "/var/on-event-jobs"
              subPath: "on-event-jobs"
              name: dataset-service-data-volume
```
So by default you should configure that way:
```
on_event_scripts:
  user_management_job_template_file_path: "/var/on-event-jobs/k8s-templates/user-management-job-template.private.yaml"
```

