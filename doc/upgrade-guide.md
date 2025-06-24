
## General notes
Starting from version 3.20.1 the versioning system is this:  
BIGRELEASE.BREAKINGCHANGE.SAFECHANGE-TAG  
Where increment in: 
 - BIGRELEASE version means major milestone with big changes.
 - BREAKINGCHANGE version means it IS NOT SAFE to upgrade without manually do some actions to migrate API clients or config file or database contents.
 - SAFECHANGE version means it IS SAFE to upgrade without manually change any of the clients, previous config or database contents (it will be migrated automatically if required).
 - TAG is optionally added to special versions. Examples: "test3", "beta2", "branch-some-new-function".

All versions with relevant changes are listed in this document with the changes required and not required for migration to each version.
In case of increments in the SAFECHANGE part of version, as you know, the changes are not strictly required but some of them are included also here and recommended to read in case you want to take advantage of the new functionality.

Note SAFECHANGE version means it's always safe to upgrade, but not always to downgrade. 
Indeed whenever the DB schema version is increased an error will appear in the log if you try to downgrade.

## Upgrade to 3.22.0
### Changes in config:
The previous parameter `user_management_scripts` has been changed to `on_event_scripts`to be more general and so include different templates to launch k8s jobs on different events:
```
on_event_scripts:
  user_management_job_template_file_path: ""
    # You can set it to launch a k8s job running a script whenever a user is created (received PUT /users/<userName>)
    # to create the user in other services of the platform or make some configurations related to the new user.
    # Example: "/var/on-event-jobs/user-management-job-template.private.yaml"
    # Set it to empty string to disable that feature.
  site_management_job_template_file_path: ""
    # You can set it to launch a k8s job running a script whenever a site is created or updated (received PUT /sites/<code>)
    # to create the site in other services of the platform or make some configurations related to the new site.
    # Example: "/var/on-event-jobs/site-management-job-template.private.yaml"
    # Set it to empty string to disable that feature.
  project_management_job_template_file_path: ""
    # You can set it to launch a k8s job running a script whenever a project is created or updated (received PUT /projects/<code>)
    # to create the project in other services of the platform or make some configurations related to the new project.
    # Example: "/var/on-event-jobs/project-management-job-template.private.yaml"
    # Set it to empty string to disable that feature.
```

## Upgrade to 3.21.0
### Changes in API:
In GET /projects, 
  - the parameter `forNewDatasets` was deprecated and now removed
  - and the parameter `purpose` is not mandatory now, just optional (default: projectList).
### Changes in config:
New optional parameters to configure the management of groups in the auth service for the projects and roles:
```
auth:
  token_validation:
    project_group_prefix: "PROJECT-"
    project_admins_group_prefix: "ADMINS-PROJECT-"
  admin_api:
    parent_group_of_project_groups: "/PROJECTS"
      # Whener a new project is created, a new group will be automatically created in the auth service within this parent group.
      # That way, when a user is joined to a project, in fact it is added to the group, 
      # an so that group will appear in the user's auth token.
      # The group name will be the project code prefixed with auth.token_validation.project_group_prefix.
      # You can set this parameter to empty string to not create automatically a group in the auth service for each new project 
      # (if you prefer to do it manually).
  user_management:
    prefix_for_roles_as_groups: "/"
    prefix_for_projects_as_groups: "/PROJECTS/PROJECT-"
    assignable_general_roles: ["application-developer", "authorized-technical-data-manager", "clinical-staff", 
                               "cloud-services-and-security-management", "data-scientists", "dataset-administrator"]
```
### Changes in DB:
In the `author` table:
 - `site_code` column has been added, it will be initialized to empty string for all existent rows, you may want to adjust it for each row.  
 - `gid` column is NULL by default now.  
DB schema version increased to 39.
The DB will be automatically migrated and so you will not be able to go back to a previous version.

## Upgrade to 3.20.8
### Changes in DB:
`times_used` column has been added to `dataset` table.  
DB schema version increased to 38.
The DB will be automatically migrated and so you will not be able to go back to a previous version.

## Upgrade to 3.20.7
### Changes in config:
New optional parameter:
```
self:
  eucaim_search_filter_by_tag: ""
    # By default only published and not invalidated datasets are returned to the EUCAIM search, 
    # but additionaly you can filter by any tag.
    # Example: "eucaim-indexed" (only datasets published, not invalidated and with that tag will be included in search results).
    # Set it to empty string to disable this extra filter.
```

## Upgrade to 3.20.6.
### Changes in DB:
`tags` column has been added to `dataset` table.  
DB schema version increased to 37.
The DB will be automatically migrated and so you will not be able to go back to a previous version.

## Upgrade to 3.20
### Changes in config:
That parameter is no more required:
```
auth.token_validation.audience: ["dataset-service"] 
  # The accepted values for the "audience" claim.
```
Instead that new parameter is now required:
```
auth.token_validation.client_id: "dataset-service"
  # This used for the accepted value in the "audience" claim,
  # but also for the key to retrive from "resource_access" claim, which contains the user's roles for this application.
  # You should write here the client ID configured in the authentication service for this application (the dataset-service).
```
