
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

## Upgrade to 3.21
(Upcoming)

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
Tags property has been added to dataset. 
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
