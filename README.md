# dataset-service

It is a backend service providing a REST API to manage datasets.
The API is described in detail using OpenAPI v3 specification standad format in the file `API-reference-v1.yaml` 
(also in swagger: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0).

Some operations require authentication using the OpenId-Connect standard protocol. 
Basically, when the user wants to login, the client application must redirect to the authentication service, to obtain a bearer token. 
Then, that token must be included in the "Authorization" header of any request sent to the dataset-service. More details below.

Basic API operations:
 - POST /api/datasets
 - GET /api/datasets/{id}
 - PATCH /api/datasets/{id}
 - GET /api/datasets

Below there is a walkthrough by examples with CURL.

Other managed routes outside /api/:
 - GET /web/{staticFilePath}   
   static files needed to load the client app (frontend) in the browser
 - GET /{anyOtherPath}      
   internally redirected to /web/index.html, the client app with JS code that manages locally other routes like /datasets 

## API Usage

```
REM set DSS_ENDPOINT=http://localhost:11000/api
set DSS_ENDPOINT=https://chaimeleon-eu.i3m.upv.es/dataset-service/api
```

### Authentication to obtain a bearer token

Some operations require previous authentication using the OpenId-Connect standard protocol.
It is recommended to use an OpenID-Connect library for the programming language of your client application, 
it will ease your work providing things like the automatic refresh of the token. 
You can use a generic library or a particular library for the implementation used in your project. 
For example, if Keycloak is used, you can use the "client adapter": https://www.keycloak.org/docs/latest/securing_apps/#supported-platforms

If you use React: https://www.npmjs.com/package/@react-keycloak/web

Basically the user will be redirected to the authentication service with a URL like this: 
https://chaimeleon-eu.i3m.upv.es/auth/realms/CHAIMELEON/protocol/openid-connect/auth/?scope=openid+email+profile&response_type=id_token&client_id=dataset-service-ui&redirect_uri=https://chaimeleon-eu.i3m.upv.es/dataset-service/

The last parameter ("redirect_uri") contains the URI of our client application.
When the user comes back to our client application, the bearer token will be included in the URL as a parameter named 'id_token'.

For development/testing purposes you can use curl to obtain a token:
```
curl -i -d "client_id=dataset-service-ui" -d "username=user" -d "password=pass" -d "grant_type=password" "https://chaimeleon-eu.i3m.upv.es/auth/realms/CHAIMELEON/protocol/openid-connect/token"
set DSS_TOKEN=eyJ...79w1rA
```
Warning: please ensure you are using "https" in the URL to avoid send password in clear text 
and take into account that it can be stored in clear also in the shell history.

Once the token has been obtained it must be included in a header like this:
`Authorization: bearer <token>`

The <token> is the JWT (JSON Web Token) provided by the authorization service that will be verified (sign, expiration time, etc.) by the dataset-service. 
Also the user data will be extracted from the token in order to set the author of the dataset (in case of creation) and the author of the operation to 
send to the tracer service.

### Creation of a dataset

POST /datasets

With the authorization header and the dataset properties within the body in JSON format. 
If success, the code 201 will be returned. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/createDataset

Example:
```
$ curl -i -X POST ^
       -H "Authorization: bearer %DSS_TOKEN%" ^
       -H "Content-Type: application/json" ^
       -d "{\"name\": \"TestDataset3\", \"description\": \"This is a dataset for testing.\", \"studies\": [{     \"studyId\": \"5e57a4356af19d299c17026d\",     \"studyName\": \"GMIBG2DECUERPOENTERO\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/GMIBG2DECUERPOENTERO20160225\",   \"series\": [\"serie1\", \"serie2\", \"serie3\"],    \"url\": \"\"   },   {     \"studyId\": \"5e5629835938d32160636353\",     \"studyName\": \"RM431RMRENAL\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/RM431RMRENAL20130820\",    \"series\": [\"serie1\"],   \"url\": \"\"   },   {     \"studyId\": \"5e6a422939b892367c8a5c23\",     \"studyName\": \"TCPEDITRICOABDOMINOPLVICOCONCONTRASTE\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/TCPEDITRICOABDOMINOPLVICOCONCONTRASTE20150129\",   \"series\": [\"serie1\"],    \"url\": \"\"   },   {     \"studyId\": \"5e6b449a3144dc2bc0841efc\",     \"studyName\": \"RM411RMABDOMEN\",     \"subjectName\": \"21N56F7T\",     \"path\": \"blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20100804\",    \"series\": [\"serie1\"],   \"url\": \"\"   },   {     \"studyId\": \"5e6a3d41c9065c475c32b3fe\",     \"studyName\": \"RM411RMABDOMEN\",     \"subjectName\": \"21N56F7T\",     \"path\": \"blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20150109\",   \"series\": [\"serie1\"],    \"url\": \"\"   },   {     \"studyId\": \"5eeba960903aec091076c180\",     \"studyName\": \"RM815RMDORSAL\",     \"subjectName\": \"1GB90F75\",     \"path\": \"blancagomez/1GB90F75_Neuroblastoma/RM815RMDORSAL20121123\",    \"series\": [\"serie1\"],   \"url\": \"\"   }], \"subjects\": [{\"subjectName\": \"17B76FEW\", \"eForm\": {}}, {\"subjectName\": \"21N56F7T\", \"eForm\": {}}, {\"subjectName\": \"1GB90F75\", \"eForm\": {}}]}" ^
       "%DSS_ENDPOINT%/datasets"

HTTP/1.1 100 Continue

HTTP/1.1 201 Created
Content-Length: 61
Content-Type: application/json

{"url": "/api/datasets/efa2cba6-4a17-4612-8074-7e9eb9c9d7ca"}
```

Example of "external" dataset creation:
```
$ curl -i -X POST ^
       -H "Authorization: bearer %DSS_TOKEN%" ^
       -F name="Maastricht Lung1" -F description="Test dataset from Maastricht University." ^
       -F clinical_data=@"NSCLC Radiomics Lung1.clinical.csv" ^
       "%DSS_ENDPOINT%/datasets?external=True"

HTTP/1.1 100 Continue

HTTP/1.1 201 Created
Content-Length: 61
Content-Type: application/json

{"url": "/api/datasets/3388a9c5-4ebb-45ba-93fc-7b54813f0cf2"}
```

### List of all datasets

GET /datasets

With the authorization header and some parameters accepted in the URL for pagination. 
If success, the code 200 will be returned and a JSON array in the body of the response. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/listDatasets

Example:
```
$ curl -i -X GET ^
       -H "Authorization: bearer %DSS_TOKEN%" ^
       "%DSS_ENDPOINT%/datasets?limit=30&skip=0"
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 728

[{"id": "f99017af-9015-4222-b064-77f3c1b49d8b", "name": "TestDataset3", "authorName": "test test", "creationDate": "2021-10-05 12:29:11.932542", "public": false, "invalidated": false, "studiesCount": 6, "subjectsCount": 3}, 
 {"id": "00e821c4-e92b-48f7-a034-ba2df547e2bf", "name": "TestDataset2", "authorName": "test test", "creationDate": "2021-10-04 14:50:47.214108", "public": false, "invalidated": false, "studiesCount": 1, "subjectsCount": 1}, 
 {"id": "efa2cba6-4a17-4612-8074-7e9eb9c9d7ca", "name": "TestDataset1", "authorName": "test test", "creationDate": "2021-10-04 14:42:37.725548", "public": false, "invalidated": false, "studiesCount": 1, "subjectsCount": 1}]
```

### Search of datasets by name

This is the same operation as the previous example but with the parameter `searchString` (case-insensitive). 

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/listDatasets

Example:
```
$ curl -i -X GET ^
       -H "Authorization: bearer %DSS_TOKEN%" ^
       "%DSS_ENDPOINT%/datasets?searchString=dataset3"
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 182

[{"id": "f99017af-9015-4222-b064-77f3c1b49d8b", "name": "TestDataset3", "authorName": "test test", "creationDate": "2021-10-05 12:29:11.932542", "public": false, "invalidated": false, "studiesCount": 6, "subjectsCount": 3}]
```

### Get details of a dataset by its id

GET /datasets/{id}

With the authorization header and some parameters accepted in the URL for pagination of studies in the dataset. 
Returns a JSON object. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/getDataset

Example:
```
$ curl -i -X GET ^
       -H "Authorization: bearer %DSS_TOKEN%" ^
       "%DSS_ENDPOINT%/datasets/f99017af-9015-4222-b064-77f3c1b49d8b?studiesLimit=30"
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 1506

{"id": "f99017af-9015-4222-b064-77f3c1b49d8b", "name": "TestDataset3", "previousId": null, "authorId": "a43d426c-11aa-41cb-ab15-616d68627c77", "authorName": "test test", "authorEmail": "test@upv.es", "creationDate": "2021-10-05 22:29:11.932542", "description": "This is a dataset for testing.", "gid": 1, "public": false, "studiesCount": 6, "subjectsCount": 3, 
"studies": [
    {"studyId": "5e57a4356af19d299c17026d", "studyName": "GMIBG2DECUERPOENTERO", "subjectName": "17B76FEW", "path": "blancagomez/17B76FEW_Neuroblastoma/GMIBG2DECUERPOENTERO20160225", "url": ""}, 
    {"studyId": "5e5629835938d32160636353", "studyName": "RM431RMRENAL", "subjectName": "17B76FEW", "path": "blancagomez/17B76FEW_Neuroblastoma/RM431RMRENAL20130820", "url": ""}, 
    {"studyId": "5e6a422939b892367c8a5c23", "studyName": "TCPEDITRICOABDOMINOPLVICOCONCONTRASTE", "subjectName": "17B76FEW", "path": "blancagomez/17B76FEW_Neuroblastoma/TCPEDITRICOABDOMINOPLVICOCONCONTRASTE20150129", "url": ""}, 
    {"studyId": "5e6b449a3144dc2bc0841efc", "studyName": "RM411RMABDOMEN", "subjectName": "21N56F7T", "path": "blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20100804", "url": ""}, 
    {"studyId": "5e6a3d41c9065c475c32b3fe", "studyName": "RM411RMABDOMEN", "subjectName": "21N56F7T", "path": "blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20150109", "url": ""}, 
    {"studyId": "5eeba960903aec091076c180", "studyName": "RM815RMDORSAL", "subjectName": "1GB90F75", "path": "blancagomez/1GB90F75_Neuroblastoma/RM815RMDORSAL20121123", "url": ""}
]}
```

### Invalidate a dataset by its id

PATCH /datasets/{id}

With the authorization header. 
If success, the code 200 will be returned. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/modifyDataset

Example:
```
$ curl -i -X PATCH ^
       -H "Authorization: bearer %DSS_TOKEN%" ^
       -H "Content-Type: application/json" ^
       -d "{\"property\": \"public\", \"value\": true}" ^
       "%DSS_ENDPOINT%/datasets/00e821c4-e92b-48f7-a034-ba2df547e2bf"
HTTP/1.1 200 OK
Content-Length: 0
Content-Type: text/html; charset=UTF-8
```

### Create a user

PUT /users/{userName}

With the authorization header and the user properties within the body in JSON format. 
If success, the code 201 will be returned. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/users/createUser

Example authenticating previously with a service account:
```
$ curl -i -d "client_id=kubeauthorizer-pod" -d "client_secret=XXXX-XXXX-XXXX" -d "grant_type=client_credentials" "https://chaimeleon-eu.i3m.upv.es/auth/realms/CHAIMELEON/protocol/openid-connect/token"
$ set DSS_TOKEN=eyJ...79w1rA
```
```
$ curl -i -X PUT -H "Authorization: bearer %DSS_TOKEN%" -H "Content-Type: application/json" ^
       -d "{\"uid\": \"d290f1ee-6c54-4b01-90e6-d701748f0851\", \"groups\": [\"data-scientists\", \"dataset-administrator\"]}" ^
       "%DSS_ENDPOINT%/users/user1"
HTTP/1.1 201 Created
Content-Length: 0
Content-Type: text/html; charset=UTF-8
```

### Get the user GID

GET /users/{userName}

With the authorization header. 
Returns a JSON object. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/users/getUser

Example:
```
$ curl -i -X GET -H "Authorization: bearer %DSS_TOKEN%" "%DSS_ENDPOINT%/users/user1"
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 13

{"gid": 2002}
```

### Check dataset access

POST /datasetAccessCheck

With the authorization header and the access properties (userName and datasets required) within the body in JSON format. 
If success, the code 204 will be returned. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.
In case of fail due to any required dataset which is not available for the user, the code 403 is returned with a JSON array containing not available datasets.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/checkDatasetAccess

Example authenticating previously with a service account:
```
$ curl -i -d "client_id=kubernetes-operator" -d "client_secret=XXXX-XXXX-XXXX" -d "grant_type=client_credentials" "https://chaimeleon-eu.i3m.upv.es/auth/realms/CHAIMELEON/protocol/openid-connect/token"
$ set DSS_TOKEN=eyJ...79w1rA
```
```
$ curl -i -X POST -H "Authorization: bearer %DSS_TOKEN%" -H "Content-Type: application/json" ^
       -d "{\"userName\": \"user1\", \"datasets\": [\"00e821c4-e92b-48f7-a034-ba2df547e2bf\", \"f99017af-9015-4222-b064-77f3c1b49d8b\"]}" ^
       "%DSS_ENDPOINT%/datasetAccessCheck"
HTTP/1.1 204 No Content
Content-Length: 0
```

### Create dataset access

POST /datasetAccess/{id}

With the authorization header and the access properties (userName, datasets required and tool info) within the body in JSON format. 
If success, the code 201 will be returned. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.
In case of fail due to any required dataset which is not available for the user, the code 403 is returned.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/createDatasetAccess

Example:
```
$ curl -i -X POST -H "Authorization: bearer %DSS_TOKEN%" -H "Content-Type: application/json" ^
       -d "{\"userName\": \"user1\", \"datasets\": [\"f99017af-9015-4222-b064-77f3c1b49d8b\"], \"toolName\": \"desktop-tensorflow\", \"toolVersion\": \"0.3.1\"}" ^
       "%DSS_ENDPOINT%/datasetAccess/c0bd6506-219b-4fc2-8fdb-3deb1d1a4ac2"
HTTP/1.1 201 Created
Content-Length: 0
```

### [Only for developers] Set or update the web UI

POST /set-ui

To set the web UI static files from any URL with a ZIP package.

Example:
```
$ curl -i -H "devToken: SECRET-TOKEN" -d "http://158.42.154.23:19000/build.zip" "https://chaimeleon-eu.i3m.upv.es/dataset-service/api/set-ui"
```

## Deployment

```
set IMAGE_NAME=harbor.chaimeleon-eu.i3m.upv.es/chaimeleon-services/dataset-service-backend
set IMAGE_TAG=1.78
```

### Build the image
```
docker build -t %IMAGE_NAME%:%IMAGE_TAG% .
```
### Upload the image
```
docker login -u registryUser harbor.chaimeleon-eu.i3m.upv.es
docker push %IMAGE_NAME%:%IMAGE_TAG%
docker logout harbor.chaimeleon-eu.i3m.upv.es
```

### Deploy with Kubernetes

Copy the template: `cp kubernetes.yaml kubernetes.mine.yaml`

Configure: edit `kubernetes.mine.yaml` (set passwords, urls, etc.)

Create a namespace: `kubectl create namespace dataset-service`

And finally: `kubectl apply -f kubernetes.mine.yaml`


### Run locally for testing purposes:

Deploy database with docker:
```
docker run -d -e POSTGRES_DB=db -e POSTGRES_USER=dssuser -e POSTGRES_PASSWORD=XXXXXX -p 5432:5432 --name my-postgres postgres:12
```
Now you can explore database with psql:
```
docker exec -it my-postgres bash
    psql db dssuser
        \dt
        select * from metadata;
        \q
    exit
```
Once database is ready, you can run the main service with a local configuration file:
```
python .\start_dataset_service.py .\etc\dataset-service-local.yaml
```

## Configuration

First of all, **default configuration values** will be loaded from a file located in (the first that exists):
 - `./etc/dataset-service.default.yaml`
 - `/etc/dataset-service/dataset-service.default.yaml`

You should change at least the password values... Please **do not modify the default config file** for that, 
it is useful as a template to always see the full configuration keys available with descriptive comments. 
To set your own configuration you should make a copy of the file or better just write only the keys you want to change to a new file.

The **configuration file** will be loaded from path (the first that exists):
 - `./etc/dataset-service.yaml`
 - `/etc/dataset-service/dataset-service.yaml`

Optional: you can set the location of the configuration file in the **first parameter** of the execution line. Example:
```
python .\start_dataset_service.py .\etc\dataset-service-local.yaml
```

The keys defined in the **configuration file** takes precedence over the same keys which are defined in **default configuration file**.

Finally you can override in the same way some (or all) of the configuration keys with the **environment variable DATASET_SERVICE_CONFIG**.
 - Using win cmd: `set DATASET_SERVICE_CONFIG={ db: { host: "mydbhost" } }`
 - Using bash: `export DATASET_SERVICE_CONFIG={ db: { host: "mydbhost" } }`

Please note it is JSON format this time, and takes precedence over all configuration files.

## Authorization

The capabilities of a user (which operations can do) are defined in the "application roles" included in the 
token received (i.e. in 'resource_access.dataset-service.roles'). Example:
```
{
  "exp": ...,
  "iat": ...,
  ...
  "iss": ...,
  "aud": ...,
  "sub": ...,
  ...
  "realm_access": {
    "roles": [
      ...
    ]
  },
  "resource_access": {
    "account": {
      "roles": [
        ...
      ]
    },
    "dataset-service": {
      "roles": [
        "access_all_datasets"
      ]
    }
  },
  ...
  "name": ...,
  "preferred_username": ...,
  "email": ...
}
```

These are the known roles:
 - __access_all_datasets__ (1): it can list, see details and use all datasets not only the public
 - __admin_datasets__ (2): also can create and modify properties (and the state) of the own datasets
 - __superadmin_datasets__ (3): also can modify any dataset (not owned datasets)

The name of each one can be customized in the configuration file. 
The number is just a hint, not part of the name: you can see them as permission levels, each level include the previous levels.

There are other special roles:
 - __admin_users__: required for the operations in '/user'.
 - __admin_datasetAccess__: required for the operations in '/datasetAccess' and '/datasetAccessCheck'.

## Dataset states

Each dataset has some flags (with value true or false) which define its state of visibility, editability and usability.  
The __flags__ are:
 - _draft_
 - _public_
 - _invalidated_
 
According to value of the flags a dataset can be in one of these states:
 - __Draft__: (_draft_ = __true__, _public_ = false, _invalidated_ = false)  
          All datasets are created in this state.  
          Only the author can see and use the dataset in Draft state and some properties can be modified (name/title, description).  
          The draft mode can be useful for testing datasets because they are "private" to the author.  
          Possible actions:  
           - Release (_draft_ -> false), goes to Released state.  
           - Invalidate (_invalidated_ -> true), goes to Invalidated state.
 - __Released__: (_draft_ = false, _public_ = false, _invalidated_ = false)  
           When released, the dataset can't be edited anymore and all the registered users with the rol 'access_all_datasets' can see it and use it.  
           Possible actions:  
            - Publish (_public_ -> true) goes to Published state.  
            - Invalidate (_invalidated_ -> true), goes to Invalidated state.
 - __Published__: (_draft_ = false, _public_ = __true__, _invalidated_ = false)  
           When published, the dataset can be seen and used by any registered user.
           It can be seen (not used) by unregistered users.  
           Possible actions:  
            - Set to non-public (_public_ -> false), returns to Released state.  
            - Invalidate (_invalidated_ -> true), goes to Invalidated state.
 - __Invalidated__: (_draft_ = true/false, _public_ = true/false, _invalidated_ = __true__)  
           Only appears in the list of the author, but anyone (who has the id or link) can see the details.  
           For example when the dataset have a PID and somebody goes to the detail from the paper where is included.  
           Anyway, a big label with the text "invalidated" should appear in details, and also in the list for the author.  
           Nobody can modify it nor use it, neither the author.  
           Possible actions:  
            - 'Reactivate' (_invalidated_ -> false), goes to previous state (Draft, Released or Published).

All the actions can be performed only by the author or superadmin.

![Dataset states and flags diagram](doc/resources/dataset-states-flags.png)

![Dataset states diagram](doc/resources/dataset-states.png)


## Dataset modifications

Some properties of dataset can be modified depending on the user and the current state of the dataset.  
In order to simplify the client and not duplicate that logic, 
there is a dynamic property named "editablePropertiesByTheUser" in the object returned by GET /api/datasets/{id} (the details of dataset). 
The value of that property is a list of the properties (including flags) that can be modified with PATCH operation by the current user in the current state of dataset.

Only the author or superadmin can modify any of the properties or flags.  
And these are the properties that can be modified (and when):
 - draft (when draft = true)
 - public (when draft = false)
 - invalidated (always)
 - name (when draft = true)
 - description (when draft = true)
 - licenseUrl (always)
 - contactInfo (always)
 - pidUrl (when draft = false)

