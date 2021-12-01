# dataset-service

It is a backend service providing a REST API to manage datasets.
The API is described in detail using OpenAPI v3 specification standad format in the file `API-reference-v1.yaml` 
(also in swagger: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0).

Authentication is required to access to the service using the OpenId-Connect standard protocol. 
Basically the client application must initially redirect the user to the authentication service, to obtain a bearer token. 
Then, that token must be included in the "Authorization" header of any request done to the dataset-service.

Basic operations:
 - POST /dataset
 - GET /dataset/{id}
 - DELETE /dataset/{id}
 - GET /datasets

Below is a walkthrough by examples with CURL.


## Usage

```
REM set DSS_ENDPOINT=http://localhost:11000/api
set DSS_ENDPOINT=https://chaimeleon-eu.i3m.upv.es/dataset-service/api
```

### Previous authentication to obtain a bearer token

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

Every invocation to the dataset-service must include a header like this:
`Authorization: bearer <token>`

The <token> is the JWT (JSON Web Token) provided by the authorization service that will be verified (sign, expiration time, etc.) by the dataset-service. 
Also the user data will be extracted from the token in order to set the author of the dataset (in case of creation) and the author of the operation to 
send to the tracer service.

### Creation of a dataset

POST /dataset

With the authorization header and the dataset properties within the body in JSON format. If success, the code 201 will be returned. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/createDataset

Example:
```
$ curl -i -X POST -H "Authorization: bearer %DSS_TOKEN%" -H "Content-Type: application/json" ^
       -d "{\"name\": \"TestDataset3\", \"description\": \"This is a dataset for testing.\", \"studies\": [{     \"studyId\": \"5e57a4356af19d299c17026d\",     \"studyName\": \"GMIBG2DECUERPOENTERO\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/GMIBG2DECUERPOENTERO20160225\",     \"url\": \"\"   },   {     \"studyId\": \"5e5629835938d32160636353\",     \"studyName\": \"RM431RMRENAL\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/RM431RMRENAL20130820\",     \"url\": \"\"   },   {     \"studyId\": \"5e6a422939b892367c8a5c23\",     \"studyName\": \"TCPEDITRICOABDOMINOPLVICOCONCONTRASTE\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/TCPEDITRICOABDOMINOPLVICOCONCONTRASTE20150129\",     \"url\": \"\"   },   {     \"studyId\": \"5e6b449a3144dc2bc0841efc\",     \"studyName\": \"RM411RMABDOMEN\",     \"subjectName\": \"21N56F7T\",     \"path\": \"blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20100804\",     \"url\": \"\"   },   {     \"studyId\": \"5e6a3d41c9065c475c32b3fe\",     \"studyName\": \"RM411RMABDOMEN\",     \"subjectName\": \"21N56F7T\",     \"path\": \"blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20150109\",     \"url\": \"\"   },   {     \"studyId\": \"5eeba960903aec091076c180\",     \"studyName\": \"RM815RMDORSAL\",     \"subjectName\": \"1GB90F75\",     \"path\": \"blancagomez/1GB90F75_Neuroblastoma/RM815RMDORSAL20121123\",     \"url\": \"\"   }], \"patients\": [{\"subjectName\": \"17B76FEW\", \"path\": \"blancagomez/17B76FEW_Neuroblastoma\", \"eForm\": \"{}\"}, {\"subjectName\": \"21N56F7T\", \"path\": \"blancagomez/21N56F7T_Neuroblastoma\", \"eForm\": \"{}\"}, {\"subjectName\": \"1GB90F75\", \"path\": \"blancagomez/1GB90F75_Neuroblastoma\", \"eForm\": \"{}\"}]}" ^
       "%DSS_ENDPOINT%/dataset"
HTTP/1.1 100 Continue

HTTP/1.1 201 Created
Content-Length: 0
Content-Type: text/html; charset=UTF-8
```

Example of "external" dataset creation:
```
$ curl -i -X POST -H "Authorization: bearer %DSS_TOKEN%" ^
       -F name="Maastricht Lung1" -F description="Test dataset from Maastricht University." ^
       -F clinical_data=@"NSCLC Radiomics Lung1.clinical.csv" ^
       "%DSS_ENDPOINT%/dataset?external=True"
HTTP/1.1 100 Continue

HTTP/1.1 201 Created
Content-Length: 0
Content-Type: text/html; charset=UTF-8
```

### List of all datasets

GET /datasets

With the authorization header and some parameters accepted in the URL for pagination. 
If success the code 200 will be returned and a JSON array in the body of the response. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/listDatasets

Example:
```
$ curl -i -X GET -H "Authorization: bearer %DSS_TOKEN%" "%DSS_ENDPOINT%/datasets?limit=30^&skip=0"
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 728

[{"id": "f99017af-9015-4222-b064-77f3c1b49d8b", "name": "TestDataset3", "authorName": "test test", "creationDate": "2021-10-05 12:29:11.932542", "studiesCount": 6, "patientsCount": 3}, 
 {"id": "00e821c4-e92b-48f7-a034-ba2df547e2bf", "name": "TestDataset2", "authorName": "test test", "creationDate": "2021-10-04 14:50:47.214108", "studiesCount": 1, "patientsCount": 1}, 
 {"id": "efa2cba6-4a17-4612-8074-7e9eb9c9d7ca", "name": "TestDataset1", "authorName": "test test", "creationDate": "2021-10-04 14:42:37.725548", "studiesCount": 1, "patientsCount": 1}]
```

### Search of datasets by name

This is the same operation as the previous example but with the parameter `searchString` (case-insensitive). 

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/listDatasets

Example:
```
$ curl -i -X GET -H "Authorization: bearer %DSS_TOKEN%" "%DSS_ENDPOINT%/datasets?searchString=dataset3"
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 182

[{"id": "f99017af-9015-4222-b064-77f3c1b49d8b", "name": "TestDataset3", "authorName": "test test", "creationDate": "2021-10-05 12:29:11.932542", "studiesCount": 6, "patientsCount": 3}]
```

### Get details of a dataset by its id

GET /dataset/{id}

With the authorization header and some parameters accepted in the URL for pagination of studies in the dataset. 
Returns a JSON object. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/getDataset

Example:
```
$ curl -i -X GET -H "Authorization: bearer %DSS_TOKEN%" "%DSS_ENDPOINT%/dataset/f99017af-9015-4222-b064-77f3c1b49d8b?studiesLimit=30"
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 1506

{"id": "f99017af-9015-4222-b064-77f3c1b49d8b", "name": "TestDataset3", "previousId": null, "authorId": "a43d426c-11aa-41cb-ab15-616d68627c77", "authorName": "test test", "authorEmail": "test@upv.es", "creationDate": "2021-10-05 22:29:11.932542", "description": "This is a dataset for testing.", "gid": 1, "public": false, "studiesCount": 6, "patientsCount": 3, 
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

DELETE /dataset/{id}

With the authorization header. If success the code 200 will be returned. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/datasets/deleteDataset

Example:
```
$ curl -i -X DELETE -H "Authorization: bearer %DSS_TOKEN%" "%DSS_ENDPOINT%/dataset/00e821c4-e92b-48f7-a034-ba2df547e2bf"
HTTP/1.1 200 OK
Content-Length: 0
Content-Type: text/html; charset=UTF-8
```

### Create user

POST /user/{userName}

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
$ curl -i -X POST -H "Authorization: bearer %DSS_TOKEN%" -H "Content-Type: application/json" ^
       -d "{\"uid\": \"d290f1ee-6c54-4b01-90e6-d701748f0851\", \"groups\": [\"data-scientists\", \"dataset-administrator\"]}" ^
       "%DSS_ENDPOINT%/user/user1"
HTTP/1.1 201 Created
Content-Length: 0
Content-Type: text/html; charset=UTF-8
```

### Get user GID

GET /user/{userName}

With the authorization header. 
Returns a JSON object. 
If fail, a 40X code will be returned with a JSON object in the body containing also the code and the error message.

Details: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0#/users/getUser

Example:
```
$ curl -i -X GET -H "Authorization: bearer %DSS_TOKEN%" "%DSS_ENDPOINT%/user/user1"
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

## Build the image
```
docker build -t chaimeleon-eu.i3m.upv.es:10443/chaimeleon-services/dataset-service-backend:0.2 .
```
## Upload the image
```
docker login -u registryUser chaimeleon-eu.i3m.upv.es:10443
docker push chaimeleon-eu.i3m.upv.es:10443/chaimeleon-services/dataset-service-backend:0.2
docker logout chaimeleon-eu.i3m.upv.es:10443
```

## Deploy with Kubernetes

Copy the template: `cp kubernetes.yaml kubernetes.mine.yaml`

Configure: edit `kubernetes.mine.yaml` (set passwords, urls, etc.)

Create a namespace: `kubectl create namespace dataset-service`

And finally: `kubectl apply -f kubernetes.mine.yaml`


## Run locally for development purposes:

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
        "view_public_datasets"
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
 - view_public_datasets (0): only can list and see details of public datasets
 - view_all_datasets (1): also can list and see details of protected datasets
 - admin_datasets (2): also can create, update and invalidate own datasets
 - superadmin_datasets (3): also can update and invalidate any dataset (not owned datasets)

The name of each one can be customized in the configuration file. 
The number is just a hint, not part of the name: you can see them as security levels, each level include the previous levels.

There are other special roles:
 - 'admin_users': required for the operations in '/user'.
 - 'admin_datasetAccess': required for the operations in '/datasetAccess' and '/datasetAccessCheck'.

