# dataset-service

It is a backend service providing a REST API to manage datasets.
The API is described in detail using OpenAPI v3 specification standad format in the file `API-reference-v1.yaml` (also in swagger: https://app.swaggerhub.com/apis/UPV-CHAIMELEON/Dataset-service/1.0.0).

Authentication is required to access to the service using the OpenId-Connect standard protocol. Basically the client application must initially redirect the user to the authentication service, to obtain a bearer token. Then, that token must be included in the "Authorization" header of any request done to the dataset-service.

Basic operations:
 - POST /dataset
 - GET /dataset/{id}
 - DELETE /dataset/{id}
 - GET /datasets

Below is a walkthrough by examples with CURL.


## Usage

**Previous authentication to obtain a bearer token**

It is recommended to use an OpenID-Connect library for the programming language of your client application, it will ease your work providing things like the automatic refresh of the token. You can use a generic library or a particular library for the implementation used in your project. For example, if Keycloak is used, you can use the "client adapter": https://www.keycloak.org/docs/latest/securing_apps/#supported-platforms

If you use React: https://www.npmjs.com/package/@react-keycloak/web

Basically the user will be redirected to the authentication service with a URL like this: https://chaimeleon-eu.i3m.upv.es/auth/realms/CHAIMELEON/protocol/openid-connect/auth/?scope=openid+email+profile&response_type=id_token&client_id=dataset-service&redirect_uri=https://chaimeleon-eu.i3m.upv.es/dataset-service/

The last parameter ("redirect_uri") contains the URI of our client application.
When the user comes back to our client application, the bearer token will be included in the URL as a parameter named 'id_token'.

For development purposes you can use curl to obtain a token:
```
curl -i -d "client_id=dataset-service" -d "username=user" -d "password=pass" -d "grant_type=password" "https://chaimeleon-eu.i3m.upv.es/auth/realms/CHAIMELEON/protocol/openid-connect/token"
```

**Creation of a dataset**
```
curl -i -X POST -H "Authorization: bearer eyJhbG...N9rw" -H "Content-Type: application/json" -d "{\"name\": \"TestDataset1\",\"description\": \"This is a dataset for testing.\", \"studies\": [{     \"studyId\": \"5e57a4356af19d299c17026d\",     \"studyName\": \"GMIBG2DECUERPOENTERO\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/GMIBG2DECUERPOENTERO20160225\",     \"url\": \"\"   },   {     \"studyId\": \"5e5629835938d32160636353\",     \"studyName\": \"RM431RMRENAL\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/RM431RMRENAL20130820\",     \"url\": \"\"   },   {     \"studyId\": \"5e6a422939b892367c8a5c23\",     \"studyName\": \"TCPEDITRICOABDOMINOPLVICOCONCONTRASTE\",     \"subjectName\": \"17B76FEW\",     \"path\": \"blancagomez/17B76FEW_Neuroblastoma/TCPEDITRICOABDOMINOPLVICOCONCONTRASTE20150129\",     \"url\": \"\"   },   {     \"studyId\": \"5e6b449a3144dc2bc0841efc\",     \"studyName\": \"RM411RMABDOMEN\",     \"subjectName\": \"21N56F7T\",     \"path\": \"blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20100804\",     \"url\": \"\"   },   {     \"studyId\": \"5e6a3d41c9065c475c32b3fe\",     \"studyName\": \"RM411RMABDOMEN\",     \"subjectName\": \"21N56F7T\",     \"path\": \"blancagomez/21N56F7T_Neuroblastoma/RM411RMABDOMEN20150109\",     \"url\": \"\"   },   {     \"studyId\": \"5eeba960903aec091076c180\",     \"studyName\": \"RM815RMDORSAL\",     \"subjectName\": \"1GB90F75\",     \"path\": \"blancagomez/1GB90F75_Neuroblastoma/RM815RMDORSAL20121123\",     \"url\": \"\"   }]}" http://localhost:11000/dataset
```

**List of all datasets**
```
curl -i -X GET -H "Authorization: bearer eyJh...9rw" http://localhost:11000/datasets?limit=30
```

**Search of datasets by name**
```
curl -i -X GET -H "Authorization: bearer eyJh...9rw" http://localhost:11000/datasets?searchString=test
```

**Get details of a dataset by its id**
```
curl -i -X GET -H "Authorization: bearer eyJh...9rw" http://localhost:11000/dataset/20024e8b-f5ff-41b1-8a43-31d18e4eb649
```

**Invalidate a dataset by its id**
```
curl -i -X DELETE -H "Authorization: bearer eyJh...9rw" http://localhost:11000/dataset/20024e8b-f5ff-41b1-8a43-31d18e4eb649
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
```
kubectl apply -f kubernetes.yaml
```

## Run locally for development purposes:

Deploy database with docker:
```
docker run -d -e POSTGRES_DB=db -e POSTGRES_USER=dssuser -e POSTGRES_PASSWORD=XXXXXX -p 5432:5432 --name my-postgres postgres:12
```
Now you can explore database with psql:
```
docker exec -it my-postgres bash
    psql db dsuser
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

You should change at least the password values... Please **do not modify the default config file** for that, it is useful as a template to always see the full configuration keys available. 
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
Using win cmd: `set DATASET_SERVICE_CONFIG={ db: { host: "mydbhost" } }`
Using bash: `export DATASET_SERVICE_CONFIG={ db: { host: "mydbhost" } }`

Please note it is JSON format this time, and takes precedence over all configuration files.



