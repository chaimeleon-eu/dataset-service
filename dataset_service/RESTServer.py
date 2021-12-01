#! /usr/bin/env python3

from datetime import datetime
import bottle
import threading
import logging
import json
#from kubernetes.client import exceptions
import yaml
import base64
import time
from dataset_service.storage import DB
#from kubernetes import client, config
#from kubernetes.client.rest import ApiException
import jwt
from jwt import PyJWKClient
import urllib
import uuid
import dataset_service.tracer as tracer
from dataset_service.dataset import *

#LOG = logging.getLogger('kube-authorizer')
LOG = logging.root

# The block code below is just to translate the body of error responses to JSON format
class MyBottle(bottle.Bottle):
    def default_error_handler(self, res):
        bottle.response.content_type = 'application/json'
        return json.dumps(dict(error = res.body, status_code = res.status_code))

# The block code below is just to send the exception trace to the log
def exception_catch(func):
    def wrapper(*args,**kwargs):
        try:
            return func(*args,**kwargs)
        except jwt.exceptions.PyJWTError as e1:
            bottle.response.status = 401
            LOG.error("invalid access token: " + str(type(e1)))
            bottle.response.content_type = 'application/json'
            return json.dumps(dict(error = "invalid access token, " + str(type(e1))))
        except Exception as e:
            LOG.exception(e)
            raise e
    return wrapper

#app = bottle.Bottle()
app = MyBottle()
app.install(exception_catch)
thisRESTServer = None
KUBERNETES_CONFIG = None
KUBERNETES_API_CLIENT = None
CONFIG = None
DB_CONNECTION_STR = ""
AUTH_PUBLIC_KEY = None


class RESTServer (bottle.ServerAdapter):

    def run(self, handler):
        try:
            # First try to use the new version
            from cheroot import wsgi
            server = wsgi.Server((self.host, self.port), handler, request_queue_size=32)
        except:
            from cherrypy import wsgiserver
            server = wsgiserver.CherryPyWSGIServer((self.host, self.port), handler, request_queue_size=32)

        self.srv = server
        try:
            server.start()
        finally:
            server.stop()

    def shutdown(self):
        if self.srv:
            self.srv.stop()

def run(host, port, config):
    global thisRESTServer, LOG, KUBERNETES_CONFIG, KUBERNETES_API_CLIENT, CONFIG, AUTH_PUBLIC_KEY
    CONFIG = config

    LOG.info("Obtaining the public key from %s..." % CONFIG.auth.token_issuer_public_keys_url)
    LOG.info("kid: %s" % CONFIG.auth.kid)
    try:
        retries = 0
        while retries < 5:
            try:
                jwks_client = PyJWKClient(CONFIG.auth.token_issuer_public_keys_url)
                AUTH_PUBLIC_KEY = jwks_client.get_signing_key(CONFIG.auth.kid)
                LOG.info("key: %s" % str(AUTH_PUBLIC_KEY.key))
                break;
            except urllib.error.HTTPError as e1:
                LOG.error("HTTPError: " + str(e1.code) + " - " + str(e1.reason))
            except urllib.error.URLError as e2:
                LOG.error("URLError: "+ str(e2.reason))
            retries += 1
            LOG.info("Retrying in 5 seconds...")
            time.sleep(5)
        if retries >= 5: raise Exception("Unable to obtaining public key.")
    except Exception as e:
        LOG.exception(e)
        raise e

    LOG.info("Connecting to database...")
    # LOG.info(str(yaml.dump(CONFIG.db.creation_dict)).replace("\n", ", "))
    LOG.info("host: %s, port: %s, dbname: %s, user: %s" % (CONFIG.db.host, CONFIG.db.port, CONFIG.db.dbname, CONFIG.db.user))
    try:
        with DB(CONFIG.db) as db:
            LOG.info("Initializing database...")
            db.setup()
    except Exception as e:
        LOG.exception(e)
        raise e

    if CONFIG.self.datasets_mount_path == '':
        LOG.warn("datasets_mount_path is empty: datasets will not be created on disk, they will be only stored in database.")
    else:
        LOG.info("Checking mount paths...")
        if os.path.isdir(CONFIG.self.datalake_mount_path):
            LOG.info("OK - datalake_mount_path: " + CONFIG.self.datalake_mount_path)
        else: 
            e = Exception("datalake_mount_path is not an existing directory: " + CONFIG.self.datalake_mount_path)
            LOG.exception(e)
            raise e
        if os.path.isdir(CONFIG.self.datasets_mount_path):
            LOG.info("OK - datasets_mount_path: " + CONFIG.self.datasets_mount_path)
        else: 
            e = Exception("datasets_mount_path is not an existing directory: " + CONFIG.self.datasets_mount_path)
            LOG.exception(e)
            raise e


#   LOG.info("Loading Kubernetes configuration...")
#   KUBERNETES_CONFIG = client.Configuration()
#   KUBERNETES_CONFIG.verify_ssl = False
#   KUBERNETES_CONFIG.host = CONFIG.kubernetes.endpoint
#   KUBERNETES_CONFIG.api_key = {"authorization": "Bearer " + CONFIG.kubernetes.auth_token}

#   KUBERNETES_API_CLIENT = client.ApiClient(KUBERNETES_CONFIG)
        
    thisRESTServer = RESTServer(host=host, port=port)
    LOG.info("Running the service in %s:%s..." % (host, port))
    bottle.run(app, server=thisRESTServer, quiet=True)

def stop():
    if thisRESTServer:
        LOG.info("Shutting down the service...")
        thisRESTServer.shutdown()

def run_in_thread(host, port, config):
    bottle_thr = threading.Thread(target=run, args=(host, port, config))
    bottle_thr.daemon = True
    bottle_thr.start()
    return bottle_thr
    

def validate_token(token):
    headers = jwt.get_unverified_header(token)
    LOG.debug("Token key id (kid header):" + headers['kid'])

    #AUTH_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\n" + CONFIG.auth.token_issuer_public_key + "\n-----END PUBLIC KEY-----"
    decodedToken = jwt.decode(token, AUTH_PUBLIC_KEY.key, algorithms=['RS256'],  
                              issuer=CONFIG.auth.issuer, audience=CONFIG.auth.audience, 
                              options={'verify_signature': True, 'require': ["exp", "iat", "iss", "aud"]})
    LOG.debug(json.dumps(decodedToken))
    return decodedToken

def setErrorResponse(code, message):
    bottle.response.status = code
    bottle.response.content_type = "application/json"
    return json.dumps(dict(error = message, status_code = code))

def appendIfNotExists(array, item):
    if item not in array: array.append(item)

def checkAuthorizationHeader(serviceAccount=False):
    try:
        encodedToken = bottle.request.headers["authorization"][7:]
    except Exception as e:
        return False, setErrorResponse(401, "authorization header is missing or invalid")
    user_info = validate_token(encodedToken)
    if not "sub" in user_info.keys(): return False, setErrorResponse(401,"invalid access token: missing 'sub'")
    if not serviceAccount:
        if not "preferred_username" in user_info.keys(): return False, setErrorResponse(401,"invalid access token: missing 'preferred_username'")
        if not "name" in user_info.keys(): return False, setErrorResponse(401,"invalid access token: missing 'name'")
        if not "email" in user_info.keys(): return False, setErrorResponse(401,"invalid access token: missing 'email'")

    try:
        user_info["appRoles"] = user_info["resource_access"]["dataset-service"]["roles"]
    except:
        return False, setErrorResponse(401,"invalid access token: missing 'resource_access.dataset-service.roles'")

    # ensure roles included in other roles
    if CONFIG.auth.roles.view_all_datasets in user_info["appRoles"]:
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.view_public_datasets)
    if CONFIG.auth.roles.admin_datasets in user_info["appRoles"]:
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.view_public_datasets)
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.view_all_datasets)
    if CONFIG.auth.roles.superadmin_datasets in user_info["appRoles"]:
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.view_public_datasets)
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.view_all_datasets)
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.admin_datasets)

    return True, user_info

def get_header_media_types(header):
    """
    Function to get the media types specified in a header.
    Returns a List of strings.
    """
    res = []
    accept = bottle.request.headers.get(header)
    if accept:
        media_types = accept.split(",")
        for media_type in media_types:
            pos = media_type.find(";")
            if pos != -1:
                media_type = media_type[:pos]
            media_type = media_type.strip()
            if media_type in ["text/yaml", "text/x-yaml"]:
                res.append("text/yaml")
            else:
                res.append(media_type)

    return res


@app.route('/', method='GET')
def RESTGetWebUI():
    LOG.debug("Received GET /")
    return bottle.static_file('index.html', CONFIG.self.static_files_dir_path)

@app.route('/<filename:re:.*\.(html|js|json|txt|map|css|jpg|png|gif|ico|svg)>', method='GET')
def RESTGetWebUI(filename):
    LOG.debug("Received GET /"+filename)
    return bottle.static_file(filename, CONFIG.self.static_files_dir_path)

@app.route('/api/', method='GET')
def RESTGetHello():
    LOG.debug("Received GET /api/")
    bottle.response.content_type = "text/plain"
    return "Hello from Dataset Service"

@app.route('/api/set-ui', method='POST')
def postDataset():
    LOG.debug("Received POST /api/set-ui")
    if CONFIG.self.dev_token == "":
        return setErrorResponse(404, "Not found: '%s'" % bottle.request.path)
    if bottle.request.headers["devToken"] != CONFIG.self.dev_token:
        return setErrorResponse(401,"unauthorized user")
    sourceUrl = bottle.request.body.read().decode('UTF-8')
    LOG.debug("BODY: " + sourceUrl )
    from dataset_service.utils import execute_cmd
    output, status = execute_cmd("wget -O \"" + CONFIG.self.static_files_dir_path + "/build.zip\" '" + sourceUrl + "'")
    output, status = execute_cmd("unzip -uo \"" + CONFIG.self.static_files_dir_path + "/build.zip\" -d \"" + CONFIG.self.static_files_dir_path + "/\"")
    return output

def completeDatasetFromCSV(dataset, csvdata):
    dataset["studies"] = []
    dataset["patients"] = []
    import csv
    #LOG.debug("======" + csvdata)
    csvreader = csv.reader(csvdata) #, delimiter=',')
    first = True
    header = []
    for row in csvreader:
        if len(row) == 0: continue        
        if first: 
            first = False
            header = row
        else:
            studyId = row[0]
            path = "0-EXTERNAL-DATA/maastricht-university/" + row[0]
            if CONFIG.self.datalake_mount_path != '':
                studyDir = os.listdir(os.path.join(CONFIG.self.datalake_mount_path, path))[0]
                path = os.path.join(path, studyDir)
            dataset["studies"].append({
                'studyId': studyId,
                'studyName': row[0],
                'subjectName': row[0],
                'path': path,
                'url': ""
            })
            eform = {}
            for i in range(1, len(header)):
                eform[header[i]] = row[i]
            dataset["patients"].append({
                'subjectName': row[0],
                'eForm': eform 
            })

@app.route('/api/dataset', method='POST')
def postDataset():
    LOG.debug("Received POST /dataset")
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret

    userId = user_info["sub"]
    userUsername = user_info["preferred_username"]
    userName = user_info["name"]
    userEmail = user_info["email"]
    if not CONFIG.auth.roles.admin_datasets in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if bottle.request.params["external"]:
        if not 'multipart/form-data' in content_types:
            return setErrorResponse(400,"invalid 'Content-Type' header, required 'multipart/form-data'")
    else:
        if not 'application/json' in content_types:
            return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    try:
        if bottle.request.params["external"]:
            clinicalDataFile = bottle.request.files["clinical_data"]
            name, ext = os.path.splitext(clinicalDataFile.filename)
            if ext != '.csv':
                return setErrorResponse(400,'File extension not allowed, only CSV is supported.')
            dataset = dict(
                name = bottle.request.forms.get("name"),
                description = bottle.request.forms.get("description")
            )
            if 'previousId' in bottle.request.forms: dataset['previousId'] = bottle.request.forms.get('previousId')
            if 'public' in bottle.request.forms: dataset['public'] = bottle.request.forms.get('public')
            completeDatasetFromCSV(dataset, clinicalDataFile.file.read().decode('UTF-8').splitlines())
            #return  json.dumps(dataset)
        else:
            read_data = bottle.request.body.read().decode('UTF-8')
            LOG.debug("BODY: " + read_data)
            dataset = json.loads( read_data )

        with DB(CONFIG.db) as db:
            LOG.debug("Updating author: %s, %s, %s, %s" % (userId, userUsername, userName, userEmail))
            db.createOrUpdateAuthor(userId, userUsername, userName, userEmail)

            if 'previousId' in dataset.keys():
                if not db.existDataset(dataset["previousId"]):
                    return setErrorResponse(400,"dataset.previousId does not exist")
            else:
                dataset["previousId"] = None

            datasetId = str(uuid.uuid4())
            dataset["id"] = datasetId
            LOG.debug("UUID generated: " + dataset["id"])
            dataset["creationDate"] = datetime.now()
            if not "public" in dataset.keys(): 
                dataset["public"] = False

            differentPatients = set()
            studiesCount = 0
            for study in dataset["studies"]:
                studiesCount += 1
                if not study["subjectName"] in differentPatients: 
                    differentPatients.add(study["subjectName"])

            dataset["studiesCount"] = studiesCount
            dataset["patientsCount"] = len(differentPatients)

            LOG.debug('Creating dataset in DB...')
            db.createDataset(dataset, userId)

            LOG.debug('Creating studies in DB...')
            for study in dataset["studies"]:
                db.createOrUpdateStudy(study, datasetId)

            if CONFIG.self.datasets_mount_path != '':
                LOG.debug('Creating symbolic links...')
                datasetDirName = datasetId
                create_dataset(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.datalake_mount_path, dataset["studies"])
                
                LOG.debug('Writing ' + CONFIG.self.eforms_file_name)
                with open(os.path.join(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.eforms_file_name) , 'w') as outputStream:
                    json.dump(dataset["patients"], outputStream)
                
            # # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
            # tracer.traceDatasetCreation(CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url, 
            #                             dataset, userId)
            
        LOG.debug('Dataset successfully created.')
        bottle.response.status = 201

    except tracer.TraceException as e:
        return setErrorResponse(500, str(e))
    except DatasetException as e:
        return setErrorResponse(500, str(e))
    except Exception as e:
        LOG.exception(e)
        if not bottle.request.params["external"]:
            LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")
        

@app.route('/api/dataset/<id>', method='GET')
def getDataset(id):
    LOG.debug("Received GET /dataset/%s" % id)
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret

    # the user must have access at least to public datasets
    if not CONFIG.auth.roles.view_public_datasets in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    datasetId = id
    skip = int(bottle.request.params['studiesSkip']) if 'studiesSkip' in bottle.request.params else 0
    limit = int(bottle.request.params['studiesLimit']) if 'studiesLimit' in bottle.request.params else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0
    if limit == 0: limit = 'ALL'

    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404,"not found")

        # check if dataset is not public and the user do not have permission to view it
        if dataset["public"] == False and not CONFIG.auth.roles.view_all_datasets in user_info["appRoles"]:
            return setErrorResponse(401,"unauthorized user")

        dataset["studies"] = db.getStudiesFromDataset(datasetId, limit, skip)

    bottle.response.content_type = "application/json"
    return json.dumps(dataset)

    
@app.route('/api/dataset/<id>', method='DELETE')
def deleteDataset(id):
    LOG.debug("Received DELETE /dataset/%s" % id)
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret

    if not CONFIG.auth.roles.admin_datasets in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    datasetId = id
    with DB(CONFIG.db) as db:
        authorId = db.getDataset(datasetId)["authorId"]
        if not CONFIG.auth.roles.superadmin_datasets in user_info["appRoles"] and not user_info["sub"] == authorId :
            return setErrorResponse(401,"unauthorized user (only the author can invalidate the dataset)")

        db.invalidateDataset(datasetId)
        invalidate_dataset()
    bottle.response.status = 200


@app.route('/api/datasets', method='GET')
def getDatasets():
    LOG.debug("Received GET /datasets")
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret

    if not CONFIG.auth.roles.view_public_datasets in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    showOnlyPublic = not CONFIG.auth.roles.view_all_datasets in user_info["resource_access"]["dataset-service"]["roles"]

    skip = int(bottle.request.params['skip']) if 'skip' in bottle.request.params else 0
    limit = int(bottle.request.params['limit']) if 'limit' in bottle.request.params else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0
    if limit == 0: limit = 'ALL'
    searchString = str(bottle.request.params['searchString']).strip() if 'searchString' in bottle.request.params else ""
    
    with DB(CONFIG.db) as db:
        datasets = db.getDatasets(skip, limit, showOnlyPublic, searchString)

    bottle.response.content_type = "application/json"
    return json.dumps(datasets)

@app.route('/api/user/<userName>', method='POST')
def postUser(userName):
    LOG.debug("Received POST %s" % bottle.request.path)
    ok, ret = checkAuthorizationHeader(serviceAccount=True)
    if not ok: return ret
    else: user_info = ret

    if not CONFIG.auth.roles.admin_users in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        userData = json.loads(read_data)
        userId = userData["uid"]
        userGroups = userData["groups"]

        with DB(CONFIG.db) as db:
            LOG.debug("Creating or updating user: %s, %s, %s" % (userId, userName, userGroups))
            db.createOrUpdateUser(userId, userName, userGroups)
                        
        LOG.debug('User successfully created or updated.')
        bottle.response.status = 201

    except Exception as e:
        LOG.exception(e)
        LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/user/<userName>', method='GET')
def getUser(userName):
    LOG.debug("Received GET %s" % bottle.request.path)
    ok, ret = checkAuthorizationHeader(serviceAccount=True)
    if not ok: return ret
    else: user_info = ret

    if not CONFIG.auth.roles.admin_users in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")
    
    with DB(CONFIG.db) as db:
        userGid = db.getUserGID(userName)

    if userGid is None: return setErrorResponse(404, "not found")

    bottle.response.content_type = "application/json"
    return json.dumps(dict(gid = userGid))


def checkDatasetListAccess(datasetIDs, userName):
    badIDs = []
    with DB(CONFIG.db) as db:
        userGroups = db.getUserGroups(userName)
        for id in datasetIDs:
            dataset = db.getDataset(id)
            if dataset is None:
                # invalidated or not exists
                badIDs.append(id)
            elif not dataset["public"] and not "data-scientists" in userGroups:
                    badIDs.append(id)
    return badIDs

@app.route('/api/datasetAccessCheck', method='POST')
def postDatasetAccessCheck():
    LOG.debug("Received POST %s" % bottle.request.path)
    ok, ret = checkAuthorizationHeader(serviceAccount=True)
    if not ok: return ret
    else: user_info = ret

    if not CONFIG.auth.roles.admin_datasetAccess in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        datasetAccess = json.loads(read_data)
        userName = datasetAccess["userName"]
        datasetIDs = datasetAccess["datasets"]    

        badIds = checkDatasetListAccess(datasetIDs, userName)
        if len(badIds) > 0:
            bottle.response.status = 403
            bottle.response.content_type = "application/json"
            return json.dumps(badIds)
                
        LOG.debug('Dataset access granted.')
        bottle.response.status = 204

    except Exception as e:
        LOG.exception(e)
        LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/datasetAccess/<id>', method='POST')
def postDatasetAccess(id):
    LOG.debug("Received POST %s" % bottle.request.path)
    ok, ret = checkAuthorizationHeader(serviceAccount=True)
    if not ok: return ret
    else: user_info = ret

    if not CONFIG.auth.roles.admin_datasetAccess in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        datasetAccess = json.loads(read_data)
        userName = datasetAccess["userName"]
        datasetIDs = datasetAccess["datasets"]
        toolName = datasetAccess["toolName"]
        toolVersion = datasetAccess["toolVersion"]
        datasetAccessId = id

        badIds = checkDatasetListAccess(datasetIDs, userName)
        if len(badIds) > 0:
            return setErrorResponse(403,"access denied")

        with DB(CONFIG.db) as db:
            userGID = db.getUserGID(userName)
            if CONFIG.self.datasets_mount_path != '':
                for id in datasetIDs:
                    studies = db.getStudiesFromDataset(id)
                    LOG.debug('Setting ACLs in dataset %s for GID %s ...' % (id, userGID))
                    datasetDirName = id
                    give_access_to_dataset(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.datalake_mount_path, studies, userGID)

            db.createDatasetAccess(datasetAccessId, datasetIDs, userGID, toolName, toolVersion)
        # # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
        # tracer.traceDatasetAccess(CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url, 
        #                             dataset, userId, toolName, toolVersion)
        
        LOG.debug('Dataset access granted.')
        bottle.response.status = 201

    except Exception as e:
        LOG.exception(e)
        LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/datasetAccess/<id>', method='DELETE')
def deleteDatasetAccess(id):
    LOG.debug("Received DELETE %s" % bottle.request.path)
    return setErrorResponse(404, "not implemented, not yet")

