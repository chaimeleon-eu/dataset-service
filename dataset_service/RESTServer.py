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

def checkAuthorizationHeader():
    try:
        encodedToken = bottle.request.headers["authorization"][7:]
    except Exception as e:
        return False, setErrorResponse(401, "authorization header is missing or invalid")
    user_info = validate_token(encodedToken)
    if not "sub" in user_info.keys(): return False, setErrorResponse(401,"invalid access token: missing 'sub'")
    if not "name" in user_info.keys(): return False, setErrorResponse(401,"invalid access token: missing 'name'")
    if not "email" in user_info.keys(): return False, setErrorResponse(401,"invalid access token: missing 'email'")
    try:
        user_info["appRoles"] = user_info["resource_access"]["dataset-service"]["roles"]
    except:
        return False, setErrorResponse(401,"invalid access token: missing 'resource_access.dataset-service.roles'")
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
def RESTGetInfrastructureIsudonfo():
    LOG.debug("Received GET /")
    bottle.response.content_type = "text/plain"
    return "Hello from Dataset Service"


@app.route('/dataset', method='POST')
def postDataset():
    LOG.debug("Received POST /dataset")
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret

    userId = user_info["sub"]
    userName = user_info["name"]
    userEmail = user_info["email"]
    if not CONFIG.auth.roles.admin_datasets in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        dataset = json.loads( read_data )
        with DB(CONFIG.db) as db:
            LOG.debug("Updating author: %s, %s, %s" % (userId, userName, userEmail))
            db.createOrUpdateAuthor(userId, userName, userEmail)

            if 'previousId' in dataset.keys():
                if not db.existDataset(dataset["previousId"]):
                    return setErrorResponse(400,"dataset.previousId does not exist")
            else:
                dataset["previousId"] = None

            datasetId = str(uuid.uuid4())
            dataset["id"] = datasetId
            LOG.debug("UUID generated: " + dataset["id"])
            dataset["creationDate"] = datetime.now()
            dataset["gid"] = 1
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

            db.createDataset(dataset, userId)

            for study in dataset["studies"]:
                db.createStudy(study, datasetId)

        bottle.response.status = 201

    except Exception as e:
        LOG.exception(e)
        LOG.error("Wrong body of the request: %s" % read_data)
        return setErrorResponse(400,"invalid input, object invalid")
        

@app.route('/dataset/<id>', method='GET')
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
        if dataset == None:
            return setErrorResponse(404,"not found")

        # check if dataset is not public and the user do not have permission to view it
        if dataset["public"] == False and not CONFIG.auth.roles.view_all_datasets in user_info["appRoles"]:
            return setErrorResponse(401,"unauthorized user")

        dataset["studies"] = db.getStudiesFromDataset(datasetId, limit, skip)

    bottle.response.content_type = "application/json"
    return json.dumps(dataset)

    
@app.route('/dataset/<id>', method='DELETE')
def deleteDataset(id):
    LOG.debug("Received DELETE /dataset/%s" % id)
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret

    if not CONFIG.auth.roles.admin_datasets in user_info["appRoles"]:
        return setErrorResponse(401,"unauthorized user")

    datasetId = id
    with DB(CONFIG.db) as db:
        dataset = db.invalidateDataset(datasetId)

    bottle.response.status = 200


@app.route('/datasets', method='GET')
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

