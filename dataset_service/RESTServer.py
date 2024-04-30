#! /usr/bin/env python3

import os
import io
from enum import Enum
from datetime import datetime
from pathlib import Path
import bottle
import logging
import json
#from kubernetes.client import exceptions
import yaml
import time
#from kubernetes import client, config
#from kubernetes.client.rest import ApiException
import jwt
from jwt import PyJWKClient
import urllib
import urllib.error
import uuid
from dataset_service import authorization, k8s, pid, tracer, keycloak, config
from dataset_service.auth import AuthClient, LoginException
from dataset_service.storage import DB
import dataset_service.dataset as dataset_file_system

#LOG = logging.getLogger('module1')
LOG = logging.root

class MyBottle(bottle.Bottle):
    # This method overwrite is just to translate the body of error responses to JSON format
    def default_error_handler(self, res):
        bottle.response.content_type = 'application/json'
        return json.dumps(dict(error = res.body, status_code = res.status_code))
    # Below method overwrite is not required in fact (all should work if we delete it).
    # It is a simplified version or the original method (https://github.com/bottlepy/bottle/blob/release-0.12/bottle.py#L788)
    # just to avoid the pylance warning 'Object of type "ModuleType" is no callable'.
    # Usefull link to understand the code: https://stackoverflow.com/a/33777519
    def route(self, path=None, method='GET', **config):
        def decorator(callback):
            for rule in bottle.makelist(path) or bottle.yieldroutes(callback):
                for verb in bottle.makelist(method):
                    self.add_route(bottle.Route(self, rule, verb.upper(), callback, **config))
            return callback
        return decorator

# The code below is just to send the exception trace to the log
def exception_catch(func):
    def wrapper(*args,**kwargs):
        try:
            return func(*args,**kwargs)
        except jwt.exceptions.PyJWTError as e1:
            bottle.response.status = 401
            LOG.error("invalid access token: " + str(type(e1)) + str(e1))
            bottle.response.content_type = 'application/json'
            return json.dumps(dict(error = "invalid access token, " + str(e1)))
        except Exception as e:
            LOG.exception(e)
            raise e
    return wrapper

#app = bottle.Bottle()
app = MyBottle()
app.install(exception_catch)
thisRESTServer = None
CONFIG = None
AUTH_PUBLIC_KEY = None
AUTH_CLIENT = None
AUTH_ADMIN_CLIENT = None

class RESTServer (bottle.ServerAdapter):
    def run(self, handler):
        # https://bottlepy.org/docs/stable/deployment.html#switching-the-server-backend
        # https://github.com/bottlepy/bottle/blob/release-0.12/bottle.py#L2834
        from cheroot import wsgi
        server = wsgi.Server((self.host, self.port), handler, request_queue_size=32)
        # old version:
        # from cherrypy import wsgiserver
        # server = wsgiserver.CherryPyWSGIServer((self.host, self.port), handler, request_queue_size=32)
        self.srv = server
        try:
            server.start()
        finally:
            server.stop()

    def shutdown(self):
        if self.srv:
            self.srv.stop()

def run(host, port, config: config.Config):
    global thisRESTServer, LOG, CONFIG, AUTH_PUBLIC_KEY, AUTH_CLIENT, AUTH_ADMIN_CLIENT
    CONFIG = config
    authorization.User.roles = authorization.Roles(CONFIG.auth.token_validation.roles)
    AUTH_CLIENT = AuthClient(CONFIG.auth.client.auth_url, CONFIG.auth.client.client_id, CONFIG.auth.client.client_secret)
    AUTH_ADMIN_CLIENT = keycloak.KeycloakAdminAPIClient(AUTH_CLIENT, CONFIG.auth.admin_api_url, CONFIG.auth.client.client_id)

    LOG.info("Obtaining the public key from %s..." % CONFIG.auth.token_validation.token_issuer_public_keys_url)
    LOG.info("kid: %s" % CONFIG.auth.token_validation.kid)
    try:
        retries = 0
        while retries < 5:
            try:
                jwks_client = PyJWKClient(CONFIG.auth.token_validation.token_issuer_public_keys_url)
                AUTH_PUBLIC_KEY = jwks_client.get_signing_key(CONFIG.auth.token_validation.kid)
                LOG.info("key: %s" % str(AUTH_PUBLIC_KEY.key))
                break
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
            LOG.info("Checking database...")
            db.setup()
    except Exception as e:
        LOG.exception(e)
        raise e

    if CONFIG.self.datasets_mount_path == '':
        LOG.warn("datasets_mount_path is empty: datasets will not be created on disk, they will be only stored in database.")
    else: 
        dataset_file_system.check_file_system(CONFIG.self.datalake_mount_path, CONFIG.self.datasets_mount_path)
    
    if CONFIG.tracer.url == '':
        LOG.warn("tracer.url is empty: actions will not be notified to the tracer-service.")
    else: 
        tracer.check_connection(AUTH_CLIENT, CONFIG.tracer.url)
        
    thisRESTServer = RESTServer(host=host, port=port)
    LOG.info("Running the service in %s:%s..." % (host, port))
    bottle.BaseRequest.MEMFILE_MAX = 120 * 1024 * 1024   # In bytes, default 102400
                                                        # We have to increase to avoid error "413: request entity too large" 
                                                        # when creating dataset.
    bottle.run(app, server=thisRESTServer, quiet=True)

def stop():
    if thisRESTServer:
        LOG.info("Shutting down the service...")
        thisRESTServer.shutdown()


def setErrorResponse(code, message):
    LOG.debug("Sending error code %d, with message: %s" % (code, message))
    bottle.response.status = code
    bottle.response.content_type = "application/json"
    return json.dumps(dict(error = message, status_code = code))

def validate_token(token) -> dict:
    if AUTH_PUBLIC_KEY is None or CONFIG is None: raise Exception()
    headers = jwt.get_unverified_header(token)
    LOG.debug("Token key id (kid header):" + headers['kid'])

    #AUTH_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\n" + CONFIG.auth.token_issuer_public_key + "\n-----END PUBLIC KEY-----"
    decodedToken = jwt.decode(token, AUTH_PUBLIC_KEY.key, algorithms=['RS256'],  
                              issuer=CONFIG.auth.token_validation.issuer, audience=CONFIG.auth.token_validation.audience, 
                              options={'verify_signature': True, 'require': ["exp", "iat", "iss", "aud"]})
    #LOG.debug(json.dumps(decodedToken))
    return decodedToken

def getTokenFromAuthorizationHeader(serviceAccount=False) -> str | None | dict:
    ''' Returns: 
        - str with the error message in case of fail
        - None if unregistered user
        - dict with the token if success
    '''
    encodedToken = bottle.request.get_header("authorization")
    if encodedToken is None: 
        LOG.debug("User: not registered")
        return None
    try:
        encodedToken = encodedToken[7:]
    except Exception as e:
        return setErrorResponse(401, "invalid authorization header")
    token = validate_token(encodedToken)
    ok, missingProperty = authorization.User.validateToken(token, serviceAccount)
    if ok: LOG.debug("User: " + token['preferred_username'])
    else: 
        LOG.debug(json.dumps(token))
        return setErrorResponse(401,"invalid access token: missing '%s'" % missingProperty)
    return token

def getTokenOfAUserFromAuthAdminClient(userId) -> str | dict:
    ''' Returns: 
        - str with the error message in case of fail
        - dict with the token if success
    '''
    if AUTH_ADMIN_CLIENT is None: raise Exception()
    token = AUTH_ADMIN_CLIENT.getUserToken(userId)
    ok, missingProperty = authorization.User.validateToken(token, False)
    if not ok: return setErrorResponse(401,"invalid access token: missing '%s'" % missingProperty)
    return token

def get_header_media_types(header):
    """
    Function to get the media types specified in a header.
    Returns a List of strings.
    """
    res = []
    accept = bottle.request.get_header(header)
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


@app.route('/api/', method='GET')
def getHello():
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    bottle.response.content_type = "text/plain"
    return "Hello from Dataset Service"
# Aternative to decorator:
# app.route('/api/', method='GET', callback=getHello)

@app.route('/health', method='GET')
def getAlive():
    #LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))   not fill the log with that
    bottle.response.content_type = "text/plain"
    return "ok"

@app.route('/api/set-ui', method='POST')
def postSetUI():
    if CONFIG is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    if CONFIG.self.dev_token == "":
        return setErrorResponse(404, "Not found: '%s'" % bottle.request.path)
    if bottle.request.get_header("devToken") != CONFIG.self.dev_token:
        return setErrorResponse(401,"unauthorized user")
    sourceUrl = bottle.request.body.read().decode('UTF-8')
    LOG.debug("BODY: " + sourceUrl )
    from dataset_service.utils import execute_cmd
    output, status = execute_cmd("wget -O \"" + CONFIG.self.static_files_dir_path + "/build.zip\" '" + sourceUrl + "'")
    output, status = execute_cmd("unzip -uo \"" + CONFIG.self.static_files_dir_path + "/build.zip\" -d \"" + CONFIG.self.static_files_dir_path + "/\"")
    return output

@app.route('/datalakeinfo/<file_path:re:.*\.(json)>', method='GET')
def getDatalakeInfo(file_path):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    LOG.debug("Static file: "+file_path)
    if CONFIG.self.datalakeinfo_token == "":
        return setErrorResponse(404, "Not found: '%s'" % bottle.request.path)
    if bottle.request.get_header("secret") != CONFIG.self.datalakeinfo_token:
        return setErrorResponse(401,"unauthorized user")
    return bottle.static_file(file_path, CONFIG.self.datalakeinfo_dir_path)


class WrongInputException(Exception): pass
class K8sException(Exception): pass

def checkPath(basePath: str, relativePath: str):
    ''' Ensures relativePath is in fact a subpath of basePath. Raises an exception if wrong path.
        This is to avoid a malicious user try to access system directories with "..", absolute paths like "/etc" or symbolic links.
    '''
    base = Path(basePath).resolve()  # Resolve symbolic links and returns absolute path
    path = (base / relativePath).resolve()
    if not base in path.parents:
        raise WrongInputException("Wrong path: " + str(base / relativePath))

def completeDatasetFromCSV(dataset, csvdata):
    if CONFIG is None: raise Exception()
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
            subjectName = row[0]
            subjectPath = "0-EXTERNAL-DATA/maastricht-university/" + row[0]
            if CONFIG.self.datalake_mount_path != '':
                checkPath(CONFIG.self.datalake_mount_path, subjectPath)
                studies = os.listdir(os.path.join(CONFIG.self.datalake_mount_path, subjectPath))
                for studyDirName in studies:
                    if studyDirName == "NIFTI": continue    # ignore special studies
                    studyDirPath = os.path.join(subjectPath, studyDirName)
                    series = []
                    for serieDirName in os.listdir(os.path.join(CONFIG.self.datalake_mount_path, studyDirPath)):
                        seriePath = os.path.join(CONFIG.self.datalake_mount_path, studyDirPath, serieDirName)
                        if os.path.isdir(seriePath):  series.append({'folderName': serieDirName, 'tags': []})

                    dataset["studies"].append({
                        'studyId': str(uuid.uuid4()),
                        'studyName': studyDirName,
                        'subjectName': subjectName,
                        'pathInDatalake': studyDirPath,
                        'series': series,
                        'url': ""
                    })
            eform = {}
            for i in range(1, len(header)):
                eform[header[i]] = row[i]
            dataset["subjects"].append({
                'subjectName': subjectName,
                'eForm': eform 
            })

def _checkPropertyAsArrayOfStrings(propName: str, value: list[str], item_possible_values: list[str]):
    if not isinstance(value, list):
        raise WrongInputException("'%s' must be an array of strings." % propName)
    for item in value:
        if not isinstance(item, str): 
            raise WrongInputException("'%s' must be an array of strings." % propName)
        if not item in item_possible_values:
            return setErrorResponse(400, "unknown item value '%s' in '%s', it should be one of {%s}" \
                                         % (item, propName, ','.join(item_possible_values)))

ITEM_POSSIBLE_VALUES_FOR_TYPE = ['original', 'annotated', 'processed']
ITEM_POSSIBLE_VALUES_FOR_COLLECTION_METHOD = ['patient-based', 'cohort', 'only-image', 'longitudinal', 'case-control', 'disease-specific']

@app.route('/api/datasets', method='POST')
def postDataset():
    if CONFIG is None \
       or not isinstance(bottle.request.query, bottle.FormsDict) \
       or not isinstance(bottle.request.forms, bottle.FormsDict) \
       or not isinstance(bottle.request.files, bottle.FormsDict) \
       or not isinstance(bottle.request.body, io.IOBase): 
        raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if user.isUnregistered() or not user.canCreateDatasets():
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if "external" in bottle.request.query and bottle.request.query["external"].lower() == "true":
        if not 'multipart/form-data' in content_types:
            return setErrorResponse(400,"invalid 'Content-Type' header, required 'multipart/form-data'")
    else:
        if not 'application/json' in content_types:
            return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    datasetDirName = ''
    datasetId = str(uuid.uuid4())
    read_data = None
    try:
        if "external" in bottle.request.query and bottle.request.query["external"].lower() == "true":
            # This is for manually create datasets out of the standard ingestion procedure 
            clinicalDataFile = bottle.request.files["clinical_data"]
            name, ext = os.path.splitext(clinicalDataFile.filename)
            if ext != '.csv':
                return setErrorResponse(400,'File extension not allowed, only CSV is supported.')
            dataset = dict(
                name = bottle.request.forms.get("name"),
                description = bottle.request.forms.get("description")
            )
            if 'previousId' in bottle.request.forms: dataset['previousId'] = bottle.request.forms.get('previousId')
            #if 'public' in bottle.request.forms: dataset['public'] = bottle.request.forms.get('public')
            dataset["studies"] = []
            dataset["subjects"] = []
            completeDatasetFromCSV(dataset, clinicalDataFile.file.read().decode('UTF-8').splitlines())
            #return  json.dumps(dataset)
        else:
            read_data = bottle.request.body.read().decode('UTF-8')
            #LOG.debug("BODY: " + read_data)
            dataset = json.loads( read_data )
            if not isinstance(dataset, dict): raise WrongInputException("The body must be a json object.")
            if not 'version' in dataset.keys(): dataset["version"] = ''
            if not 'purpose' in dataset.keys(): dataset["purpose"] = ''
            if 'type' in dataset.keys(): 
                _checkPropertyAsArrayOfStrings('type', dataset["type"], ITEM_POSSIBLE_VALUES_FOR_TYPE)
            else: dataset["type"] = []
            if 'collectionMethod' in dataset.keys(): 
                _checkPropertyAsArrayOfStrings('collectionMethod', dataset["collectionMethod"], ITEM_POSSIBLE_VALUES_FOR_COLLECTION_METHOD)
            else: dataset["collectionMethod"] = []
            if not 'studies' in dataset.keys() or not isinstance(dataset["studies"], list): 
                raise WrongInputException("'studies' property is required and must be an array.")
            if not 'subjects' in dataset.keys() or not isinstance(dataset["subjects"], list): 
                raise WrongInputException("'subjects' property is required and must be an array.")
            
            # Security check: review all data sent by the user which is involved in path constructions
            if CONFIG.self.datalake_mount_path != '':
                for study in dataset["studies"]:
                    checkPath(CONFIG.self.datalake_mount_path, study['pathInDatalake'])
                    for serie in study["series"]:
                        checkPath(CONFIG.self.datalake_mount_path, os.path.join(study['pathInDatalake'], serie["folderName"]))
            if CONFIG.self.datasets_mount_path != '':
                for study in dataset["studies"]:
                    checkPath(CONFIG.self.datasets_mount_path, os.path.join(datasetId, study['subjectName']))

            # Integrity checks
            subjects = set()
            study_paths = set()
            for subject in dataset["subjects"]:
                if subject["subjectName"] in subjects:
                    raise WrongInputException("The subjectName '%s' is duplicated in 'subjects' array of the dataset." % subject["subjectName"])
                else:
                    subjects.add(subject["subjectName"])
            for study in dataset["studies"]:
                if not study["subjectName"] in subjects:
                    raise WrongInputException("The study with id '%s' has a 'subjectName' which is not in the " % study["studyId"]
                                             +"'subjects' array of the dataset." )
                study['pathInDatalake'] = str(study['pathInDatalake']).removesuffix('/')
                studyDirName = os.path.basename(study['pathInDatalake'])
                study_path = os.path.join(study["subjectName"], studyDirName)
                # example of study_path: 17B76FEW/TCPEDITRICOABDOMINOPLVICO20150129
                if study_path in study_paths:
                    raise WrongInputException("The study with id '%s' seems duplicated, " % study["studyId"]
                                             +"it has the same directory name '%s' as another study of the same subject '%s', " % (studyDirName, study["subjectName"])
                                             +"this will cause a conflict when creating the dataset's directories structure." )
                else: study_paths.add(study_path)
            
            # File system checks
            if CONFIG.self.datalake_mount_path != '':
                studiesToDelete = []
                for study in dataset["studies"]:
                    seriesToDelete = []
                    for serie in study["series"]:
                        seriePathInDatalake = os.path.join(CONFIG.self.datalake_mount_path, study['pathInDatalake'], serie['folderName'])
                        if not os.path.exists(seriePathInDatalake):
                            LOG.warn("The directory '%s' does not exist. The serie will not be included in the dataset." % seriePathInDatalake)
                            seriesToDelete.append(serie)
                    for serie in seriesToDelete: 
                        study["series"].remove(serie)
                    if len(study["series"]) == 0:
                        LOG.warn("The study with id '%s' does not have any series. It will not be included in the dataset." % study["studyId"])
                        studiesToDelete.append(study)
                for study in studiesToDelete:
                    dataset["studies"].remove(study)

        with DB(CONFIG.db) as db:
            LOG.debug("Updating author: %s, %s, %s, %s" % (user.uid, user.username, user.name, user.email))
            db.createOrUpdateAuthor(user.uid, user.username, user.name, user.email)

            user_projects = user.getProjects()
            if 'project' in dataset.keys(): 
                if not dataset["project"] in user_projects:
                    return setErrorResponse(400,"dataset.project does not exist for the user")
            else:
                dataset["project"] = user_projects.pop()  # we take the first project of user
            
            if 'previousId' in dataset.keys():
                previousDataset = db.getDataset(dataset["previousId"])
                if previousDataset is None:
                    return setErrorResponse(400,"dataset.previousId does not exist")
                if not user.canModifyDataset(previousDataset):
                    return setErrorResponse(401,"the dataset selected as previous (%s) "
                                               +"must be editable by the user (%s)" % (previousDataset["id"], user.username))
            else:
                dataset["previousId"] = None

            LOG.debug("UUID generated: " + datasetId)
            dataset["id"] = datasetId
            dataset["creationDate"] = datetime.now()
            #if not "public" in dataset.keys(): 
            dataset["public"] = False

            dataset["studiesCount"] = len(dataset["studies"])
            dataset["subjectsCount"] = 0   # let's set now to zero 
                                           # because the real number is obtained later, after a check for duplicates
            # The rest of Metada will be collected later, now let's leave it empty (default values in DB)
            
            LOG.debug('Creating dataset in DB...')
            db.createDataset(dataset, user.uid)

            LOG.debug('Creating studies in DB...')
            for study in dataset["studies"]:
                db.createOrUpdateStudy(study, datasetId)

            LOG.debug('Creating dataset directory...')
            datasetDirName = datasetId
            dataset_file_system.create_dataset_dir(CONFIG.self.datasets_mount_path, datasetDirName)
            datasetDirPath = os.path.join(CONFIG.self.datasets_mount_path, datasetDirName)

            LOG.debug('Writing E-FORM: ' + CONFIG.self.eforms_file_name)
            with open(os.path.join(datasetDirPath, CONFIG.self.eforms_file_name) , 'w') as outputStream:
                json.dump(dataset["subjects"], outputStream)

            LOG.debug('Writing INDEX: ' + CONFIG.self.index_file_name)
            # dataset["studies"] contains just the information we want to save in the index.json file,
            # but we have to set paths relative to the dataset directory
            for study in dataset["studies"]:
                subjectDirName = study['subjectName']
                studyDirName = os.path.basename(study['pathInDatalake'])
                # example of study['path']: 17B76FEW/TCPEDITRICOABDOMINOPLVICO20150129
                study['path'] = os.path.join(subjectDirName, studyDirName)
                del study['pathInDatalake']
            # and dump to the index file in the dataset directory
            with open(os.path.join(datasetDirPath, CONFIG.self.index_file_name) , 'w') as outputStream:
                json.dump(dataset["studies"], outputStream)

            LOG.debug('Creating status in DB...')
            db.createDatasetCreationStatus(datasetId, "pending", "Launching dataset creation job...")
            LOG.debug('Launching dataset creation job...')
            k8sClient = k8s.K8sClient()
            ok = k8sClient.add_dataset_creation_job(datasetId)
            if not ok: 
                db.setDatasetCreationStatus(datasetId, "error", "Unexpected error launching dataset creation job.")
                raise K8sException("Unexpected error launching dataset creation job.")

        LOG.debug('Dataset successfully created in DB and creation job launched in K8s.')
        bottle.response.status = 201
        bottle.response.content_type = "application/json"
        return json.dumps(dict(apiUrl = "/api/datasets/" + datasetId,
                               url = CONFIG.self.dataset_link_format % datasetId))

    except (WrongInputException, K8sException) as e:
        if datasetDirName != '': dataset_file_system.remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
        return setErrorResponse(400, str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None:
            LOG.error("May be the body of the request is wrong: %s" % read_data)
        if datasetDirName != '': dataset_file_system.remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")
        #return setErrorResponse(500, "Unexpected error, may be the input is wrong\n%s" % str(e))


@app.route('/api/datasets/<id>/adjustFilePermissionsInDatalake', method='POST')
def postDataset_adjustFilePermissionsInDatalake(id):
    '''
    Note: this method is only for admins, to readjust permissions in case they have been changed for any reason.
    '''
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.isSuperAdminDatasets():
        return setErrorResponse(401,"unauthorized user")

    datasetId = id
    with DB(CONFIG.db) as db:
        if not db.existDataset(datasetId):
            return setErrorResponse(404, "not found")
        datasetStudies, total = db.getStudiesFromDataset(datasetId)
        dataset_file_system.adjust_file_permissions_in_datalake(CONFIG.self.datalake_mount_path, datasetStudies)
    
    LOG.debug('Dataset files permissions successfully adjusted.')
    bottle.response.status = 204

def _recollectMetadataForDataset(datasetId):
    if CONFIG is None: raise Exception()
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None: 
            return dict(success=False, msg="Not recollected: dataset removed.")
        if dataset["draft"] and db.getDatasetCreationStatus(datasetId) != None:
            return dict(success=False, msg="Not recollected: it is still being created.")
        datasetStudies, total = db.getStudiesFromDataset(datasetId)
        dataset["studies"] = datasetStudies
        eformsFilePath = os.path.join(CONFIG.self.datasets_mount_path, datasetId, CONFIG.self.eforms_file_name)
        try:
            dataset_file_system.collectMetadata(dataset, CONFIG.self.datalake_mount_path, eformsFilePath)
        except Exception as e:
            LOG.exception(e)
            return dict(success=False, msg="Not recollected: exception catched (corrupt dataset?).")
        db.updateDatasetAndStudyMetadata(dataset)
    return dict(success=True, msg="Successfully recollected.")

@app.route('/api/datasets/recollectMetadata', method='POST')
def recollectMetadataForAllDatasets():
    '''
    Note: this method is only for admins and for special case when upgrade to a new version which adds new metadata fields.
          So, with this method any previous dataset can be rescan for collecting metadata again and fill all the fields.
    '''
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.isSuperAdminDatasets():
        return setErrorResponse(401,"unauthorized user")

    with DB(CONFIG.db) as db:
        searchFilter = authorization.Search_filter()
        searchFilter.adjustByUser(user)
        datasets, total =  db.getDatasets(0, 0, '', searchFilter, '', '')
        LOG.debug("Total datasets to process: %d" % total)
        results = []
        i = 0
        for ds in datasets:
            i += 1
            LOG.debug('Collecting metadata for dataset %s [%d/%d]' % (ds['id'], i, total))
            result = _recollectMetadataForDataset(ds['id'])
            results.append(dict(id=ds['id'], result=result))
            LOG.debug('Result: %s' % json.dumps(result))
    LOG.debug('End of datasets metadata recollection.')
    bottle.response.status = 200
    bottle.response.content_type = "application/json"
    return json.dumps(results)

@app.route('/api/datasets/<id>/relaunchCreationJob', method='POST')
def relaunchDatasetCreationJob(id):
    '''
    Note: this method is only for admins and for special case when a creation job is interrupted (and fail) for any reason.
          So, when the problem is solved, then the admin can launch another creation job in k8s in order to complete the process of creation.
    '''
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.isSuperAdminDatasets():
        return setErrorResponse(401,"unauthorized user")
    datasetId = id
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None: 
            return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        if not user.canRelaunchDatasetCreation(dataset):
            return setErrorResponse(401,"unauthorized user")
        k8sClient = k8s.K8sClient()
        job = k8sClient.exist_dataset_creation_job(datasetId)
        if job:
            if k8sClient.is_running_dataset_creation_job(job):
                return setErrorResponse(400,"there is a creation job running for this dataset, please stop or delete it before relaunch")
            else: k8sClient.delete_dataset_creation_job(datasetId)
        LOG.debug('Updating status in DB...')
        db.setDatasetCreationStatus(datasetId, "pending", "Relaunching dataset creation job...")
        LOG.debug('Relaunching dataset creation job in k8s...')
        ok = k8sClient.add_dataset_creation_job(datasetId)
        if not ok: 
            db.setDatasetCreationStatus(datasetId, "error", "Unexpected error launching dataset creation job.")
            raise K8sException("Unexpected error launching dataset creation job.")
    LOG.debug('Dataset creation job successfully launched in K8s.')
    bottle.response.status = 204
    
@app.route('/api/datasets/<id>/creationStatus', method='GET')
def getDatasetCreationStatus(id):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    datasetId = id
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404,"not found")

        # check access permission
        if not user.canViewDatasetDetails(dataset):
            return setErrorResponse(401,"unauthorized user")

        status = db.getDatasetCreationStatus(datasetId)
        if status is None:
            # The job removes the status in DB at the end of successful creation
            status = dict(datasetId = datasetId, status = "finished", lastMessage = "Successfully created")

    bottle.response.content_type = "application/json"
    return json.dumps(status)

def _checkDatasetIntegrity(datasetId):
    if CONFIG is None or AUTH_CLIENT is None: raise Exception()
    if CONFIG.self.datasets_mount_path == '' or CONFIG.tracer.url == '':
        return dict(success=False, msg="Not checked: file system or tracer no configured.")
    datasetDirName = datasetId
    datasetDirPath = os.path.join(CONFIG.self.datasets_mount_path, datasetDirName)

    studiesHashes = {}
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None: 
            return dict(success=False, msg="Not checked: dataset removed.")
        if dataset["draft"] and db.getDatasetCreationStatus(datasetId) != None:
            return dict(success=False, msg="Not checked: it is still being created.")
        lastCheck = None if dataset["lastIntegrityCheck"] is None \
                        else datetime.fromisoformat(dataset["lastIntegrityCheck"]).timestamp()
        now = datetime.now().timestamp()
        if lastCheck != None and now < (lastCheck + INTEGRITY_CHECK_LIFE_TIME):
            return dict(success=True, msg="Integrity OK (checked on %s)" % lastCheck)
        
        studies, total = db.getStudiesFromDataset(datasetId)
        for study in studies:
            studiesHashes[study["studyId"]] = study["hash"]
    
    wrongHash = tracer.checkDatasetIntegrity(AUTH_CLIENT, CONFIG.tracer.url, datasetId, datasetDirPath,
                                                CONFIG.self.index_file_name, CONFIG.self.eforms_file_name,
                                                studiesHashes)
    with DB(CONFIG.db) as db:
        db.setDatasetLastIntegrityCheck(datasetId, datetime.now())
    if wrongHash is None:
        return dict(success=True, msg="Integrity OK.")
    else: return dict(success=False, msg="Resource hash mismatch: %s" % wrongHash)

@app.route('/api/datasets/<id>/checkIntegrity', method='POST')
def checkDatasetIntegrity(id):
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canCheckIntegrityOfDatasets():
        return setErrorResponse(401,"unauthorized user")
    
    datasetId = id
    result = _checkDatasetIntegrity(datasetId)
    bottle.response.status = 200
    bottle.response.content_type = "application/json"
    return json.dumps(result)

INTEGRITY_CHECK_LIFE_TIME = 30 * 24 * 60 * 60   # 30 days in seconds

@app.route('/api/datasets/checkIntegrity', method='POST')
def checkAllDatasetsIntegrity():
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canCheckIntegrityOfDatasets():
        return setErrorResponse(401,"unauthorized user")

    with DB(CONFIG.db) as db:
        searchFilter = authorization.Search_filter()
        searchFilter.adjustByUser(user)
        datasets, total =  db.getDatasets(0, 0, '', searchFilter, '', '')
        LOG.debug("Total datasets to check: %d" % total)
        results = []
        for ds in datasets:
            result = _checkDatasetIntegrity(ds['id'])
            results.append(dict(id=ds['id'], result=result))

    bottle.response.status = 200
    bottle.response.content_type = "application/json"
    return json.dumps(results)

@app.route('/api/datasets/<id>', method='GET')
def getDataset(id):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    datasetId = id
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404,"not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        datasetACL = db.getDatasetACL(id)
    if not user.canViewDatasetDetails(dataset):
        return setErrorResponse(401,"unauthorized user")
    dataset["editablePropertiesByTheUser"] = user.getEditablePropertiesByTheUser(dataset)
    dataset["allowedActionsForTheUser"] = user.getAllowedActionsForTheUser(dataset, datasetACL)
    if user.isUnregistered():
        del dataset["authorEmail"]
    bottle.response.content_type = "application/json"
    return json.dumps(dataset)

@app.route('/api/datasets/<id>/studies', method='GET')
def getDatasetStudies(id):
    if CONFIG is None or not isinstance(bottle.request.query, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    datasetId = id
    skip = int(bottle.request.query['skip']) if 'skip' in bottle.request.query else 0
    limit = int(bottle.request.query['limit']) if 'limit' in bottle.request.query else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0

    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404,"not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        if not user.canViewDatasetDetails(dataset):
            return setErrorResponse(401,"unauthorized user")

        studies, total = db.getStudiesFromDataset(datasetId, limit, skip)
        username = "unregistered" if user.isUnregistered() else user.username
        for study in studies: 
            # pathInDatalake is an internal info not interesting for the normal user nor unregistered user
            del study['pathInDatalake']
            del study['hash']
            # QuibimPrecision requires to set the username in the url
            study['url'] = str(study['url']).replace("<USER>", username, 1)
            
    bottle.response.content_type = "application/json"
    return json.dumps({ "total": total,
                        "returned": len(studies),
                        "skipped": skip,
                        "limit": limit,
                        "list": studies })

def createZenodoDeposition(db, dataset):
    if CONFIG is None: raise Exception()
    if dataset["pids"]["urls"]["zenodoDoi"] is None: 
        datasetId = dataset["id"]
        studies, total = db.getStudiesFromDataset(datasetId)
        newValue = pid.getZenodoDOI(CONFIG.zenodo.url, CONFIG.zenodo.access_token, dataset, studies, 
                                    CONFIG.self.dataset_link_format, CONFIG.zenodo.community, CONFIG.zenodo.grant)
        db.setZenodoDOI(datasetId, newValue)

def updateZenodoDeposition(db, dataset):
    if CONFIG is None: raise Exception()
    pidUrl = dataset["pids"]["urls"]["zenodoDoi"]
    if pidUrl != None: 
        i = pidUrl.rfind('.') + 1
        depositionId = pidUrl[i:]
        pid.updateZenodoDeposition(CONFIG.zenodo.url, CONFIG.zenodo.access_token, dataset, 
                                   CONFIG.self.dataset_link_format, CONFIG.zenodo.community, CONFIG.zenodo.grant, 
                                   depositionId)

@app.route('/api/datasets/<id>', method='PATCH')
def patchDataset(id):
    if CONFIG is None or AUTH_CLIENT is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if user.isUnregistered():
        return setErrorResponse(401,"unauthorized user")

    datasetId = id
    read_data = bottle.request.body.read().decode('UTF-8')
    LOG.debug("BODY: " + read_data)
    patch = json.loads( read_data )
    property = patch["property"]
    newValue = patch["value"]
    trace_details = None

    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404,"not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        if property not in user.getEditablePropertiesByTheUser(dataset):
            return setErrorResponse(400,"the property is not in the editablePropertiesByTheUser list")

        if property == "draft":
            if bool(newValue) == False and dataset["previousId"] != None:
                # Let's set in the previous dataset the reference to this one.
                # This way, the UI can show a notice in the previous dataset that there is a new version, with a link to this one.
                previousDataset = db.getDataset(dataset["previousId"])
                if previousDataset is None: raise Exception()
                #  Check the previousId is not in other datasets.
                #  It has to be checked here because when released this dataset, the previous will disappear from the upgradable datasets list,
                #  so the other datasets sharing the previousId will be in a wrong state, they will have a previousId not selectable.
                newVersionsOfTheSameDataset = db.getDatasetsSharingPreviousId(dataset["previousId"])
                if len(newVersionsOfTheSameDataset) > 1:
                    return setErrorResponse(400,"There are more than one draft datasets selected as the next version for the same dataset: "
                                               +str(newVersionsOfTheSameDataset) + ". "
                                               +"Please delete or change the previousId in some of them (only one can be the next version) .")
                # The next check can not happen because of the previous check, but it is kept just in case the previous is removed in the future.
                if previousDataset["nextId"] != None:
                    return setErrorResponse(400,"The previousId (%s) is not valid, " % dataset["previousId"]
                                               +"it references to an old dataset which already has a new version (%s). " % previousDataset["nextId"]
                                               +"You may want to set the previousId to that new version (\"rebase\" your dataset), "
                                               +"or you can simply set to null (and put some link to the base dataset in the description).")
                db.setDatasetNextId(previousDataset["id"], datasetId)
            db.setDatasetDraft(datasetId, bool(newValue))
            if bool(newValue) == False:
                trace_details = "RELEASE"
        elif property == "public":
            db.setDatasetPublic(datasetId, bool(newValue))
            dataset["public"] = bool(newValue)
            if bool(newValue) and dataset["pids"]["preferred"] is None:
                # When publish, a PID will be autogenerated if it is still none 
                createZenodoDeposition(db, dataset)
                db.setDatasetPid(datasetId, "zenodoDoi")
            else:
                updateZenodoDeposition(db, dataset)
            trace_details = "PUBLISH" if bool(newValue) else "UNPUBLISH"
        elif property == "invalidated":
            db.setDatasetInvalidated(datasetId, bool(newValue))
            if bool(newValue):
                LOG.debug('Removing ACL entries in dataset %s ...' % (datasetId))
                datasetDirName = datasetId
                dataset_file_system.invalidate_dataset(CONFIG.self.datasets_mount_path, datasetDirName)              
            else:  # reactivated
                db.setDatasetInvalidationReason(datasetId, None)  # reset invalidation reason
            trace_details = "INVALIDATE" if bool(newValue) else "REACTIVATE"
        elif property == "invalidationReason":
            db.setDatasetInvalidationReason(datasetId, str(newValue))
            # Don't notify the tracer, it is just a note to remember the reason.
        elif property == "name":
            db.setDatasetName(datasetId, str(newValue))
            # Don't notify the tracer, this property can be changed only in draft state
        elif property == "version":
            db.setDatasetVersion(datasetId, str(newValue))
            # Don't notify the tracer, this property can be changed only in draft state
        elif property == "description":
            db.setDatasetDescription(datasetId, str(newValue))
            # Don't notify the tracer, this property can be changed only in draft state
        elif property == "purpose":
            db.setDatasetPurpose(datasetId, str(newValue))
            # Don't notify the tracer, this property can be changed only in draft state
        elif property == "type":
            _checkPropertyAsArrayOfStrings("value", newValue, ITEM_POSSIBLE_VALUES_FOR_TYPE)
            db.setDatasetType(datasetId, newValue)
            # Don't notify the tracer, this property can be changed only in draft state
        elif property == "collectionMethod":
            _checkPropertyAsArrayOfStrings("value", newValue, ITEM_POSSIBLE_VALUES_FOR_COLLECTION_METHOD)
            db.setDatasetCollectionMethod(datasetId, newValue)
            # Don't notify the tracer, this property can be changed only in draft state
        # elif property == "project":
        #     newProject = str(newValue)
        #     if not newProject in user.getProjects():
        #         return setErrorResponse(400,"invalid value, unknown project code for the user")
        #     db.setDatasetProject(datasetId, newProject)
        #     # Don't notify the tracer, this property can be changed only in draft state
        elif property == "previousId":
            if newValue != None:
                previousDataset = db.getDataset(str(newValue))
                if previousDataset is None:
                    return setErrorResponse(400,"invalid value, the dataset id does not exist")
                if not user.canModifyDataset(previousDataset):
                    return setErrorResponse(401,"the dataset selected as previous (%s) "
                                               +"must be editable by the user (%s)" % (previousDataset["id"], user.username))
            db.setDatasetPreviousId(datasetId, newValue)  # newValue can be None or str
            # Don't notify the tracer, this property can be changed only in draft state
        elif property == "license":
            newTitle = str(newValue["title"])
            newUrl = str(newValue["url"])
            db.setDatasetLicense(datasetId, newTitle, newUrl)
            # dataset["license"] = dict(title=newTitle,url=newUrl)  license is written in a PDF file
            # updateZenodoDeposition(db, dataset)                   and deposition files cannot be changed once published
            trace_details = "LICENSE_UPDATED"
        elif property == "pids":
            preferred = str(newValue["preferred"]).strip()
            custom = None
            if preferred == "zenodoDoi":
                createZenodoDeposition(db, dataset)
            elif preferred == "custom":
                custom = str(newValue["urls"]["custom"])
                if not custom.startswith("http"):
                    return setErrorResponse(400, "invalid value for urls.custom")
            else: return setErrorResponse(400, "invalid value for preferred")
            db.setDatasetPid(datasetId, preferred, custom)
            trace_details = "PID_UPDATED"
        elif property == "contactInfo":
            db.setDatasetContactInfo(datasetId, str(newValue))
            # dataset["contactInfo"] = str(newValue)       contactInfo is written in a PDF file
            # updateZenodoDeposition(db, dataset)          and deposition files cannot be changed once published
            trace_details = "CONTACT_INFORMATION_UPDATED"
        else:
            return setErrorResponse(400, "invalid property")

        if CONFIG.tracer.url != '' and trace_details != None:
            LOG.debug('Notifying to tracer-service...')
            # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
            tracer.traceDatasetUpdate(AUTH_CLIENT, CONFIG.tracer.url, datasetId, user.uid, trace_details)
    bottle.response.status = 200

@app.route('/api/datasets/<id>/acl', method='GET')
def getDatasetACL(id):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    datasetId = id
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404,"not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        if not user.canManageACL(dataset):
            return setErrorResponse(401,"unauthorized user")
        acl = db.getDatasetACL_detailed(datasetId)
    bottle.response.content_type = "application/json"
    return json.dumps(acl)

class Edit_operation(Enum):
    ADD = 1
    DELETE = 2

def changeDatasetACL(datasetId, username, operation):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404,"dataset not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        if not user.canManageACL(dataset): 
            return setErrorResponse(401,"unauthorized user")
        userId, userGid = db.getUserIDs(username)
        if userId is None: return setErrorResponse(404, "user not found")
        if operation == Edit_operation.ADD:
            LOG.debug("Adding user '%s' to the ACL of dataset %s." % (username, datasetId))
            db.addUserToDatasetACL(datasetId, userId)
            LOG.debug('User successfully added to the ACL.')
            bottle.response.status = 201
        elif operation == Edit_operation.DELETE:
            LOG.debug("Deleting user '%s' from the ACL of dataset %s." % (username, datasetId))
            db.deleteUserFromDatasetACL(datasetId, userId)
            LOG.debug('User successfully deleted from the ACL.')
            bottle.response.status = 204
        else: raise Exception()

@app.route('/api/datasets/<id>/acl/<username>', method='PUT')
def putUserToDatasetACL(id, username):
    changeDatasetACL(id, username, Edit_operation.ADD)

@app.route('/api/datasets/<id>/acl/<username>', method='DELETE')
def deleteUserFromDatasetACL(id, username):
    changeDatasetACL(id, username, Edit_operation.DELETE)


def parse_flag_value(s: str) -> bool | None:
    s = s.lower()
    if s == 'true': return True
    if s == 'false': return False
    return None

@app.route('/api/datasets', method='GET')
def getDatasets():
    if CONFIG is None or not isinstance(bottle.request.query, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    searchFilter = authorization.Search_filter(draft = None, public = None, invalidated = None)
    if 'draft' in bottle.request.query:
        searchFilter.draft = parse_flag_value(bottle.request.query['draft'])
    if 'public' in bottle.request.query:
        searchFilter.public = parse_flag_value(bottle.request.query['public'])
    if 'invalidated' in bottle.request.query:
        searchFilter.invalidated = parse_flag_value(bottle.request.query['invalidated'])
    if 'project' in bottle.request.query:
        project = str(bottle.request.query['project'])
        if not project.replace('-','a').isalnum(): 
            return setErrorResponse(400, "invalid value for parameter 'project', only alphanumeric chars and '-'")
        searchFilter.setSelectedProjects(set([project]))
    searchFilter.adjustByUser(user)

    skip = int(bottle.request.query['skip']) if 'skip' in bottle.request.query else 0
    limit = int(bottle.request.query['limit']) if 'limit' in bottle.request.query else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0
    searchString =  str(bottle.request.query['searchString']).strip()  if 'searchString' in bottle.request.query else ""
    searchSubject = str(bottle.request.query['searchSubject']).strip() if 'searchSubject' in bottle.request.query else ""
    sortBy =        str(bottle.request.query['sortBy']).strip()        if 'sortBy' in bottle.request.query else ""
    sortDirection = str(bottle.request.query['sortDirection']).strip() if 'sortDirection' in bottle.request.query else ""
    
    with DB(CONFIG.db) as db:
        datasets, total = db.getDatasets(skip, limit, searchString, searchFilter, sortBy, sortDirection, searchSubject)
    bottle.response.content_type = "application/json"
    return json.dumps({ "total": total,
                        "returned": len(datasets),
                        "skipped": skip,
                        "limit": limit,
                        "list": datasets })
    
@app.route('/api/datasets/eucaimSearch', method='POST')
def eucaimSearchDatasets():
    if CONFIG is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    if CONFIG.self.eucaim_search_token == "":
        return setErrorResponse(404, "Not found: '%s'" % bottle.request.path)
    if bottle.request.get_header("Authorization") != "Secret " + CONFIG.self.eucaim_search_token:
        return setErrorResponse(401,"unauthorized user")
    
    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")
    read_data = bottle.request.body.read().decode('UTF-8')
    LOG.debug("BODY: " + read_data)
    try:
        search_rq = json.loads(read_data)
        if not isinstance(search_rq, dict): raise WrongInputException("The body must be a json object.")
        if not 'ast' in search_rq: raise WrongInputException("Missing property 'ast' in the request object.")
        if not isinstance(search_rq['ast'], dict): raise WrongInputException("The value of property 'ast' must be a json object.")
        #parseAST()
        with DB(CONFIG.db) as db:
            result = db.eucaimSearchDatasets(0, 0, search_rq['ast'])
            
        LOG.debug('Result: '+json.dumps({'collections': result}))
        bottle.response.status = 200
        bottle.response.content_type = "application/json"
        return json.dumps({'collections': result})
    except (WrongInputException, DB.searchValidationException) as e:
        return setErrorResponse(422, "Request validation error: %s" % e)
    except Exception as e:
        LOG.exception(e)
        if read_data != None:
            LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")


@app.route('/api/upgradableDatasets', method='GET')
def getUpgradableDatasets():
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canCreateDatasets():
        return setErrorResponse(401,"unauthorized user")

    searchFilter = authorization.Upgradables_filter()
    searchFilter.adjustByUser(user)

    with DB(CONFIG.db) as db:
        datasets = db.getUpgradableDatasets(searchFilter)
        
    bottle.response.content_type = "application/json"
    return json.dumps(datasets)


@app.route('/api/datasets/<id>', method='DELETE')
def deleteDataset(id):
    ''' Notes: 
        The normal procedure is to invalidate the datasets, because they can not be deleted from the tracer-service, but it will be hidden;
        this method is only intended for development state and for superadmin_datasets role, 
        to delete test datasets when the tracer-service is also cleaned or is going to be cleaned when changing to the production state.
        Only in case of the dataset is still creating (or with error in creation) (still not in the tracer-service) 
        then it can be deleted normally and by normal users.
    '''
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if user.isUnregistered():
        return setErrorResponse(401, "unauthorized user")

    datasetId = id
    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        if not user.canDeleteDataset(dataset):
            return setErrorResponse(401, "unauthorized user")

        if "creating" in dataset and dataset["creating"]:
            #First of all stop the job
            LOG.debug('Deleting dataset creation job...')
            k8sClient = k8s.K8sClient()
            ok = k8sClient.delete_dataset_creation_job(datasetId)
            if not ok: return setErrorResponse(500, "Unexpected error")
        
        accesses = db.getOpenDatasetAccesses(datasetId)
        if len(accesses) > 0:
            return setErrorResponse(400, "The dataset can't be removed because it is currently accessed by: " + json.dumps(accesses))

        # Now let's remove the dataset from all sites
        LOG.debug("Removing dataset ACL in the database...")
        db.clearDatasetACL(datasetId)
        LOG.debug("Removing dataset accesses in the database...")
        for ac in accesses: 
            db.deleteDatasetAccess(ac["datasetAccessId"])
        LOG.debug("Removing dataset in the database...")
        db.deleteDataset(datasetId)
        LOG.debug("Removing studies not included in other datasets...")
        db.deleteOrphanStudies()

        if CONFIG.self.datasets_mount_path != '':
            LOG.debug("Removing dataset directory...")
            datasetDirName = datasetId
            dataset_file_system.remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)

    LOG.debug('Dataset successfully removed.')
    bottle.response.status = 204


@app.route('/api/licenses', method='GET')
def getLicenses():
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    # user = authorization.User(ret)

    with DB(CONFIG.db) as db:
        licenses = db.getLicenses()

    bottle.response.content_type = "application/json"
    return json.dumps(licenses)


@app.route('/api/projects', method='GET')
def getProjects():
    if CONFIG is None or not isinstance(bottle.request.query, bottle.FormsDict) or AUTH_ADMIN_CLIENT is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    if 'forNewDataset' in bottle.request.query and bottle.request.query['forNewDataset'].lower() == 'true':
        # New datasets only can be assigned to projects which the user has joined to
        projects = list(user.getProjects())
    else:
        # List all the possible values for "project" filter in GET /datasets
        searchFilter = authorization.Search_filter()
        searchFilter.adjustByUser(user)
        with DB(CONFIG.db) as db:
            projects = db.getProjects(searchFilter)

    bottle.response.content_type = "application/json"
    return json.dumps(projects)


@app.route('/api/user/<userName>', method='POST')
def postUser(userName):
    # Transitional condition while clients change to new PUT operation
    LOG.debug("Received (Transitional) POST %s" % bottle.request.path)
    return putUser(userName)

@app.route('/api/users/<userName>', method='PUT')
def putUser(userName):
    if CONFIG is None or AUTH_ADMIN_CLIENT is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")
    read_data = None
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        userData = json.loads(read_data)
        userId = userData["uid"] if "uid" in userData.keys() else None
        userGroups = userData["groups"]
        userGid = int(userData["gid"]) if "gid" in userData.keys() else None
        if userId is None:
            userId = AUTH_ADMIN_CLIENT.getUserId(userName)
            if userId is None: raise Exception("username '%s' not found in auth service" % userName)

        with DB(CONFIG.db) as db:
            LOG.debug("Creating or updating user: %s, %s, %s, %s" % (userId, userName, userGid, userGroups))
            db.createOrUpdateUser(userId, userName, userGroups, userGid)
                        
        LOG.debug('User successfully created or updated.')
        bottle.response.status = 201

    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/users/<userName>', method='GET')
def getUser(userName):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401,"unauthorized user")
    
    with DB(CONFIG.db) as db:
        userId, userGid = db.getUserIDs(userName)

    if userGid is None: return setErrorResponse(404, "user not found")

    bottle.response.content_type = "application/json"
    return json.dumps(dict(gid = userGid))


def checkDatasetListAccess(datasetIDs: list, userName: str):
    if CONFIG is None: raise Exception()
    badIDs = []
    with DB(CONFIG.db) as db:
        userId, userGID = db.getUserIDs(userName)
        if userId is None: return datasetIDs.copy()  # all datasetIDs are bad
        ret = getTokenOfAUserFromAuthAdminClient(userId)
        if isinstance(ret, str): return ret  # return error message
        user = authorization.User(ret)
        #userGroups = db.getUserGroups(userName)
        #userGroups = AUTH_ADMIN_CLIENT.getUserGroups(userId)
        for id in datasetIDs:
            dataset = db.getDataset(id)
            if dataset is None:
                # invalidated or not exists
                badIDs.append(id); continue
            # if not authorization.tmpUserCanAccessDataset(userId, userGroups, dataset):
            if dataset["draft"]:
                dataset["creating"] = (db.getDatasetCreationStatus(id) != None)
            datasetACL = db.getDatasetACL(id)
            if not user.canUseDataset(dataset, datasetACL):
                badIDs.append(id); continue
    if user._token != None: LOG.debug("###### User in groups: %s" % json.dumps(user._token["groups"]))
    if len(badIDs) == 0 and user.isOpenChallenge(): 
        return 'openchallenge'
    return badIDs

@app.route('/api/datasetAccessCheck', method='POST')
def postDatasetAccessCheck():
    if not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")
    read_data = None
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        datasetAccess = json.loads(read_data)
        userName = datasetAccess["userName"]
        datasetIDs = datasetAccess["datasets"]

        badIds = checkDatasetListAccess(datasetIDs, userName)
        if badIds == 'openchallenge':  # special response (actually not badId)
            LOG.debug('Openchallenge dataset access granted. Return: %s' % json.dumps(['openchallenge']))
            bottle.response.status = 200
            bottle.response.content_type = "application/json"
            return json.dumps(['openchallenge'])

        if len(badIds) > 0:
            bottle.response.status = 403
            bottle.response.content_type = "application/json"
            return json.dumps(badIds)
                
        LOG.debug('Dataset access granted.')
        bottle.response.status = 204

    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/datasetAccess/<id>', method='POST')
def postDatasetAccess(id):
    if CONFIG is None or AUTH_CLIENT is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    read_data = None
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        datasetAccess = json.loads(read_data)
        userName = datasetAccess["userName"]
        datasetIDs = datasetAccess["datasets"]
        toolName = datasetAccess["toolName"]
        toolVersion = datasetAccess["toolVersion"]
        image = datasetAccess["image"]
        commandLine = datasetAccess["commandLine"]
        isJob = datasetAccess["isJob"]
        resourcesFlavor = datasetAccess["resourcesFlavor"]
        openchallengeJobType = datasetAccess["openchallengeJobType"]
        datasetAccessId = id

        toolName = toolName[:256]
        toolVersion = toolVersion[:256]
        image = image[:256]
        commandLine = commandLine[:512]  # cut to max length
        accessType = 'b' if isJob else 'i'   # 'i' (interactive desktop or web app), 
                                             # 'b' (batch job)

        badIds = checkDatasetListAccess(datasetIDs, userName)
        if badIds == 'openchallenge':  # special response (actually not badId)
            badIds = []
            datasetIDs = ['0c709381-69fd-44fd-8417-26e1a181c1a9','a092a929-ef5b-41f2-93ee-9231c56beeb6','376e4aad-4813-47c7-a73a-f9304b8984b8',
                          'c3d944ee-45ef-47a5-8968-8ab0673646bf','fbf77161-ed71-4983-a49f-1a22c717f32f']
            LOG.debug('Openchallenge dataset access granted.')
            
        if len(badIds) > 0:
            return setErrorResponse(403,"access denied")

        with DB(CONFIG.db) as db:
            userId, userGID = db.getUserIDs(userName)
            if CONFIG.self.datasets_mount_path != '' and not isJob:
                # For jobs is not required to set the ACLs because they are already set for desktop and it's a high-cost operation
                for id in datasetIDs:
                    pathsOfStudies = db.getPathsOfStudiesFromDataset(id)
                    LOG.debug('Setting ACLs in dataset %s for GID %s ...' % (id, userGID))
                    datasetDirName = id
                    dataset_file_system.give_access_to_dataset(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.datalake_mount_path, pathsOfStudies, userGID)

            db.createDatasetAccess(datasetAccessId, datasetIDs, userGID, accessType, toolName, toolVersion, image, commandLine, 
                                   datetime.now(), resourcesFlavor, openchallengeJobType)
            LOG.debug("############### %s # %s # %s # %s # %s" % (datasetAccessId, datasetIDs, userGID, image, commandLine))

            if CONFIG.tracer.url != '' and not isJob:
                # The accesses by jobs are stored in db to mantain the access even if the desktop is deleted and to have history of images and command lines
                # but they are not sent to Tracer because the access by desktop is already traced.
                # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
                tracer.traceDatasetsAccess(AUTH_CLIENT, CONFIG.tracer.url, datasetIDs, userId, toolName, toolVersion)
        
        LOG.debug('Dataset access granted.')
        bottle.response.status = 201

    except LoginException as e:
        return setErrorResponse(500, "Unexpected, error: "+ str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/datasetAccess/<id>', method='PATCH')
def endDatasetAccess(id):
    if CONFIG is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401,"unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    read_data = None
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        DatasetAccessEndData = json.loads(read_data)
        status = DatasetAccessEndData["status"]
        startTime = DatasetAccessEndData["startTime"]
        startTime = datetime.fromisoformat(startTime.removesuffix('Z')) if startTime != '' else None
        endTime = DatasetAccessEndData["endTime"]
        endTime = datetime.fromisoformat(endTime.removesuffix('Z')) if endTime != '' else None
        datasetAccessId = id
        with DB(CONFIG.db) as db:
            if not db.existDatasetAccess(datasetAccessId):
                return setErrorResponse(404, "not found")    

            if CONFIG.self.datasets_mount_path != '':
                userGID, datasetIDsCandidatesForRemovePermission = db.getDatasetAccess(datasetAccessId)
                db.endDatasetAccess(datasetAccessId, startTime, endTime, status)
                # collect all the datasets still accessed after the end of this datasetAccess
                datasetIDsStillAccessedAfter = db.getDatasetsCurrentlyAccessedByUser(userGID)
                # collect all the studies still accessed after the end of this datasetAccess
                pathsOfstudiesStillAfter = set()
                for id in datasetIDsStillAccessedAfter:
                    pathsOfstudiesStillAfter.update(db.getPathsOfStudiesFromDataset(id))

                for id in datasetIDsCandidatesForRemovePermission:
                    if id in datasetIDsStillAccessedAfter: continue  # this dataset is still accessed, skip without remove permissions
                    datasetDirName = id
                    LOG.debug('Removing ACLs for GID %s in dataset directory %s not accessed anymore by this user...' % (userGID, datasetDirName))
                    dataset_file_system.remove_access_to_dataset(CONFIG.self.datasets_mount_path, datasetDirName, userGID)
                    pathsOfStudies = set(db.getPathsOfStudiesFromDataset(id))
                    pathsOfStudies.difference_update(pathsOfstudiesStillAfter)  # take out the studies still accessed to avoid remove permissions on them
                    LOG.debug('Removing ACLs for GID %s in %d studies no accessed anymore by this user...' % (userGID, len(pathsOfStudies)))
                    dataset_file_system.remove_access_to_studies(CONFIG.self.datalake_mount_path, pathsOfStudies, userGID)

        LOG.debug('Dataset access successfully ended.')
        bottle.response.status = 204
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/datasets/<id>/accessHistory', method='GET')
def getDatasetAccessHistory(id):
    if CONFIG is None or not isinstance(bottle.request.query, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401,"unauthorized user")

    datasetId = id
    skip = int(bottle.request.query['skip']) if 'skip' in bottle.request.query else 0
    limit = int(bottle.request.query['limit']) if 'limit' in bottle.request.query else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0

    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404,"not found")
        if dataset["draft"]:
            dataset["creating"] = (db.getDatasetCreationStatus(datasetId) != None)
        if not user.canViewDatasetDetails(dataset):
            return setErrorResponse(401,"unauthorized user")

        accesses, total = db.getDatasetAccesses(datasetId, limit, skip)    
    bottle.response.content_type = "application/json"
    return json.dumps({ "total": total,
                        "returned": len(accesses),
                        "skipped": skip,
                        "limit": limit,
                        "list": accesses })

@app.route('/api-doc', method='GET')
def getStaticApiDoc():
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    return bottle.static_file('index.html', CONFIG.self.static_api_doc_dir_path)

# Routes are evaluated in the order they were defined.
# So this is to send appropiate error to unknown operations but with the /api prefix.
# Just to not continue evaluating the rest of routes of front-end.
@app.route('/api/<any_path:re:.+>', method='GET')
def getUnknown(any_path):
    LOG.debug("Received GET unknown path: %s" % bottle.request.path)
    return setErrorResponse(404, "Not found '%s'" % bottle.request.path)


# ================
# Front-end routes
# ================

# static files (any route that ends with '.' + known extension, including subpaths)
# To avoid conflicts, static files prefixed with /web/
@app.route('/web/<file_path:re:.*\.(html|js|json|txt|map|css|jpg|png|gif|ico|svg|pdf)>', method='GET')
def getStaticFileWeb(file_path):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    LOG.debug("Static file (web): "+file_path)
    return bottle.static_file(file_path, CONFIG.self.static_files_dir_path)

# Routes are evaluated in the order they were defined.
# So this is to send appropiate error to missing file but with the /web prefix.
# Just to not continue evaluating the rest of routes.
@app.route('/web/<any_path:re:.+>', method='GET')
def getUnknownWeb(any_path):
    LOG.debug("Received GET unknown path: %s" % bottle.request.path)
    return setErrorResponse(404, "Not found '%s'" % bottle.request.path)

# # temporal for backward compatibility
# @app.route('/<file_path:re:.*\.(html|js|json|txt|map|css|jpg|png|gif|ico|svg)>', method='GET')
# def getStaticFile(file_path):
#     LOG.debug("Received GET %s" % bottle.request.path)
#     LOG.debug("Static file: "+file_path)
#     return bottle.static_file(file_path, CONFIG.self.static_files_dir_path)

# Any other path (without prefix /api/ or /web/) will be responded with the index.html content,
# index.html loads a javascript interface that manage those other paths.
@app.route('/', method='GET')
@app.route('/<any_path:re:.+>', method='GET')
def getWebUI(any_path=''):
    if CONFIG is None: raise Exception()
    LOG.debug("Received GET /" + any_path)
    LOG.debug("Routed to index.html")
    return bottle.static_file('index.html', CONFIG.self.static_files_dir_path)

