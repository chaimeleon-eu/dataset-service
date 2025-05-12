#! /usr/bin/env python3

import os
import io
import shutil
from enum import Enum
from datetime import datetime
from pathlib import Path
from typing import Iterable
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
from . import authorization, k8s, pid, tracer, keycloak, config, hash
from .auth import AuthClient, LoginException
from .storage import DB, DBDatasetsOperator, DBProjectsOperator, DBDatasetAccessesOperator
from .storage import DBDatasetsEUCAIMSearcher, SearchValidationException 
from . import dataset as dataset_file_system
from . import utils

#LOG = logging.getLogger('module1')
LOG = logging.root

class MyBottle(bottle.Bottle):
    # This method overwrite is just to translate the body of error responses to JSON format
    def default_error_handler(self, res):
        bottle.response.content_type = 'application/json'
        return json.dumps(dict(error = res.body, status_code = res.status_code))
    # Below method overwrite is not required in fact (all should work if we delete it).
    # It is a simplified version of the original method (https://github.com/bottlepy/bottle/blob/release-0.12/bottle.py#L788)
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
    authorization.User.client_id = CONFIG.auth.token_validation.client_id
    authorization.User.PROJECT_GROUP_PREFIX = CONFIG.auth.token_validation.project_group_prefix
    authorization.User.PROJECT_ADMINS_GROUP_PREFIX = CONFIG.auth.token_validation.project_admins_group_prefix
    AUTH_CLIENT = AuthClient(CONFIG.auth.client.auth_url, CONFIG.auth.client.client_id, CONFIG.auth.client.client_secret)
    AUTH_ADMIN_CLIENT = keycloak.KeycloakAdminAPIClient(AUTH_CLIENT, CONFIG.auth.admin_api.url, CONFIG.auth.admin_api.client_id_to_request_user_tokens)

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
                              issuer=CONFIG.auth.token_validation.issuer, audience=CONFIG.auth.token_validation.client_id, 
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
        return setErrorResponse(401, "invalid access token: missing '%s'" % missingProperty)
    return token

def getTokenOfAUserFromAuthAdminClient(userId) -> str | dict:
    ''' Returns: 
        - str with the error message in case of fail
        - dict with the token if success
    '''
    if AUTH_ADMIN_CLIENT is None: raise Exception()
    token = AUTH_ADMIN_CLIENT.getUserToken(userId)
    ok, missingProperty = authorization.User.validateToken(token, False)
    if not ok: return setErrorResponse(401, "invalid access token: missing '%s'" % missingProperty)
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
    if CONFIG is None \
       or not isinstance(bottle.request.query, bottle.FormsDict) \
       or not isinstance(bottle.request.files, bottle.FormsDict) \
       or not isinstance(bottle.request.body, io.IOBase): 
        raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    if CONFIG.self.dev_token == "":
        return setErrorResponse(404, "Not found: '%s'" % bottle.request.path)
    if bottle.request.get_header("devToken") != CONFIG.self.dev_token:
        return setErrorResponse(401, "unauthorized user")
    destinationZipPath = CONFIG.self.static_files_dir_path + "/build.zip"
    if "method" in bottle.request.query and bottle.request.query["method"] == "fileInBody":
        LOG.debug("Method: fileInBody.")
        zipFile = bottle.request.files["zip"]
        name, ext = os.path.splitext(zipFile.filename)
        if ext != '.zip':
            return setErrorResponse(400, 'File extension not allowed, only zip is supported.')
        if os.path.exists(destinationZipPath): os.remove(destinationZipPath)
        zipFile.save(destinationZipPath)
    else:
        sourceUrl = bottle.request.body.read().decode('UTF-8')
        LOG.debug("URL in body: " + sourceUrl )
        utils.download_url(sourceUrl, destinationZipPath)

    # As the new package usually contains different file names in "static" dir, we have to remove the previous one, 
    # otherwise the files accumulate taking up disk space.
    shutil.rmtree(os.path.join(CONFIG.self.static_files_dir_path, "static"), ignore_errors=True)
    output, status = utils.execute_cmd("unzip -uo \"" + destinationZipPath + "\" -d \"" + CONFIG.self.static_files_dir_path + "/\"")
    final_msg = "UI package successfully updated." if status == 0 else "Error updating UI package."
    LOG.debug(final_msg)
    if not isinstance(output, list): raise Exception("Unexpected type for output variable.")
    output.append(final_msg)
    return output

@app.route('/datalakeinfo/<file_path:re:.*\.(json)>', method='GET')
def getDatalakeInfo(file_path):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    LOG.debug("Static file: "+file_path)
    if CONFIG.self.datalakeinfo_token == "":
        return setErrorResponse(404, "Not found: '%s'" % bottle.request.path)
    if bottle.request.get_header("secret") != CONFIG.self.datalakeinfo_token:
        return setErrorResponse(401, "unauthorized user")
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

def _checkPropertyAsString(propName:str, value: str, possible_values: list[str] | None = None, min_length: int = 0, max_length: int = 0, 
                           only_alphanum_or_dash: bool = False):
    if not isinstance(value, str): 
        raise WrongInputException("'%s' must be a string." % propName)
    if possible_values != None and not value in possible_values:
        raise WrongInputException("Unknown value '%s' in '%s', it should be one of {%s}" % (value, propName, ', '.join(possible_values)))
    if min_length > 0 and len(value) < min_length:
        raise WrongInputException("Length of '%s' must be %s characters or more." % (propName, min_length))
    if max_length > 0 and len(value) > max_length:
        raise WrongInputException("Max length of '%s' is %s characters." % (propName, max_length))
    if only_alphanum_or_dash and not value.replace('-','a').isalnum(): 
        raise WrongInputException("Invalid value for '%s', only alphanumeric characters and '-' are allowed." % (propName))

def _checkPropertyAsArrayOfStrings(propName: str, value: list[str], item_possible_values: Iterable[str] | None = None, item_max_length: int = 0,
                                   item_only_alphanum_or_dash: bool = False):
    if not isinstance(value, list):
        raise WrongInputException("'%s' must be an array of strings." % propName)
    for item in value:
        if not isinstance(item, str): 
            raise WrongInputException("'%s' must be an array of strings." % propName)
        if item_possible_values != None and not item in item_possible_values:
            raise WrongInputException("Unknown item value '%s' in '%s', it should be one of {%s}" \
                                        % (item, propName, ', '.join(item_possible_values)))
        if item_max_length > 0 and len(item) > item_max_length:
            raise WrongInputException("The item value '%s' in '%s' exceeds the max length of %s characters." \
                                        % (item, propName, item_max_length))
        if item_only_alphanum_or_dash and not item.replace('-','a').isalnum(): 
            raise WrongInputException("Invalid item value '%s' in '%s', only alphanumeric characters and '-' are allowed." % (item, propName))

def _checkPropertyAsObject(propName: str, value: dict):
    if not isinstance(value, dict): raise WrongInputException("'%s' must be an object." % propName)

def _checkPropertyAsLicense(newValue):
    _checkPropertyAsObject("license", newValue)
    if not "title" in newValue: raise WrongInputException("Missing 'title' in the new license.")
    if not "url" in newValue: raise WrongInputException("Missing 'url' in the new license.")
    if not isinstance(newValue["title"], str): raise WrongInputException("The title of license must be a string.")
    if not isinstance(newValue["url"], str): raise WrongInputException("The url of license must be a string.")

ITEM_POSSIBLE_VALUES_FOR_TYPE = ['original', 'annotated', 'processed', 'personal-data']
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
        return setErrorResponse(401, "unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if "external" in bottle.request.query and bottle.request.query["external"].lower() == "true":
        if not 'multipart/form-data' in content_types:
            return setErrorResponse(400, "invalid 'Content-Type' header, required 'multipart/form-data'")
    else:
        if not 'application/json' in content_types:
            return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")

    datasetDirName = ''
    datasetId = str(uuid.uuid4())
    read_data = None
    try:
        if "external" in bottle.request.query and bottle.request.query["external"].lower() == "true":
            # This is for manually create datasets out of the standard ingestion procedure 
            clinicalDataFile = bottle.request.files["clinical_data"]
            name, ext = os.path.splitext(clinicalDataFile.filename)
            if ext != '.csv':
                return setErrorResponse(400, 'File extension not allowed, only CSV is supported.')
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
            LOG.debug("Reading request body as JSON...")
            read_data = bottle.request.body.read().decode('UTF-8')
            #LOG.debug("BODY: " + read_data)
            dataset = json.loads( read_data )
            if not isinstance(dataset, dict): raise WrongInputException("The body must be a json object.")
            if not 'version' in dataset.keys(): dataset["version"] = '??'
            if not 'provenance' in dataset.keys(): dataset["provenance"] = '??'
            if not 'purpose' in dataset.keys(): dataset["purpose"] = '??'
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
            
            # Integrity checks
            subjects = set()
            study_paths = set()
            LOG.debug("Checking for duplicated subjects...")
            for subject in dataset["subjects"]:
                if subject["subjectName"] in subjects:
                    raise WrongInputException("The subjectName '%s' is duplicated in 'subjects' array of the dataset." % subject["subjectName"])
                else:
                    subjects.add(subject["subjectName"])
            LOG.debug("Checking for studies with missing subjects or conflicting paths...")
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
            

        with DB(CONFIG.db) as db:
            dbdatasets = DBDatasetsOperator(db)
            LOG.debug("Updating author: %s, %s, %s, %s" % (user.uid, user.username, user.name, user.email))
            dbdatasets.createOrUpdateAuthor(user.uid, user.username, user.name, user.email)

            user_projects = _getProjects(user)
            if 'project' in dataset.keys(): 
                if not dataset["project"] in user_projects:
                    return setErrorResponse(400, "dataset.project does not exist for the user")
            else:
                dataset["project"] = user_projects.pop()  # we take the first project of user
            
            if 'previousId' in dataset.keys():
                previousDataset = dbdatasets.getDataset(dataset["previousId"])
                if previousDataset is None:
                    return setErrorResponse(400, "dataset.previousId does not exist")
                if not user.canModifyDataset(previousDataset):
                    return setErrorResponse(401, "the dataset selected as previous (%s) "
                                               + "must be editable by the user (%s)" % (previousDataset["id"], user.username))
            else:
                dataset["previousId"] = None

            LOG.debug("UUID generated: " + datasetId)
            dataset["id"] = datasetId
            dataset["creationDate"] = datetime.now()
            #if not "public" in dataset.keys(): 
            dataset["public"] = False

            dataset["studiesCount"] = 0   # Let's set now to zero because the real number is obtained later,
            dataset["subjectsCount"] = 0  # after some checks like missing series.
            # The rest of Metada will be collected later, now let's leave it empty (default values in DB)
            
            LOG.debug('Creating dataset in DB...')
            dbdatasets.createDataset(dataset, user.uid)
            projectConfig = DBProjectsOperator(db).getProjectConfig(dataset["project"])
            if projectConfig is None: raise Exception("Unexpected error: the project assigned to dataset does not exist.")
            dbdatasets.setDatasetContactInfo(datasetId, projectConfig["defaultContactInfo"])
            dbdatasets.setDatasetLicense(datasetId, projectConfig["defaultLicense"]["title"], projectConfig["defaultLicense"]["url"])

            LOG.debug('Creating dataset directory...')
            datasetDirName = datasetId
            dataset_file_system.create_dataset_dir(CONFIG.self.datasets_mount_path, datasetDirName)
            datasetDirPath = os.path.join(CONFIG.self.datasets_mount_path, datasetDirName)

            LOG.debug('Writing E-FORMs file: ' + CONFIG.self.eforms_file_name)
            with open(os.path.join(datasetDirPath, CONFIG.self.eforms_file_name) , 'w') as outputStream:
                json.dump(dataset["subjects"], outputStream)

            # The index file will be written later when the final list of studies is obtained.

            LOG.debug('Writing studies temporal list: ' + CONFIG.self.studies_tmp_file_name)
            with open(os.path.join(datasetDirPath, CONFIG.self.studies_tmp_file_name) , 'w') as outputStream:
                json.dump(dataset["studies"], outputStream)

            LOG.debug('Creating status in DB...')
            dbdatasets.createDatasetCreationStatus(datasetId, "pending", "Launching dataset creation job...")
            LOG.debug('Launching dataset creation job...')
            k8sClient = k8s.K8sClient()
            ok = k8sClient.add_dataset_creation_job(datasetId)
            if not ok: 
                dbdatasets.setDatasetCreationStatus(datasetId, "error", "Unexpected error launching dataset creation job.")
                raise K8sException("Unexpected error launching dataset creation job.")

        LOG.debug('Dataset successfully created in DB and creation job launched in K8s.')
        bottle.response.status = 201
        bottle.response.content_type = "application/json"
        return json.dumps(dict(apiUrl = "/api/datasets/" + datasetId,
                               url = CONFIG.self.dataset_link_format % datasetId))

    except (WrongInputException, K8sException) as e:
        if datasetDirName != '': dataset_file_system.remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
        return setErrorResponse(400, str(e))
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
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
        return setErrorResponse(401, "unauthorized user")

    datasetId = id
    with DB(CONFIG.db) as db:
        dbdatasets = DBDatasetsOperator(db)
        if not dbdatasets.existsDataset(datasetId):
            return setErrorResponse(404, "not found")
        datasetStudies, total = dbdatasets.getStudiesFromDataset(datasetId)
        dataset_file_system.adjust_file_permissions_in_datalake(CONFIG.self.datalake_mount_path, datasetStudies)

        # # After adjust the file permissions with chmod 700, the ACL in studies dirs is still there but it has not effect, so we have to readjust them also
        # pathsOfStudies = db.getPathsOfStudiesFromDataset(id)
        # datasetAccesses = db.getOpenDatasetAccesses(datasetId)
        # usernames = set()
        # for datasetAccess in datasetAccesses:
        #     username = datasetAccess["username"]
        #     if username in usernames: continue
        #     userId, userGID = db.getUserIDs(datasetAccess["username"])
        #     LOG.debug('Setting ACLs in dataset %s for GID %s ...' % (id, userGID))
        #     datasetDirName = id
        #     dataset_file_system.give_access_to_dataset(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.datalake_mount_path, pathsOfStudies, userGID)
    
    LOG.debug('Dataset files permissions successfully adjusted.')
    bottle.response.status = 204

def _recollectMetadataForDataset(datasetId):
    if CONFIG is None: raise Exception()
    with DB(CONFIG.db) as db:
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: 
            return dict(success=False, msg="Not recollected: dataset removed.")
        if dataset["draft"] and dbdatasets.getDatasetCreationStatus(datasetId) != None:
            return dict(success=False, msg="Not recollected: it is still being created.")
        datasetStudies, total = dbdatasets.getStudiesFromDataset(datasetId)
        dataset["studies"] = datasetStudies
        eformsFilePath = os.path.join(CONFIG.self.datasets_mount_path, datasetId, CONFIG.self.eforms_file_name)
        try:
            dataset_file_system.collectMetadata(dataset, CONFIG.self.datalake_mount_path, eformsFilePath)
        except Exception as e:
            LOG.exception(e)
            return dict(success=False, msg="Not recollected: exception catched (corrupt dataset?).")
        dbdatasets.updateDatasetAndStudyMetadata(dataset)
    return dict(success=True, msg="Successfully recollected.")

@app.route('/api/datasets/<id>/recollectMetadata', method='POST')
def recollectMetadataForDataset(id):
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
        return setErrorResponse(401, "unauthorized user")

    datasetId = id
    with DB(CONFIG.db) as db:
        if not DBDatasetsOperator(db).existsDataset(datasetId):
            return setErrorResponse(404, "not found")
    LOG.debug('Collecting metadata for dataset %s' % datasetId)
    result = _recollectMetadataForDataset(datasetId)
    LOG.debug('Result: %s' % json.dumps(result))
    bottle.response.status = 200
    bottle.response.content_type = "application/json"
    return json.dumps(result)

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
        return setErrorResponse(401, "unauthorized user")

    with DB(CONFIG.db) as db:
        searchFilter = authorization.Search_filter()
        searchFilter.adjustByUser(user)
        datasets, total =  DBDatasetsOperator(db).getDatasets(0, 0, '', searchFilter, '', '')
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
        return setErrorResponse(401, "unauthorized user")
    datasetId = id
    with DB(CONFIG.db) as db:
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
        if not user.canRelaunchDatasetCreation(dataset):
            return setErrorResponse(401, "unauthorized user")
        k8sClient = k8s.K8sClient()
        job = k8sClient.exist_dataset_creation_job(datasetId)
        if job:
            if k8sClient.is_running_job(job):
                return setErrorResponse(400, "there is a creation job running for this dataset, please stop or delete it before relaunch")
            else: k8sClient.delete_dataset_creation_job(datasetId)
        LOG.debug('Updating status in DB...')
        dbdatasets.setDatasetCreationStatus(datasetId, "pending", "Relaunching dataset creation job...")
        LOG.debug('Relaunching dataset creation job in k8s...')
        ok = k8sClient.add_dataset_creation_job(datasetId)
        if not ok: 
            dbdatasets.setDatasetCreationStatus(datasetId, "error", "Unexpected error launching dataset creation job.")
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
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404, "not found")

        # check access permission
        if not user.canViewDatasetDetails(dataset):
            return setErrorResponse(401, "unauthorized user")

        status = dbdatasets.getDatasetCreationStatus(datasetId)
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
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: 
            return dict(success=False, msg="Not checked: dataset removed.")
        if dataset["draft"] and dbdatasets.getDatasetCreationStatus(datasetId) != None:
            return dict(success=False, msg="Not checked: it is still being created.")
        lastCheck = None if dataset["lastIntegrityCheck"] is None \
                        else datetime.fromisoformat(dataset["lastIntegrityCheck"]).replace(tzinfo=None)
        if lastCheck != None and (datetime.now() - lastCheck).days < CONFIG.self.dataset_integrity_check_life_days:
            if dataset["corrupted"]:
                ok, integrityStr = False, "wrong" 
            else: ok, integrityStr = True, "OK" 
            return dict(success=ok,  msg="Integrity %s (checked on %s)" % (integrityStr, lastCheck))
        
        studies, total = dbdatasets.getStudiesFromDataset(datasetId)
        for study in studies:
            studiesHashes[study["studyId"]] = study["hash"]
    
    hashesOperator = hash.datasetHashesOperator(CONFIG.db, CONFIG.self.series_hash_cache_life_days)
    wrongHash = tracer.checkDatasetIntegrity(AUTH_CLIENT, CONFIG.tracer.url, datasetId, datasetDirPath,
                                                CONFIG.self.index_file_name, CONFIG.self.eforms_file_name,
                                                studiesHashes, hashesOperator)
    corrupted = (wrongHash != None)
    with DB(CONFIG.db) as db:
        DBDatasetsOperator(db).setDatasetLastIntegrityCheck(datasetId, corrupted, datetime.now())
    if corrupted:
        return dict(success=False, msg="Resource hash mismatch: %s" % wrongHash)
    else: return dict(success=True, msg="Integrity OK.")

@app.route('/api/datasets/<id>/checkIntegrity', method='POST')
def checkDatasetIntegrity(id):
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canCheckIntegrityOfDatasets():
        return setErrorResponse(401, "unauthorized user")
    
    datasetId = id
    result = _checkDatasetIntegrity(datasetId)
    bottle.response.status = 200
    bottle.response.content_type = "application/json"
    return json.dumps(result)

@app.route('/api/datasets/checkIntegrity', method='POST')
def checkAllDatasetsIntegrity():
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canCheckIntegrityOfDatasets():
        return setErrorResponse(401, "unauthorized user")

    with DB(CONFIG.db) as db:
        searchFilter = authorization.Search_filter()
        searchFilter.adjustByUser(user)
        datasets, total =  DBDatasetsOperator(db).getDatasets(0, 0, '', searchFilter, '', '')
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
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
        datasetACL = dbdatasets.getDatasetACL(id)
    if not user.canViewDatasetDetails(dataset):
        return setErrorResponse(401, "unauthorized user")
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
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
        if not user.canViewDatasetDetails(dataset):
            return setErrorResponse(401, "unauthorized user")
        studies, total = dbdatasets.getStudiesFromDataset(datasetId, limit, skip)
    
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

def createZenodoDeposition(db: DB, dataset):
    if CONFIG is None: raise Exception()
    if dataset["pids"]["urls"]["zenodoDoi"] is None: 
        datasetId = dataset["id"]
        dbdatasets = DBDatasetsOperator(db)
        studies, total = dbdatasets.getStudiesFromDataset(datasetId)
        projectConfig = DBProjectsOperator(db).getProjectConfig(dataset["project"])
        if projectConfig is None: raise Exception()
        author = projectConfig["zenodoAuthor"] if projectConfig["zenodoAuthor"] != '' else dataset["authorName"]
        newValue = pid.getZenodoDOI(CONFIG.zenodo.url, projectConfig["zenodoAccessToken"], dataset, studies, author,
                                    CONFIG.self.dataset_link_format, projectConfig["zenodoCommunity"], projectConfig["zenodoGrant"])
        dbdatasets.setZenodoDOI(datasetId, newValue)

def updateZenodoDeposition(db, dataset):
    if CONFIG is None: raise Exception()
    pidUrl = dataset["pids"]["urls"]["zenodoDoi"]
    if pidUrl != None: 
        i = pidUrl.rfind('.') + 1
        depositionId = pidUrl[i:]
        projectConfig = DBProjectsOperator(db).getProjectConfig(dataset["project"])
        if projectConfig is None: raise Exception()
        author = projectConfig["zenodoAuthor"] if projectConfig["zenodoAuthor"] != '' else dataset["authorName"]
        pid.updateZenodoDeposition(CONFIG.zenodo.url, projectConfig["zenodoAccessToken"], dataset, author,
                                   CONFIG.self.dataset_link_format, projectConfig["zenodoCommunity"], projectConfig["zenodoGrant"], 
                                   depositionId)

@app.route('/api/datasets/<id>', method='PATCH')
def patchDataset(id):
    if CONFIG is None or AUTH_CLIENT is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if user.isUnregistered():
        return setErrorResponse(401, "unauthorized user")
    try:
        datasetId = id
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        patch = json.loads( read_data )
        property = patch["property"]
        newValue = patch["value"]
        trace_details = None

        with DB(CONFIG.db) as db:
            dbdatasets = DBDatasetsOperator(db)
            dataset = dbdatasets.getDataset(datasetId)
            if dataset is None:
                return setErrorResponse(404, "not found")
            if dataset["draft"]:
                dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
            if property not in user.getEditablePropertiesByTheUser(dataset):
                return setErrorResponse(400, "the property is not in the editablePropertiesByTheUser list")

            if property == "draft":
                if not isinstance(newValue, bool): raise WrongInputException("The value must be boolean.")
                if newValue == False and dataset["previousId"] != None:
                    # Let's set in the previous dataset the reference to this one.
                    # This way, the UI can show a notice in the previous dataset that there is a new version, with a link to this one.
                    previousDataset = dbdatasets.getDataset(dataset["previousId"])
                    if previousDataset is None: raise Exception()
                    #  Check the previousId is not in other datasets.
                    #  It has to be checked here because when released this dataset, the previous will disappear from the upgradable datasets list,
                    #  so the other datasets sharing the previousId will be in a wrong state, they will have a previousId not selectable.
                    newVersionsOfTheSameDataset = dbdatasets.getDatasetsSharingPreviousId(dataset["previousId"])
                    if len(newVersionsOfTheSameDataset) > 1:
                        return setErrorResponse(400, "There are more than one draft datasets selected as the next version for the same dataset: "
                                                +str(newVersionsOfTheSameDataset) + ". "
                                                +"Please delete or change the previousId in some of them (only one can be the next version) .")
                    # The next check can not happen because of the previous check, but it is kept just in case the previous is removed in the future.
                    if previousDataset["nextId"] != None:
                        return setErrorResponse(400, "The previousId (%s) is not valid, " % dataset["previousId"]
                                                + "it references to an old dataset which already has a new version (%s). " % previousDataset["nextId"]
                                                + "You may want to set the previousId to that new version (\"rebase\" your dataset), "
                                                + "or you can simply set to null (and put some link to the base dataset in the description).")
                    dbdatasets.setDatasetNextId(previousDataset["id"], datasetId)
                dbdatasets.setDatasetDraft(datasetId, newValue)
                if bool(newValue) == False:
                    trace_details = "RELEASE"
            elif property == "public":
                if not isinstance(newValue, bool): raise WrongInputException("The value must be boolean.")
                dbdatasets.setDatasetPublic(datasetId, newValue)
                dataset["public"] = newValue
                if newValue and dataset["pids"]["preferred"] is None:
                    # When publish, a PID will be autogenerated if it is still none 
                    createZenodoDeposition(db, dataset)
                    dbdatasets.setDatasetPid(datasetId, "zenodoDoi")
                else:
                    updateZenodoDeposition(db, dataset)
                trace_details = "PUBLISH" if newValue else "UNPUBLISH"
            elif property == "invalidated":
                if not isinstance(newValue, bool): raise WrongInputException("The value must be boolean.")
                dbdatasets.setDatasetInvalidated(datasetId, newValue)
                if newValue:
                    LOG.debug('Removing ACL entries in dataset %s ...' % (datasetId))
                    datasetDirName = datasetId
                    dataset_file_system.invalidate_dataset(CONFIG.self.datasets_mount_path, datasetDirName)              
                else:  # reactivated
                    dbdatasets.setDatasetInvalidationReason(datasetId, None)  # reset invalidation reason
                trace_details = "INVALIDATE" if newValue else "REACTIVATE"
            elif property == "invalidationReason":
                _checkPropertyAsString("value", newValue, max_length=128)
                dbdatasets.setDatasetInvalidationReason(datasetId, newValue)
                # Don't notify the tracer, it is just a note to remember the reason.
            elif property == "name":
                _checkPropertyAsString("value", newValue, max_length=256)
                dbdatasets.setDatasetName(datasetId, newValue)
                # Don't notify the tracer, this property can be changed only in draft state
            elif property == "version":
                _checkPropertyAsString("value", newValue, max_length=16)
                dbdatasets.setDatasetVersion(datasetId, newValue)
                # Don't notify the tracer, this property can be changed only in draft state
            elif property == "description":
                _checkPropertyAsString("value", newValue)
                dbdatasets.setDatasetDescription(datasetId, newValue)
                # Don't notify the tracer, this property can be changed only in draft state
            elif property == "tags":
                _checkPropertyAsArrayOfStrings("value", newValue, item_max_length=20, item_only_alphanum_or_dash=True)
                dbdatasets.setDatasetTags(datasetId, newValue)
                # Don't notify the tracer, this property is just for organizational purpose
            elif property == "provenance":
                _checkPropertyAsString("value", newValue)
                dbdatasets.setDatasetProvenance(datasetId, newValue)
                # Don't notify the tracer, this property can be changed only in draft state
            elif property == "purpose":
                _checkPropertyAsString("value", newValue)
                dbdatasets.setDatasetPurpose(datasetId, newValue)
                # Don't notify the tracer, this property can be changed only in draft state
            elif property == "type":
                _checkPropertyAsArrayOfStrings("value", newValue, ITEM_POSSIBLE_VALUES_FOR_TYPE)
                dbdatasets.setDatasetType(datasetId, newValue)
                # Don't notify the tracer, this property can be changed only in draft state
            elif property == "collectionMethod":
                _checkPropertyAsArrayOfStrings("value", newValue, ITEM_POSSIBLE_VALUES_FOR_COLLECTION_METHOD)
                dbdatasets.setDatasetCollectionMethod(datasetId, newValue)
                # Don't notify the tracer, this property can be changed only in draft state
            # elif property == "project":
            #     _checkPropertyAsString("value", newValue, max_length=16)
            #     if not newValue in user.getProjects():
            #         return setErrorResponse(400, "invalid value, unknown project code for the user")
            #     db.setDatasetProject(datasetId, newValue)
            #     # Don't notify the tracer, this property can be changed only in draft state
            elif property == "previousId":
                if newValue != None:
                    _checkPropertyAsString("value", newValue, max_length=40)
                    previousDataset = dbdatasets.getDataset(newValue)
                    if previousDataset is None:
                        raise WrongInputException("invalid value, the dataset id does not exist")
                    if not user.canModifyDataset(previousDataset):
                        return setErrorResponse(401, "the dataset selected as previous (%s) "
                                                +"must be editable by the user (%s)" % (previousDataset["id"], user.username))
                dbdatasets.setDatasetPreviousId(datasetId, newValue)  # newValue can be None or str
                # Don't notify the tracer, this property can be changed only in draft state
            elif property == "license":
                _checkPropertyAsLicense(newValue)
                dbdatasets.setDatasetLicense(datasetId, newValue["title"], newValue["url"])
                # dataset["license"] = dict(title=newTitle,url=newUrl)  license is written in a PDF file when uploaded to zenodo
                # updateZenodoDeposition(db, dataset)                   and deposition files cannot be changed once published
                trace_details = "LICENSE_UPDATED"
            elif property == "pids":
                if not isinstance(newValue, dict): raise WrongInputException("The value must be an object.")
                if not "preferred" in newValue: raise WrongInputException("Missing 'preferred' in the new pids.")
                if not isinstance(newValue["preferred"], str): raise WrongInputException("The preferred pid must be a string.")
                preferred = newValue["preferred"].strip()
                custom = None
                if preferred == "zenodoDoi":
                    createZenodoDeposition(db, dataset)
                elif preferred == "custom":
                    if not "urls" in newValue: raise WrongInputException("Missing 'urls' in the new pids.")
                    if not isinstance(newValue["urls"], dict): raise WrongInputException("The 'urls' of pids must be an object.")
                    if not "custom" in newValue["urls"]: raise WrongInputException("Missing 'custom' in the 'urls' of new pids.")
                    if not isinstance(newValue["urls"]["custom"], str): raise WrongInputException("The custom url of pids must be a string.")
                    custom = newValue["urls"]["custom"]
                    if not custom.startswith("http"):
                        raise WrongInputException("invalid value for urls.custom")
                else: raise WrongInputException("invalid value for preferred")
                dbdatasets.setDatasetPid(datasetId, preferred, custom)
                trace_details = "PID_UPDATED"
            elif property == "contactInfo":
                _checkPropertyAsString("value", newValue, max_length=256)
                dbdatasets.setDatasetContactInfo(datasetId, newValue)
                # dataset["contactInfo"] = newValue            contactInfo is written in a PDF file
                # updateZenodoDeposition(db, dataset)          and deposition files cannot be changed once published
                trace_details = "CONTACT_INFORMATION_UPDATED"
            elif property == "authorId":
                # This is a special operation only allowed for the role "superadmin_datasets",
                # (s)he can create a dataset for another user and then transfer the authorship to the user.
                _checkPropertyAsString("value", newValue, max_length=64)
                if not dbdatasets.existsUserID(newValue):
                    return setErrorResponse(400, "invalid value, the user id does not exist")
                dbdatasets.setDatasetAuthor(datasetId, newValue)
                # dataset = db.getDataset(datasetId)
                # updateZenodoDeposition(db, dataset)
                trace_details = "AUTHOR_CHANGED"
            else:
                return setErrorResponse(400, "invalid property")

            if CONFIG.tracer.url != '' and trace_details != None:
                LOG.debug('Notifying to tracer-service...')
                # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
                tracer.traceDatasetUpdate(AUTH_CLIENT, CONFIG.tracer.url, datasetId, user.uid, trace_details)
        LOG.debug('Dataset successfully updated.')
        bottle.response.status = 204
    except WrongInputException as e:
        return setErrorResponse(400, str(e))
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except pid.PidException as e:
        return setErrorResponse(500, str(e))

@app.route('/api/datasets/<id>/acl', method='GET')
def getDatasetACL(id):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    datasetId = id
    with DB(CONFIG.db) as db:
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
        if not user.canManageACL(dataset):
            return setErrorResponse(401, "unauthorized user")
        acl = dbdatasets.getDatasetACL_detailed(datasetId)
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
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404, "dataset not found")
        if dataset["draft"]:
            dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
        if not user.canManageACL(dataset): 
            return setErrorResponse(401, "unauthorized user")
        userId, userGid = dbdatasets.getUserIDs(username)
        if userId is None: return setErrorResponse(404, "user not found")
        if operation == Edit_operation.ADD:
            LOG.debug("Adding user '%s' to the ACL of dataset %s." % (username, datasetId))
            dbdatasets.addUserToDatasetACL(datasetId, userId)
            LOG.debug('User successfully added to the ACL.')
            bottle.response.status = 201
        elif operation == Edit_operation.DELETE:
            LOG.debug("Deleting user '%s' from the ACL of dataset %s." % (username, datasetId))
            dbdatasets.deleteUserFromDatasetACL(datasetId, userId)
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
    try:
        if 'draft' in bottle.request.query:
            searchFilter.draft = parse_flag_value(bottle.request.query['draft'])
        if 'public' in bottle.request.query:
            searchFilter.public = parse_flag_value(bottle.request.query['public'])
        if 'invalidated' in bottle.request.query:
            searchFilter.invalidated = parse_flag_value(bottle.request.query['invalidated'])
        if 'tags' in bottle.request.query:
            tags = bottle.request.query.getall('tags')
            _checkPropertyAsArrayOfStrings('tags', tags, item_only_alphanum_or_dash=True)
            searchFilter.tags = set(tags)
        if 'project' in bottle.request.query:
            project = bottle.request.query['project']
            _checkPropertyAsString('project', project, only_alphanum_or_dash=True)
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
        onlyLastVersions = ('onlyLastVersions' in bottle.request.query and bool(parse_flag_value(bottle.request.query['onlyLastVersions'])))
    except WrongInputException as e:
        return setErrorResponse(400, str(e))
    
    with DB(CONFIG.db) as db:
        datasets, total = DBDatasetsOperator(db).getDatasets(skip, limit, searchString, searchFilter, sortBy, sortDirection, searchSubject, onlyLastVersions)
    bottle.response.content_type = "application/json"
    return json.dumps({ "total": total,
                        "returned": len(datasets),
                        "skipped": skip,
                        "limit": limit,
                        "list": datasets,
                        "allowedActionsForTheUser": user.getAllowedActionsOnDatasetsForTheUser()})
    
@app.route('/api/datasets/eucaimSearch', method='POST')
def eucaimSearchDatasets():
    if CONFIG is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    if CONFIG.self.eucaim_search_token == "":
        return setErrorResponse(404, "Not found: '%s'" % bottle.request.path)
    if bottle.request.get_header("Authorization") != "Secret " + CONFIG.self.eucaim_search_token:
        return setErrorResponse(401, "unauthorized user")
    
    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")
    read_data = bottle.request.body.read().decode('UTF-8')
    LOG.debug("BODY: " + read_data)
    try:
        search_rq = json.loads(read_data)
        if not isinstance(search_rq, dict): raise WrongInputException("The body must be a json object.")
        if not 'ast' in search_rq: raise WrongInputException("Missing property 'ast' in the request object.")
        if not isinstance(search_rq['ast'], dict): raise WrongInputException("The value of property 'ast' must be a json object.")
        #parseAST()
        with DB(CONFIG.db) as db:
            result = DBDatasetsEUCAIMSearcher(db).eucaimSearchDatasets(
                search_rq['ast'], CONFIG.self.eucaim_search_filter_by_tag, 0, 0)
            
        LOG.debug('Result: '+json.dumps({'collections': result}))
        bottle.response.status = 200
        bottle.response.content_type = "application/json"
        return json.dumps({'collections': result})
    except (WrongInputException, SearchValidationException) as e:
        return setErrorResponse(422, "Request validation error: %s" % e)
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
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
        return setErrorResponse(401, "unauthorized user")

    searchFilter = authorization.Upgradables_filter()
    searchFilter.adjustByUser(user)
    with DB(CONFIG.db) as db:
        datasets = DBDatasetsOperator(db).getUpgradableDatasets(searchFilter)
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
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
        if not user.canDeleteDataset(dataset):
            return setErrorResponse(401, "unauthorized user")

        if "creating" in dataset and dataset["creating"]:
            #First of all stop the job
            LOG.debug('Deleting dataset creation job...')
            k8sClient = k8s.K8sClient()
            ok = k8sClient.delete_dataset_creation_job(datasetId)
            if not ok: return setErrorResponse(500, "Unexpected error")
        
        db_ds_accesses = DBDatasetAccessesOperator(db)
        accesses = db_ds_accesses.getOpenDatasetAccesses(datasetId)
        if len(accesses) > 0:
            return setErrorResponse(400, "The dataset can't be removed because it is currently accessed by: " + json.dumps(accesses))

        # Now let's remove the dataset from all sites
        LOG.debug("Removing dataset ACL in the database...")
        dbdatasets.clearDatasetACL(datasetId)
        LOG.debug("Removing dataset accesses in the database...")
        for ac in accesses: 
            db_ds_accesses.deleteDatasetAccess(ac["datasetAccessId"])
        LOG.debug("Removing dataset in the database...")
        dbdatasets.deleteDataset(datasetId)
        LOG.debug("Removing series not included in any dataset...")
        dbdatasets.deleteOrphanSeries()
        LOG.debug("Removing studies not included in any dataset...")
        dbdatasets.deleteOrphanStudies()

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
        licenses = DBDatasetsOperator(db).getLicenses()
    bottle.response.content_type = "application/json"
    return json.dumps(licenses)


def _getProjects(user: authorization.User) -> set[str]:
    if CONFIG is None: raise Exception()
    if user.isSuperAdminDatasets(): 
        allProjects = set()
        with DB(CONFIG.db) as db:
            for p in DBProjectsOperator(db).getProjects(): allProjects.add(p["code"])
        return allProjects
    else:
        return user.getProjects()

@app.route('/api/projects', method='GET')
def getProjects():
    if CONFIG is None or not isinstance(bottle.request.query, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)

    purpose = "projectList"
    if 'purpose' in bottle.request.query:
        purpose = bottle.request.query['purpose']
        try: _checkPropertyAsString('purpose', purpose, ["datasetCreation", "userManagement", "datasetSearchFilter", "projectList"])
        except WrongInputException as e: return setErrorResponse(400, str(e))
    
    if purpose == "datasetCreation" or purpose == "userManagement":
        # New datasets only can be assigned to projects which the user has joined to
        ret = list(_getProjects(user))
    elif purpose == "datasetSearchFilter":
        # List all the possible values for "project" filter in GET /datasets
        searchFilter = authorization.Search_filter()
        searchFilter.adjustByUser(user)
        with DB(CONFIG.db) as db:
            ret = DBDatasetsOperator(db).getProjectsForSearchFilter(searchFilter)
    else:  # purpose == "projectList"
        with DB(CONFIG.db) as db:
            projects = DBProjectsOperator(db).getProjects()
        for project in projects:
            project["logoUrl"] = CONFIG.self.root_url + '/project-logos/' + project["logoFileName"] if project["logoFileName"] != "" else ""
            del project["logoFileName"]
        ret = {"list": projects, 
               "allowedActionsForTheUser": user.getAllowedActionsOnProjectsForTheUser()}
    bottle.response.content_type = "application/json"
    return json.dumps(ret)

def _checkUserCanModifyProject(code):
    if CONFIG is None: raise Exception()
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminProjects():
        with DB(CONFIG.db) as db:
            if not DBProjectsOperator(db).existsProject(code) or not user.canModifyProject(code):
                return setErrorResponse(401, "unauthorized user")
    return user

def _obtainAndWriteLogoToPngFile(sourceUrl, destinationFilePath):
    if CONFIG is None: raise Exception()
    if not isinstance(sourceUrl, str) or not utils.is_valid_url(sourceUrl, empty_path_allowed=False):
        raise WrongInputException("'logoUrl' property must be an URL-formated string.")
    LOG.debug("URL in body: " + sourceUrl )
    originalFilePath = destinationFilePath + '.original'
    try:
        try:
            utils.download_url(sourceUrl, originalFilePath, CONFIG.logos.max_upload_file_size_mb)
        except Exception as e:
            LOG.exception(e)
            raise WrongInputException("There was a problem downloading the logo image file. Bad URL?")
        try:
            utils.resize_and_encode_logo_file_to_png(originalFilePath, destinationFilePath, CONFIG.logos.image_size_px)
        except Exception as e:
            LOG.exception(e)
            raise WrongInputException("There was a problem processing the logo image file. " 
                                      "Probably unsupported format, try another or convert by yourself to PNG/JPEG.")
    finally:
        # remove original file if exist
        if os.path.exists(originalFilePath): os.unlink(originalFilePath)
    
    # elif 'logoImage' in projectData.keys():
    #     logoFile = bottle.request.files["logoImage"]
    #     name, ext = os.path.splitext(logoFile.filename)
    #     if not ext in ['.png']:
    #         return setErrorResponse(400, 'File extension not allowed, supported formats: png')
    #     logoFile.file.read().decode('UTF-8').splitlines()
    #     logoFile.save(destinationPath + ".tmp")
    #     imageType = checkImageAndGetType()
    #     return True

@app.route('/api/projects/<code>', method='PUT')
def putProject(code):
    if CONFIG is None or not isinstance(bottle.request.body, io.IOBase) \
       or not isinstance(bottle.request.files, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    try: _checkPropertyAsString('projectCode', code, min_length=2, max_length=16, only_alphanum_or_dash=True )
    except WrongInputException as e: return setErrorResponse(400, str(e))
    ret = _checkUserCanModifyProject(code)
    if isinstance(ret, str): return ret  # return error message
    
    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")
    read_data = None
    logoFileName = code + ".png"   # str(uuid.uuid4())
    destinationFilePath = os.path.join(CONFIG.self.static_files_logos_dir_path, logoFileName)
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        projectData = json.loads(read_data)
        if not 'name' in projectData.keys(): raise WrongInputException("'name' property is required.")
        _checkPropertyAsString("name", projectData["name"], min_length=3, max_length=160)
        name = projectData["name"]
        if not 'shortDescription' in projectData.keys(): raise WrongInputException("'shortDescription' property is required.")
        _checkPropertyAsString("shortDescription", projectData["shortDescription"])
        shortDescription = projectData["shortDescription"]
        externalUrl = projectData["externalUrl"] if "externalUrl" in projectData.keys() else ''
        if not isinstance(externalUrl, str) or (externalUrl != '' and not utils.is_valid_url(externalUrl)):
            raise WrongInputException("'externalUrl' property must be an URL-formated string.")
        
        if 'logoUrl' in projectData.keys() and projectData["logoUrl"] != "":
            _obtainAndWriteLogoToPngFile(projectData["logoUrl"], destinationFilePath + ".tmp")
        else:
            logoFileName = ""

        if not 'projectConfig' in projectData.keys(): raise WrongInputException("'projectConfig' property is required.")
        _checkPropertyAsObject("projectConfig", projectData["projectConfig"])

        with DB(CONFIG.db) as db:
            LOG.debug("Creating or updating project: %s" % code)
            DBProjectsOperator(db).createOrUpdateProject(code, name, shortDescription, externalUrl, logoFileName)
            # project configuration is included in this operation also
            _putProjectConfig(projectData["projectConfig"], code, db)

            if AUTH_ADMIN_CLIENT != None and CONFIG.auth.admin_api.parent_group_of_project_groups != "":
                AUTH_ADMIN_CLIENT.createGroup(CONFIG.auth.token_validation.project_group_prefix+code, 
                                              CONFIG.auth.admin_api.parent_group_of_project_groups)

        if os.path.exists(destinationFilePath + ".tmp"): 
            os.rename(destinationFilePath + ".tmp", destinationFilePath)
        LOG.debug('Project successfully created or updated.')
        bottle.response.status = 201
    except WrongInputException as e:
        return setErrorResponse(400, str(e))
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        # delete if exist temporal image logo file
        if os.path.exists(destinationFilePath + ".tmp"): os.unlink(destinationFilePath + ".tmp")
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")

def _putProjectConfig(projectConfig, projectCode, db):
    defaultContactInfo = projectConfig["defaultContactInfo"] if "defaultContactInfo" in projectConfig.keys() else ''
    if "defaultLicense" in projectConfig.keys():
        _checkPropertyAsLicense(projectConfig["defaultLicense"])
        defaultLicenseTitle = projectConfig["defaultLicense"]["title"]
        defaultLicenseUrl = projectConfig["defaultLicense"]["url"]
    else:
        defaultLicenseTitle = ""
        defaultLicenseUrl = ""
    zenodoAccessToken = projectConfig["zenodoAccessToken"] if "zenodoAccessToken" in projectConfig.keys() else ''
    if not isinstance(zenodoAccessToken, str): raise WrongInputException("'zenodoAccessToken' property must be a string.")
    zenodoAuthor = projectConfig["zenodoAuthor"] if "zenodoAuthor" in projectConfig.keys() else ''
    if not isinstance(zenodoAuthor, str): raise WrongInputException("'zenodoAuthor' property must be a string.")
    zenodoCommunity = projectConfig["zenodoCommunity"] if "zenodoCommunity" in projectConfig.keys() else ''
    if not isinstance(zenodoCommunity, str): raise WrongInputException("'zenodoCommunity' property must be a string.")
    zenodoGrant = projectConfig["zenodoGrant"] if "zenodoGrant" in projectConfig.keys() else ''
    if not isinstance(zenodoGrant, str): raise WrongInputException("'zenodoGrant' property must be a string.")
    DBProjectsOperator(db).setProjectConfig(projectCode, defaultContactInfo, defaultLicenseTitle, defaultLicenseUrl,
                                            zenodoAccessToken, zenodoAuthor, zenodoCommunity, zenodoGrant)


@app.route('/api/projects/<code>/config', method='PUT')
def putProjectConfig(code):
    if CONFIG is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = _checkUserCanModifyProject(code)
    if isinstance(ret, str): return ret  # return error message
    
    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")
    read_data = None
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        projectConfig = json.loads(read_data)
        with DB(CONFIG.db) as db:
            LOG.debug("Updating config of project: %s" % code)
            _putProjectConfig(projectConfig, code, db)
        LOG.debug('Project config successfully updated.')
        bottle.response.status = 201
    except WrongInputException as e:
        return setErrorResponse(400, str(e))
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")


@app.route('/api/projects/<code>', method='GET')
def getProject(code):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    
    with DB(CONFIG.db) as db:
        project = DBProjectsOperator(db).getProject(code)
    if project is None: return setErrorResponse(404, "not found")
    project["logoUrl"] = CONFIG.self.root_url + '/project-logos/' + project["logoFileName"] if project["logoFileName"] != "" else ""
    del project["logoFileName"]
    project["editablePropertiesByTheUser"] = user.getEditablePropertiesOfProjectByTheUser(code)
    project["allowedActionsForTheUser"] = user.getAllowedActionsOnProjectForTheUser(code)
    bottle.response.content_type = "application/json"
    return json.dumps(project)

@app.route('/api/projects/<code>/config', method='GET')
def getProjectConfig(code):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = _checkUserCanModifyProject(code)
    if isinstance(ret, str): return ret  # return error message
    
    with DB(CONFIG.db) as db:
        dbprojects = DBProjectsOperator(db)
        project = dbprojects.getProject(code)
        if project is None: return setErrorResponse(404, "not found")
        config = dbprojects.getProjectConfig(code)
    bottle.response.content_type = "application/json"
    return json.dumps(config)

@app.route('/api/projects/<code>', method='PATCH')
def patchProject(code):
    if CONFIG is None or AUTH_CLIENT is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader()
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if user.isUnregistered():
        return setErrorResponse(401, "unauthorized user")
    
    logoFileName = code + ".png"  # str(uuid.uuid4())
    destinationFilePath = os.path.join(CONFIG.self.static_files_logos_dir_path, logoFileName)
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        patch = json.loads( read_data )
        property = patch["property"]
        newValue = patch["value"]
        with DB(CONFIG.db) as db:
            dbprojects = DBProjectsOperator(db)
            if not dbprojects.existsProject(code):
                return setErrorResponse(404, "not found")
            if property not in user.getEditablePropertiesOfProjectByTheUser(code):
                return setErrorResponse(400, "the property is not in the editablePropertiesByTheUser list")
            elif property == "name":
                _checkPropertyAsString("value", newValue, min_length=3, max_length=160)
                dbprojects.setProjectName(code, newValue)
            elif property == "shortDescription":
                if not isinstance(newValue, str): raise WrongInputException("The value must be string.")
                _checkPropertyAsString("value", newValue)
                dbprojects.setProjectShortDescription(code, newValue)
            elif property == "externalUrl":
                if not isinstance(newValue, str) or (newValue != '' and not utils.is_valid_url(newValue)):
                    raise WrongInputException("'externalUrl' property must be an URL-formated string.")
                dbprojects.setProjectExternalUrl(code, newValue)
            elif property == "logoUrl":
                if newValue != "":
                    _obtainAndWriteLogoToPngFile(newValue, destinationFilePath + ".tmp")
                else:
                    logoFileName = ""
                dbprojects.setProjectLogoFileName(code, logoFileName)
                if os.path.exists(destinationFilePath + ".tmp"): 
                    os.rename(destinationFilePath + ".tmp", destinationFilePath)
            else:
                return setErrorResponse(400, "invalid property")

        LOG.debug('Project successfully updated.')
        bottle.response.status = 204
    except WrongInputException as e:
        return setErrorResponse(400, str(e))
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        # delete if exist temporal image logo file
        if os.path.exists(destinationFilePath + ".tmp"): os.unlink(destinationFilePath + ".tmp")
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")


@app.route('/api/user/<userName>', method='POST')
def postUser(userName):
    # Transitional condition while clients change to new PUT operation
    LOG.debug("Received (Transitional) POST %s" % bottle.request.path)
    return putUser(userName)

def _getRolesAndProjectsFromUserId(userId):
    if CONFIG is None or AUTH_ADMIN_CLIENT is None: raise Exception()
    groups = AUTH_ADMIN_CLIENT.getUserGroups(userId)
    roles, projects = [], []
    roles_prefix_len = len(CONFIG.auth.user_management.prefix_for_roles_as_groups)
    projects_prefix_len = len(CONFIG.auth.user_management.prefix_for_projects_as_groups)
    for g in groups:
        if str(g).startswith(CONFIG.auth.user_management.prefix_for_projects_as_groups):
            projects.append(g[projects_prefix_len:])
        elif str(g).startswith(CONFIG.auth.user_management.prefix_for_roles_as_groups):
            roles.append(g[roles_prefix_len:])
    return roles, projects

def _updateRolesForUserId(userId, currentRoles, newRoles):
    if CONFIG is None or AUTH_ADMIN_CLIENT is None: raise Exception()
    LOG.debug("Current roles of the user: " + json.dumps(currentRoles))
    LOG.debug("New roles for the user: " + json.dumps(newRoles))
    for r in currentRoles:
        if not r in newRoles:
            LOG.debug("The current role '%s' is not in the new selection, it must be removed. " % r)
            AUTH_ADMIN_CLIENT.removeUserFromGroup(userId, CONFIG.auth.user_management.prefix_for_roles_as_groups + r)
    for r in newRoles:
        if not r in currentRoles:
            LOG.debug("The role '%s' in the new selection is not previously assigned, it must be added. " % r)
            AUTH_ADMIN_CLIENT.addUserToGroup(userId, CONFIG.auth.user_management.prefix_for_roles_as_groups + r)

def _updateProjectsForUserId(userId, currentProjects, newProjects):
    if CONFIG is None or AUTH_ADMIN_CLIENT is None: raise Exception()
    LOG.debug("Current projects of the user: " + json.dumps(currentProjects))
    LOG.debug("New projects for the user: " + json.dumps(newProjects))
    for p in currentProjects:
        if not p in newProjects:
            LOG.debug("The current project '%s' is not in the new selection, it must be removed. " % p)
            AUTH_ADMIN_CLIENT.removeUserFromGroup(userId, CONFIG.auth.user_management.prefix_for_projects_as_groups + p)
    for p in newProjects:
        if not p in currentProjects:
            LOG.debug("The project '%s' in the new selection is not previously assigned, it must be added. " % p)
            AUTH_ADMIN_CLIENT.addUserToGroup(userId, CONFIG.auth.user_management.prefix_for_projects_as_groups + p)


@app.route('/api/users/<username>', method='PUT')
def putUser(username):
    if CONFIG is None or AUTH_ADMIN_CLIENT is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401, "unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")
    read_data = None
    try:
        read_data = bottle.request.body.read().decode('UTF-8')
        LOG.debug("BODY: " + read_data)
        userData = json.loads(read_data)
        #userGroups = userData["groups"]
        userGid = int(userData["gid"]) if "gid" in userData.keys() else None
        site = userData["siteCode"] if "siteCode" in userData.keys() else None
        newProjects = userData["projects"] if "projects" in userData.keys() else None
        newRoles = userData["roles"] if "roles" in userData.keys() else None

        if site != None: _checkPropertyAsString("siteCode", site, min_length=2, max_length=50)
        if newProjects != None:
            available_projects = _getProjects(user)
            # The available projects for now are those which the validator user are joined to.
            _checkPropertyAsArrayOfStrings("projects", newProjects, available_projects)
        if newRoles != None:
            _checkPropertyAsArrayOfStrings("roles", newRoles, CONFIG.auth.user_management.assignable_general_roles)

        userId = AUTH_ADMIN_CLIENT.getUserId(username)
        if userId is None: raise WrongInputException("Username '%s' not found in the auth service." % username)

        LOG.debug("Creating or updating user in DB: %s, %s, %s" % (userId, username, userGid))
        with DB(CONFIG.db) as db:
            DBDatasetsOperator(db).createOrUpdateUser(userId, username, site, userGid)

            currentRoles, currentProjects = _getRolesAndProjectsFromUserId(userId)
            if newRoles != None:
                _updateRolesForUserId(userId, currentRoles, newRoles)
            if newProjects != None:
                _updateProjectsForUserId(userId, currentProjects, newProjects)

        if CONFIG.user_management_scripts.job_template_file_path != "":
            # only launch job if any relevant change
            if newRoles != None or newProjects != None or site != None:
                if newRoles is None: newRoles = currentRoles
                if newProjects is None: newProjects = currentProjects
                if site is None: 
                    with DB(CONFIG.db) as db:
                        ret = DBDatasetsOperator(db).getUser(username)
                        if ret is None: raise Exception()
                        site = ret["siteCode"]
                LOG.debug('Launching user creation job...')
                k8sClient = k8s.K8sClient()
                ok = k8sClient.add_user_creation_job(username, newRoles, site, newProjects, 
                                                     CONFIG.user_management_scripts.job_template_file_path)
                if not ok: 
                    raise K8sException("Unexpected error launching user creation job.")

        LOG.debug('User successfully created or updated.')
        bottle.response.status = 201
    except (WrongInputException, keycloak.KeycloakAdminAPIException) as e:
        return setErrorResponse(400, str(e))
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except (K8sException, Exception) as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")

@app.route('/api/users/<username>/managementJobs', method='GET')
def getUserManagementJobs(username):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401, "unauthorized user")
    
    # Check the user exists, just for security
    with DB(CONFIG.db) as db:
        userId, userGid = DBDatasetsOperator(db).getUserIDs(username)
        if userId is None: return setErrorResponse(404, "user not found")
    k8sClient = k8s.K8sClient()
    jobs = k8sClient.list_user_creation_jobs(username)
    bottle.response.content_type = "application/json"
    return json.dumps(jobs)

@app.route('/api/users/<username>/managementJobs/<uid>', method='GET')
def getUserManagementJobLog(username, uid):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401, "unauthorized user")
    
    # Check the user exists, just for security
    with DB(CONFIG.db) as db:
        userId, userGid = DBDatasetsOperator(db).getUserIDs(username)
        if userId is None: return setErrorResponse(404, "user not found")
    k8sClient = k8s.K8sClient()
    logs = k8sClient.get_user_creation_job_logs(username, uid)
    if logs is None: return setErrorResponse(404, "not found")
    bottle.response.content_type = "text/plain"
    return logs

@app.route('/api/users/<username>', method='GET')
def getUser(username):
    if CONFIG is None or AUTH_ADMIN_CLIENT is None or not isinstance(bottle.request.query, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401, "unauthorized user")
    try:
        scope = "gid"
        if 'scope' in bottle.request.query:
            scope = bottle.request.query['scope']
            _checkPropertyAsString('scope', scope, ["gid", "all"])
        
        if scope == "gid":
            with DB(CONFIG.db) as db:
                userId, userGid = DBDatasetsOperator(db).getUserIDs(username)
            if userId is None: return setErrorResponse(404, "user not found")
            ret = dict(gid = userGid)
        else:  # scope = all
            with DB(CONFIG.db) as db:
                ret = DBDatasetsOperator(db).getUser(username)
            #if ret is None: return setErrorResponse(404, "user not found")
            if ret is None: ret = {"username": username}
            userId = AUTH_ADMIN_CLIENT.getUserId(username)
            if userId is None: return setErrorResponse(404, "user not found in the auth service")
            #if "uid" in ret and ret["uid"] != userId: raise Exception("User id from DB is not the same as from the auth service.")
            ret["roles"], ret["projects"] = _getRolesAndProjectsFromUserId(userId)
            ret["attributesFromAuthService"] = AUTH_ADMIN_CLIENT.getUserAttributes(userId)

        bottle.response.content_type = "application/json"
        return json.dumps(ret)
    except WrongInputException as e:
        return setErrorResponse(400, str(e))
    except keycloak.KeycloakAdminAPIException as e:
        return setErrorResponse(500, str(e))

@app.route('/api/users', method='GET')
def getUsers():
    if CONFIG is None or AUTH_ADMIN_CLIENT is None or not isinstance(bottle.request.query, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401, "unauthorized user")
    
    try:
        disabled = None
        if 'disabled' in bottle.request.query:
            disabled = parse_flag_value(bottle.request.query['disabled'])

        skip = int(bottle.request.query['skip']) if 'skip' in bottle.request.query else 0
        limit = int(bottle.request.query['limit']) if 'limit' in bottle.request.query else 30
        if skip < 0: skip = 0
        if limit < 0: limit = 0
        searchString =  str(bottle.request.query['searchString']).strip()  if 'searchString' in bottle.request.query else ""
    except WrongInputException as e:
        return setErrorResponse(400, str(e))

    users, total = AUTH_ADMIN_CLIENT.getUsers(skip, limit, searchString, disabled)

    with DB(CONFIG.db) as db:
        usersInDB = DBDatasetsOperator(db).getUsers()
    for u in users:
        username = u["username"]
        u["gid"] = usersInDB[username]["gid"] if username in usersInDB else None

    bottle.response.content_type = "application/json"
    return json.dumps({ "total": total,
                        "returned": len(users),
                        "skipped": skip,
                        "limit": limit,
                        "list": users})

@app.route('/api/userRoles', method='GET')
def getUserRoles():
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401, "unauthorized user")
            
    bottle.response.content_type = "application/json"
    return json.dumps(CONFIG.auth.user_management.assignable_general_roles)

@app.route('/api/userSites', method='GET')
def getUserSites():
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminUsers():
        return setErrorResponse(401, "unauthorized user")

    with DB(CONFIG.db) as db:
        ret = DBDatasetsOperator(db).getSites()
    bottle.response.content_type = "application/json"
    return json.dumps(ret)


def checkDatasetListAccess(datasetIDs: list, userName: str):
    if CONFIG is None: raise Exception()
    badIDs = []
    with DB(CONFIG.db) as db:
        dbdatasets = DBDatasetsOperator(db)
        userId, userGID = dbdatasets.getUserIDs(userName)
        if userId is None: return datasetIDs.copy()  # all datasetIDs are bad
        ret = getTokenOfAUserFromAuthAdminClient(userId)
        if isinstance(ret, str): return ret  # return error message
        user = authorization.User(ret)
        for id in datasetIDs:
            dataset = dbdatasets.getDataset(id)
            if dataset is None:
                # invalidated or not exists
                badIDs.append(id); continue
            if dataset["draft"]:
                dataset["creating"] = (dbdatasets.getDatasetCreationStatus(id) != None)
            datasetACL = dbdatasets.getDatasetACL(id)
            if not user.canUseDataset(dataset, datasetACL):
                badIDs.append(id); continue
    return badIDs

@app.route('/api/datasetAccessCheck', method='POST')
def postDatasetAccessCheck():
    if not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401, "unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")
    read_data = None
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
            ret = json.dumps(badIds)
            LOG.debug('Access denied to datasets: ' + ret)
            return ret
                
        LOG.debug('Dataset access granted.')
        bottle.response.status = 204
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")

@app.route('/api/datasetAccess/<id>', method='POST')
def postDatasetAccess(id):
    if CONFIG is None or AUTH_CLIENT is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401, "unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")

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
        isJobFromDesktop = datasetAccess["isJobFromDesktop"] if "isJobFromDesktop" in datasetAccess else isJob
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
        if len(badIds) > 0:
            return setErrorResponse(403, "access denied")

        with DB(CONFIG.db) as db:
            dbdatasets = DBDatasetsOperator(db)
            userId, userGID = dbdatasets.getUserIDs(userName)
            if userGID is None:
                return setErrorResponse(400, "The user does not have a GID.")
            if CONFIG.self.datasets_mount_path != '' and not isJobFromDesktop:
                # For jobs from desktop (jobman-cli) is not required to set the ACLs because they are already set for desktop and it's a high-cost operation
                for id in datasetIDs:
                    pathsOfStudies = dbdatasets.getPathsOfStudiesFromDataset(id)
                    LOG.debug('Setting ACLs in dataset %s for GID %s ...' % (id, userGID))
                    datasetDirName = id
                    dataset_file_system.give_access_to_dataset(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.datalake_mount_path, pathsOfStudies, userGID)

            DBDatasetAccessesOperator(db).createDatasetAccess(datasetAccessId, datasetIDs, userGID, accessType, toolName, toolVersion, image, commandLine, 
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
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")

@app.route('/api/datasetAccess/<id>', method='PATCH')
def endDatasetAccess(id):
    if CONFIG is None or not isinstance(bottle.request.body, io.IOBase): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401, "unauthorized user")

    content_types = get_header_media_types('Content-Type')
    if not 'application/json' in content_types:
        return setErrorResponse(400, "invalid 'Content-Type' header, required 'application/json'")

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
            dbdatasets = DBDatasetsOperator(db)
            db_ds_accesses = DBDatasetAccessesOperator(db)
            if not db_ds_accesses.existsDatasetAccess(datasetAccessId):
                return setErrorResponse(404, "not found")    

            if CONFIG.self.datasets_mount_path != '':
                userGID, datasetIDsCandidatesForRemovePermission = db_ds_accesses.getDatasetAccess(datasetAccessId)
                db_ds_accesses.endDatasetAccess(datasetAccessId, startTime, endTime, status)
                # collect all the datasets still accessed after the end of this datasetAccess
                datasetIDsStillAccessedAfter = db_ds_accesses.getDatasetsCurrentlyAccessedByUser(userGID)
                # collect all the studies still accessed after the end of this datasetAccess
                pathsOfstudiesStillAfter = set()
                for id in datasetIDsStillAccessedAfter:
                    pathsOfstudiesStillAfter.update(dbdatasets.getPathsOfStudiesFromDataset(id))

                for id in datasetIDsCandidatesForRemovePermission:
                    if id in datasetIDsStillAccessedAfter: continue  # this dataset is still accessed, skip without remove permissions
                    datasetDirName = id
                    LOG.debug('Removing ACLs for GID %s in dataset directory %s not accessed anymore by this user...' % (userGID, datasetDirName))
                    dataset_file_system.remove_access_to_dataset(CONFIG.self.datasets_mount_path, datasetDirName, userGID)
                    pathsOfStudies = set(dbdatasets.getPathsOfStudiesFromDataset(id))
                    pathsOfStudies.difference_update(pathsOfstudiesStillAfter)  # take out the studies still accessed to avoid remove permissions on them
                    LOG.debug('Removing ACLs for GID %s in %d studies no accessed anymore by this user...' % (userGID, len(pathsOfStudies)))
                    dataset_file_system.remove_access_to_studies(CONFIG.self.datalake_mount_path, pathsOfStudies, userGID)

        LOG.debug('Dataset access successfully ended.')
        bottle.response.status = 204
    except json.decoder.JSONDecodeError as e:
        return setErrorResponse(400, "Error deconding the body as JSON: " + str(e))
    except Exception as e:
        LOG.exception(e)
        if read_data != None: LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500, "Unexpected error, may be the input is wrong")

@app.route('/api/datasets/<id>/accessHistory', method='GET')
def getDatasetAccessHistory(id):
    if CONFIG is None or not isinstance(bottle.request.query, bottle.FormsDict): raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    if isinstance(ret, str): return ret  # return error message
    user = authorization.User(ret)
    if not user.canAdminDatasetAccesses():
        return setErrorResponse(401, "unauthorized user")

    datasetId = id
    skip = int(bottle.request.query['skip']) if 'skip' in bottle.request.query else 0
    limit = int(bottle.request.query['limit']) if 'limit' in bottle.request.query else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0

    with DB(CONFIG.db) as db:
        dbdatasets = DBDatasetsOperator(db)
        dataset = dbdatasets.getDataset(datasetId)
        if dataset is None: return setErrorResponse(404, "not found")
        if dataset["draft"]:
            dataset["creating"] = (dbdatasets.getDatasetCreationStatus(datasetId) != None)
        if not user.canViewDatasetDetails(dataset):
            return setErrorResponse(401, "unauthorized user")

        accesses, total = DBDatasetAccessesOperator(db).getDatasetAccesses(datasetId, limit, skip)    
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

# ====================
# Project logos routes
# ====================

# static files (any route that ends with '.' + known extension, including subpaths)
# To avoid conflicts, static files prefixed with /project-logos/
@app.route('/project-logos/<file_path:re:.+>', method='GET')
def getStaticFileLogos(file_path):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    LOG.debug("Static file (logos): "+file_path)
    return bottle.static_file(file_path, CONFIG.self.static_files_logos_dir_path)

# ====================
# output-files routes
# ====================

# To avoid conflicts, static files prefixed with /output-files/
@app.route('/output-files/<file_path:re:.+>', method='GET')
def getStaticFileOutput(file_path):
    if CONFIG is None: raise Exception()
    LOG.debug("Received %s %s" % (bottle.request.method, bottle.request.path))
    # ret = getTokenFromAuthorizationHeader(serviceAccount=True)
    # if isinstance(ret, str): return ret  # return error message
    # user = authorization.User(ret)
    # if user.isUnregistered:
    #     return setErrorResponse(401, "unauthorized user")
    LOG.debug("Static file (output): "+file_path)
    return bottle.static_file(file_path, CONFIG.self.static_files_output_dir_path)


# ====================
# Any other route
# ====================

# Any other path (without prefix /api/ or /web/ or /project-logos/ or /output-files/) 
# will be responded with the index.html content,
# index.html loads a javascript interface that manage those other paths.
@app.route('/', method='GET')
@app.route('/<any_path:re:.+>', method='GET')
def getWebUI(any_path=''):
    if CONFIG is None: raise Exception()
    LOG.debug("Received GET /" + any_path)
    LOG.debug("Routed to index.html")
    return bottle.static_file('index.html', CONFIG.self.static_files_dir_path)

