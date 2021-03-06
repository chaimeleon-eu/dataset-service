#! /usr/bin/env python3

from datetime import datetime
from enum import Enum
from pathlib import Path
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
import pydicom
import dataset_service.tracer as tracer
import dataset_service.pid as pid
from dataset_service.dataset import *
from dataset_service import dicom

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

    if CONFIG.tracer.url == '':
        LOG.warn("tracer.url is empty: actions will not be notified to the tracer-service.")
    else:
        LOG.info("Checking connection to tracer-service on %s..." % CONFIG.tracer.url)
        try:
            retries = 0
            while retries < 5:
                try:
                    hash_codes = tracer.getSupportedHashAlgorithms(
                        CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url)
                    LOG.info("Accepted hash codes: %s" % json.dumps(hash_codes))
                    break;
                except urllib.error.HTTPError as e1:
                    LOG.error("HTTPError: " + str(e1.code) + " - " + str(e1.reason))
                except urllib.error.URLError as e2:
                    LOG.error("URLError: "+ str(e2.reason))
                retries += 1
                LOG.info("Retrying in 5 seconds...")
                time.sleep(5)
            if retries >= 5: raise Exception("Unable to connect to tracer-service.")
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

def appendIfNotExists(array, item):
    if item not in array: array.append(item)

def checkAuthorizationHeader(serviceAccount=False):
    if not "authorization" in bottle.request.headers: 
        LOG.debug("Not registered user.")
        return True, None
    try:
        encodedToken = bottle.request.headers["authorization"][7:]
    except Exception as e:
        return False, setErrorResponse(401, "invalid authorization header")
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
    if CONFIG.auth.roles.admin_datasets in user_info["appRoles"]:
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.access_all_datasets)
    if CONFIG.auth.roles.superadmin_datasets in user_info["appRoles"]:
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.access_all_datasets)
        appendIfNotExists(user_info["appRoles"], CONFIG.auth.roles.admin_datasets)

    return True, user_info

def userCanCreateDatasets(user_info):
    return user_info != None and CONFIG.auth.roles.admin_datasets in user_info["appRoles"]

class Access_type(Enum):
    VIEW_DETAILS = 1
    USE = 2

def userCanAccessDataset(user_info, dataset, access_type = Access_type.VIEW_DETAILS):
    if user_info != None and CONFIG.auth.roles.superadmin_datasets in user_info["appRoles"]: return True

    if dataset["invalidated"] and access_type is Access_type.USE: return False

    if dataset["draft"] and (user_info is None or user_info["sub"] != dataset["authorId"]):
        return False

    if dataset["public"]: return True

    return (user_info != None and CONFIG.auth.roles.access_all_datasets in user_info["appRoles"])

def tmpUserCanAccessDataset(userId, userGroups, dataset, access_type = Access_type.USE):
    if "cloud-services-and-security-management" in userGroups: return True

    if dataset["invalidated"] and access_type is Access_type.USE: return False

    if dataset["draft"] and userId != dataset["authorId"]:
        return False

    if dataset["public"]: return True
    
    return "data-scientists" in userGroups
    
def userCanModifyDataset(user_info, dataset):
    if user_info is None: return False
    if not CONFIG.auth.roles.admin_datasets in user_info["appRoles"]: return False
    if CONFIG.auth.roles.superadmin_datasets in user_info["appRoles"]: return True
    return user_info["sub"] == dataset["authorId"]

class Search_filter():
    def __init__(self, draft, public, invalidated):
        self.draft = draft
        self.public = public
        self.invalidated = invalidated
        self.userIdForInvalidatedAndDraft = None

def adjustSearchFilterByUser(user_info, search_filter):
    if user_info is None or not CONFIG.auth.roles.access_all_datasets in user_info["appRoles"]:
        search_filter.public = True
        search_filter.invalidated = False
        search_filter.draft = False
        return search_filter

    if not CONFIG.auth.roles.admin_datasets in user_info["appRoles"]:
        search_filter.invalidated = False
        search_filter.draft = False
        return search_filter

    if not CONFIG.auth.roles.superadmin_datasets in user_info["appRoles"]:
        search_filter.userIdForInvalidatedAndDraft = user_info["sub"]

    return search_filter

def userCanAdminUsers(user_info):
    return user_info != None and CONFIG.auth.roles.admin_users in user_info["appRoles"]

def userCanAdminDatasetAccess(user_info):
    return user_info != None and CONFIG.auth.roles.admin_datasetAccess in user_info["appRoles"]

def getEditablePropertiesByTheUser(user_info, dataset):
    editableProperties = []
    if userCanModifyDataset(user_info, dataset):
        if dataset["draft"]: 
            editableProperties.append("draft")
            editableProperties.append("name")
            editableProperties.append("description")
        else:
            editableProperties.append("public")
            editableProperties.append("pids")
        editableProperties.append("invalidated")
        editableProperties.append("contactInfo")
        editableProperties.append("license")
    return editableProperties


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


@app.route('/api/', method='GET')
def getHello():
    LOG.debug("Received GET /api/")
    bottle.response.content_type = "text/plain"
    return "Hello from Dataset Service"

@app.route('/api/set-ui', method='POST')
def postSetUI():
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

class PathException(Exception):
    pass

def checkPath(basePath, relativePath):
    # Ensures relativePath is in fact a subpath of basePath. Raises an exception if wrong path.
    # This is to avoid a malicious user try to access system directories with "..", absolute paths like "/etc" or symbolic links.
    base = Path(basePath).resolve()  # Resolve symbolic links and returns absolute path
    path = (base / relativePath).resolve()
    if not base in path.parents:
        raise PathException("Wrong path: " + str(base / relativePath))

def completeDatasetFromCSV(dataset, csvdata):
    dataset["studies"] = []
    dataset["subjects"] = []
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
                        if os.path.isdir(seriePath):  series.append(serieDirName)

                    dataset["studies"].append({
                        'studyId': str(uuid.uuid4()),
                        'studyName': studyDirName,
                        'subjectName': subjectName,
                        'path': studyDirPath,
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

    
def getMetadataFromFirstDicomFile(serieDirPath):
    for name in os.listdir(serieDirPath):
        if name.lower().endswith(".dcm"):
            dicomFilePath = os.path.join(serieDirPath, name)
            dcm = pydicom.dcmread(dicomFilePath)
            age = (dcm[dicom.AGE_TAG].value if dicom.AGE_TAG in dcm else None)
            sex = (dcm[dicom.SEX_TAG].value if dicom.SEX_TAG in dcm else None)
            bodyPart = (dcm[dicom.BODY_PART_TAG].value if dicom.BODY_PART_TAG in dcm else None)
            modality = (dcm[dicom.MODALITY_TAG].value  if dicom.MODALITY_TAG in dcm else None)
            #datasetType = dcm[dicom.DATASET_TYPE_TAG].value    it seems very similar to modality
            return age, sex, bodyPart, modality
    return None, None, None, None

def collectMetadata(dataset):
    differentSubjects = set()
    studiesCount = 0
    maxAge = "000D"
    minAge = "999Y"
    sexList = set()
    bodyPartList = set()
    modalityList = set()
    for study in dataset["studies"]:
        studiesCount += 1
        if not study["subjectName"] in differentSubjects: 
            differentSubjects.add(study["subjectName"])
        if CONFIG.self.datalake_mount_path != '':
            seriePathInDatalake = os.path.join(CONFIG.self.datalake_mount_path, study['path'], study['series'][0])
            age, sex, bodyPart, modality = getMetadataFromFirstDicomFile(seriePathInDatalake)
            if (age is None or sex is None or bodyPart is None or modality is None):
                # sometimes first serie is special, try with the second if exists
                if len(study['series']) > 1:
                    seriePathInDatalake = os.path.join(CONFIG.self.datalake_mount_path, study['path'], study['series'][1])
                    age, sex, bodyPart, modality = getMetadataFromFirstDicomFile(seriePathInDatalake)
            if age != None:
                minAge = min(minAge, age, key=lambda x: dicom.getAgeInDays(x))
                maxAge = max(maxAge, age, key=lambda x: dicom.getAgeInDays(x))
            if sex != None: sexList.add(sex)
            if bodyPart != None: bodyPartList.add(bodyPart) 
            if modality != None: modalityList.add(modality)
            
    dataset["studiesCount"] = studiesCount
    dataset["subjectsCount"] = len(differentSubjects)
    dataset["ageLow"] = (minAge if minAge != "999Y" else None)
    dataset["ageHigh"] = (maxAge if maxAge != "000D" else None)
    dataset["sex"] = list(sexList)
    dataset["bodyPart"] = list(bodyPartList)
    dataset["modality"] = list(modalityList)
    LOG.debug("  -studiesCount: %s" % dataset["studiesCount"])
    LOG.debug("  -subjectsCount: %s" % dataset["subjectsCount"])
    LOG.debug("  -ageLow: %s" % dataset["ageLow"])
    LOG.debug("  -ageHigh: %s" % dataset["ageHigh"])
    LOG.debug("  -sex: %s" % dataset["sex"])
    LOG.debug("  -bodyPart: %s" % dataset["bodyPart"])
    LOG.debug("  -modality: %s" % dataset["modality"])
    

def collectMetadata2(dataset):
    if CONFIG.self.CONFIG.self.datalake_mount_path == '': return
    from pydicom.fileset import FileSet
    fs = FileSet()
    for study in dataset["studies"]:
        for serie in study["series"]:
            seriePathInDatalake = os.path.join(CONFIG.self.datalake_mount_path, study['path'], study['series'][0])
            fs.add(seriePathInDatalake)
    values = fs.find_values(["StudyInstanceUID", "PatientID", (0x0010, 0x1010), (0x0010, 0x0040), (0x0008, 0x0016), (0x0018, 0x0015), (0x0008, 0x0060)])
    dataset["studiesCount"] = len(values["StudyInstanceUID"])
    dataset["subjectsCount"] = len(values["PatientID"])
    # dataset["ageLow"] = reduce(lambda x, y: min(x, getAgeInYears(y)), values[0x0010, 0x1010])
    # dataset["ageHigh"] = reduce(lambda x, y: max(x, getAgeInYears(y)), values[0x0010, 0x1010])
    dataset["ageLow"] = min(values[0x0010, 0x1010], key=lambda x: dicom.getAgeInDays(x))
    dataset["ageHigh"] = max(values[0x0010, 0x1010], key=lambda x: dicom.getAgeInDays(x))
    dataset["sex"] = values[0x0010, 0x0040]
    dataset["datasetType"] = values[0x0008, 0x0016]
    dataset["bodyPart"] = values[0x0018, 0x0015]
    dataset["modality"] = values[0x0008, 0x0060]

@app.route('/api/datasets', method='POST')
def postDataset():
    LOG.debug("Received POST /dataset")
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret # can be None
    if user_info is None or not userCanCreateDatasets(user_info):
        return setErrorResponse(401,"unauthorized user")

    userId = user_info["sub"]
    userUsername = user_info["preferred_username"]
    userName = user_info["name"]
    userEmail = user_info["email"]

    content_types = get_header_media_types('Content-Type')
    if "external" in bottle.request.params and bottle.request.params["external"].lower() == "true":
        if not 'multipart/form-data' in content_types:
            return setErrorResponse(400,"invalid 'Content-Type' header, required 'multipart/form-data'")
    else:
        if not 'application/json' in content_types:
            return setErrorResponse(400,"invalid 'Content-Type' header, required 'application/json'")

    datasetDirName = ''
    datasetId = str(uuid.uuid4())
    try:
        if "external" in bottle.request.params and bottle.request.params["external"].lower() == "true":
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
            completeDatasetFromCSV(dataset, clinicalDataFile.file.read().decode('UTF-8').splitlines())
            #return  json.dumps(dataset)
        else:
            read_data = bottle.request.body.read().decode('UTF-8')
            LOG.debug("BODY: " + read_data)
            dataset = json.loads( read_data )
            # Security check: review all data sent by the user which is involved in path constructions
            if CONFIG.self.datalake_mount_path != '':
                for study in dataset["studies"]:
                    checkPath(CONFIG.self.datalake_mount_path, study['path'])
                    for serie in study["series"]:
                        checkPath(CONFIG.self.datalake_mount_path, os.path.join(study['path'], serie))
            if CONFIG.self.datasets_mount_path != '':
                for study in dataset["studies"]:
                    checkPath(CONFIG.self.datasets_mount_path, os.path.join(datasetId, study['subjectName']))

        with DB(CONFIG.db) as db:
            LOG.debug("Updating author: %s, %s, %s, %s" % (userId, userUsername, userName, userEmail))
            db.createOrUpdateAuthor(userId, userUsername, userName, userEmail)

            if 'previousId' in dataset.keys():
                if not db.existDataset(dataset["previousId"]):
                    return setErrorResponse(400,"dataset.previousId does not exist")
            else:
                dataset["previousId"] = None

            dataset["id"] = datasetId
            LOG.debug("UUID generated: " + dataset["id"])
            dataset["creationDate"] = datetime.now()
            #if not "public" in dataset.keys(): 
            dataset["public"] = False

            LOG.debug("Scanning dataset for collecting metadata... ")
            collectMetadata(dataset)
                        
            LOG.debug('Creating dataset in DB...')
            db.createDataset(dataset, userId)

            LOG.debug('Creating studies in DB...')
            for study in dataset["studies"]:
                db.createOrUpdateStudy(study, datasetId)

            if CONFIG.self.datasets_mount_path != '':
                LOG.debug('Creating directory and symbolic links...')
                datasetDirName = datasetId
                create_dataset(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.datalake_mount_path, dataset["studies"])
                
                LOG.debug('Writing E-FORM:' + CONFIG.self.eforms_file_name)
                with open(os.path.join(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.eforms_file_name) , 'w') as outputStream:
                    json.dump(dataset["subjects"], outputStream)

                LOG.debug('Writing INDEX: ' + CONFIG.self.index_file_name)
                # dataset["studies"] contains just the information we want to save in the index.json file
                # Set paths relative to the dataset directory
                for study in dataset["studies"]:
                    subjectDirName = study['subjectName']
                    study["path"] = os.path.join(subjectDirName, os.path.basename(study['path']))
                # And dump to the file in the dataset directory
                with open(os.path.join(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.index_file_name) , 'w') as outputStream:
                    json.dump(dataset["studies"], outputStream)

                if CONFIG.tracer.url != '':
                    LOG.debug('Calculating SHAs and notifying to tracer-service.')
                    # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
                    tracer.traceDatasetCreation(CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url, 
                                                CONFIG.self.datasets_mount_path, datasetDirName, dataset, userId)
                
        LOG.debug('Dataset successfully created.')
        bottle.response.status = 201
        bottle.response.content_type = "application/json"
        return json.dumps(dict(url = "/api/datasets/" + datasetId))

    except (PathException) as e:
        if datasetDirName != '': remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
        return setErrorResponse(400, str(e))
    except (tracer.TraceException, DatasetException) as e:
        if datasetDirName != '': remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
        return setErrorResponse(500, str(e))
    except Exception as e:
        LOG.exception(e)
        if not "external" in bottle.request.params or bottle.request.params["external"].lower() != "true":
            LOG.error("May be the body of the request is wrong: %s" % read_data)
        if datasetDirName != '': remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/datasets/<id>', method='GET')
def getDataset(id):
    LOG.debug("Received GET /dataset/%s" % id)
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret # can be None

    datasetId = id
    skip = int(bottle.request.params['studiesSkip']) if 'studiesSkip' in bottle.request.params else 0
    limit = int(bottle.request.params['studiesLimit']) if 'studiesLimit' in bottle.request.params else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0

    with DB(CONFIG.db) as db:
        dataset = db.getDataset(datasetId)
        if dataset is None:
            return setErrorResponse(404,"not found")

        # check access permission
        if not userCanAccessDataset(user_info, dataset):
            return setErrorResponse(401,"unauthorized user")

        dataset["editablePropertiesByTheUser"] = getEditablePropertiesByTheUser(user_info, dataset)

        studies, total = db.getStudiesFromDataset(datasetId, limit, skip)
        if not 'v2' in bottle.request.params:  
            # transitional param while clients change to the new reponse type
            dataset["studies"] = studies
        else:
            dataset["studies"] = {
                "total": total,
                "returned": len(studies),
                "skipped": skip,
                "limit": limit,
                "list": studies 
            }

    bottle.response.content_type = "application/json"
    return json.dumps(dataset)

def createZenodoDeposition(db, dataset):
    if dataset["pids"]["urls"]["zenodoDoi"] is None: 
        datasetId = dataset["id"]
        studies, total = db.getStudiesFromDataset(datasetId)
        newValue = pid.getZenodoDOI(CONFIG.zenodo.url, CONFIG.zenodo.access_token, dataset, studies, 
                                    CONFIG.zenodo.dataset_link_format, CONFIG.zenodo.community, CONFIG.zenodo.grant)
        db.setZenodoDOI(datasetId, newValue)

def updateZenodoDeposition(db, dataset):
    pidUrl = dataset["pids"]["urls"]["zenodoDoi"]
    if pidUrl != None: 
        i = pidUrl.rfind('.') + 1
        depositionId = pidUrl[i:]
        pid.updateZenodoDeposition(CONFIG.zenodo.url, CONFIG.zenodo.access_token, dataset, 
                                   CONFIG.zenodo.dataset_link_format, CONFIG.zenodo.community, CONFIG.zenodo.grant, 
                                   depositionId)

@app.route('/api/datasets/<id>', method='PATCH')
def patchDataset(id):
    LOG.debug("Received PATCH /dataset/%s" % id)
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret # can be None
    if user_info is None:
        return setErrorResponse(401,"unauthorized user")
    userId = user_info["sub"]
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
        if property not in getEditablePropertiesByTheUser(user_info, dataset):
            return setErrorResponse(400,"the property is not in the editablePropertiesByTheUser list")

        if property == "draft":
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
                invalidate_dataset(CONFIG.self.datasets_mount_path, datasetDirName)              
            trace_details = "INVALIDATE" if bool(newValue) else "REACTIVATE"
        elif property == "name":
            db.setDatasetName(datasetId, str(newValue))
        elif property == "description":
            db.setDatasetDescription(datasetId, str(newValue))
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
            tracer.traceDatasetUpdate(CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url, 
                                      datasetId, userId, trace_details)
    bottle.response.status = 200

@app.route('/api/datasets', method='GET')
def getDatasets():
    LOG.debug("Received GET /datasets")
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret # can be None

    searchFilter = Search_filter(draft = None, public = None, invalidated = None)
    if 'draft' in bottle.request.params:
        searchFilter.draft = bool(bottle.request.params['draft'])
    if 'public' in bottle.request.params:
        searchFilter.public = bool(bottle.request.params['public'])
    if 'invalidated' in bottle.request.params:
        searchFilter.invalidated = bool(bottle.request.params['invalidated'])
    #authorId
    searchFilter = adjustSearchFilterByUser(user_info, searchFilter)

    skip = int(bottle.request.params['skip']) if 'skip' in bottle.request.params else 0
    limit = int(bottle.request.params['limit']) if 'limit' in bottle.request.params else 30
    if skip < 0: skip = 0
    if limit < 0: limit = 0
    searchString = str(bottle.request.params['searchString']).strip() if 'searchString' in bottle.request.params else ""
    
    with DB(CONFIG.db) as db:
        datasets, total = db.getDatasets(skip, limit, searchString, searchFilter)
        response = {
            "total": total,
            "returned": len(datasets),
            "skipped": skip,
            "limit": limit,
            "list": datasets 
        }

    bottle.response.content_type = "application/json"
    if not 'v2' in bottle.request.params:  
        # transitional param while clients change to the new reponse type
        return json.dumps(response["list"])
    return json.dumps(response)


@app.route('/api/licenses', method='GET')
def getLicenses():
    LOG.debug("Received GET /licenses")
    ok, ret = checkAuthorizationHeader()
    if not ok: return ret
    else: user_info = ret # can be None

    with DB(CONFIG.db) as db:
        licenses = db.getLicenses()

    bottle.response.content_type = "application/json"
    return json.dumps(licenses)


@app.route('/api/user/<userName>', method='POST')
def postUser(userName):
    # Transitional condition while clients change to new PUT operation
    LOG.debug("Received (Transitional) POST %s" % bottle.request.path)
    return putUser(userName)

@app.route('/api/users/<userName>', method='PUT')
def putUser(userName):
    LOG.debug("Received PUT %s" % bottle.request.path)
    ok, ret = checkAuthorizationHeader(serviceAccount=True)
    if not ok: return ret
    else: user_info = ret # can be None

    if not userCanAdminUsers(user_info):
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

@app.route('/api/users/<userName>', method='GET')
def getUser(userName):
    LOG.debug("Received GET %s" % bottle.request.path)
    ok, ret = checkAuthorizationHeader(serviceAccount=True)
    if not ok: return ret
    else: user_info = ret # can be None

    if not userCanAdminUsers(user_info):
        return setErrorResponse(401,"unauthorized user")
    
    with DB(CONFIG.db) as db:
        userId, userGid = db.getUserIDs(userName)

    if userGid is None: return setErrorResponse(404, "not found")

    bottle.response.content_type = "application/json"
    return json.dumps(dict(gid = userGid))


def checkDatasetListAccess(datasetIDs, userName):
    badIDs = []
    with DB(CONFIG.db) as db:
        userId, userGID = db.getUserIDs(userName)
        userGroups = db.getUserGroups(userName)
        for id in datasetIDs:
            dataset = db.getDataset(id)
            if dataset is None:
                # invalidated or not exists
                badIDs.append(id)
            elif not tmpUserCanAccessDataset(userId, userGroups, dataset):
                badIDs.append(id)
    return badIDs

@app.route('/api/datasetAccessCheck', method='POST')
def postDatasetAccessCheck():
    LOG.debug("Received POST %s" % bottle.request.path)
    ok, ret = checkAuthorizationHeader(serviceAccount=True)
    if not ok: return ret
    else: user_info = ret # can be None

    if not userCanAdminDatasetAccess(user_info):
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
    else: user_info = ret # can be None

    if not userCanAdminDatasetAccess(user_info):
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
            userId, userGID = db.getUserIDs(userName)
            if CONFIG.self.datasets_mount_path != '':
                for id in datasetIDs:
                    studies, total = db.getStudiesFromDataset(id)
                    LOG.debug('Setting ACLs in dataset %s for GID %s ...' % (id, userGID))
                    datasetDirName = id
                    give_access_to_dataset(CONFIG.self.datasets_mount_path, datasetDirName, CONFIG.self.datalake_mount_path, studies, userGID)

            db.createDatasetAccess(datasetAccessId, datasetIDs, userGID, toolName, toolVersion)
            if CONFIG.tracer.url != '':
                # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
                tracer.traceDatasetsAccess(CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url, 
                                          datasetIDs, userId, toolName, toolVersion)
        
        LOG.debug('Dataset access granted.')
        bottle.response.status = 201

    except Exception as e:
        LOG.exception(e)
        LOG.error("May be the body of the request is wrong: %s" % read_data)
        return setErrorResponse(500,"Unexpected error, may be the input is wrong")

@app.route('/api/datasetAccess/<id>', method='DELETE')
def deleteDatasetAccess(id):
    LOG.debug("Received DELETE %s" % bottle.request.path)
    return setErrorResponse(404, "Not implemented, not yet")

# Routes are evaluated in the order they were defined.
# So this is to send appropiate error to unknown operations but with the /api prefix.
# Just to not continue evaluating the rest of routes of front-end.
@app.route('/api/<any_path:re:.+>', method='GET')
def getUnknown(any_path):
    LOG.debug("Received unknown %s" % bottle.request.path)
    return setErrorResponse(404, "Not found '%s'" % bottle.request.path)


# ================
# Front-end routes
# ================

# static files (any route that ends with '.' + known extension, including subpaths)
# To avoid conflicts, static files prefixed with /web/
@app.route('/web/<file_path:re:.*\.(html|js|json|txt|map|css|jpg|png|gif|ico|svg)>', method='GET')
def getStaticFileWeb(file_path):
    LOG.debug("Received GET %s" % bottle.request.path)
    LOG.debug("Static file (web): "+file_path)
    return bottle.static_file(file_path, CONFIG.self.static_files_dir_path)

# Routes are evaluated in the order they were defined.
# So this is to send appropiate error to missing file but with the /web prefix.
# Just to not continue evaluating the rest of routes.
@app.route('/web/<any_path:re:.+>', method='GET')
def getUnknown(any_path):
    LOG.debug("Received unknown %s" % bottle.request.path)
    return setErrorResponse(404, "Not found '%s'" % bottle.request.path)

# temporal for backward compatibility
@app.route('/<file_path:re:.*\.(html|js|json|txt|map|css|jpg|png|gif|ico|svg)>', method='GET')
def getStaticFile(file_path):
    LOG.debug("Received GET %s" % bottle.request.path)
    LOG.debug("Static file: "+file_path)
    return bottle.static_file(file_path, CONFIG.self.static_files_dir_path)

# Any other path (without prefix /api/ or /web/) will be responded with the index.html content,
# index.html loads a javascript interface that manage those other paths.
@app.route('/', method='GET')
@app.route('/<any_path:re:.+>', method='GET')
def getWebUI(any_path=''):
    LOG.debug("Received GET /" + any_path)
    LOG.debug("Routed to index.html")
    return bottle.static_file('index.html', CONFIG.self.static_files_dir_path)

