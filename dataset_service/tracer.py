import os
import logging
import urllib.parse
import urllib.error
import http.client
import json
import time
from dataset_service import hash

class TraceException(Exception):
    pass
    
def check_connection(authUrl, clientId, clientSecret, tracerUrl):
    logging.root.info("Checking connection to tracer-service on %s..." % tracerUrl)
    try:
        retries = 0
        while retries < 5:
            try:
                hash_codes = getSupportedHashAlgorithms(authUrl, clientId, clientSecret, tracerUrl)
                logging.root.info("Accepted hash codes: %s" % json.dumps(hash_codes))
                break
            except urllib.error.HTTPError as e1:
                logging.root.error("HTTPError: " + str(e1.code) + " - " + str(e1.reason))
            except urllib.error.URLError as e2:
                logging.root.error("URLError: "+ str(e2.reason))
            retries += 1
            logging.root.info("Retrying in 5 seconds...")
            time.sleep(5)
        if retries >= 5: raise Exception("Unable to connect to tracer-service.")
    except Exception as e:
        logging.root.exception(e)
        raise e

def login(oidc_url, client_id, client_secret):
    logging.root.debug("Logging into the tracer...")
    auth = urllib.parse.urlparse(oidc_url)
    if auth.hostname is None: raise Exception('Wrong oidc_url.')
    connection = http.client.HTTPSConnection(auth.hostname, auth.port)

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = urllib.parse.urlencode({'client_id' : client_id, 'client_secret' : client_secret, 'grant_type': 'client_credentials'})
    connection.request("POST", auth.path, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200:
        logging.root.error('Tracer login error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer login failed.')
    else:
        logging.root.debug('Tracer login success.')
        response = json.loads(msg)
        #print(response)
        return response['access_token']

def getSupportedHashAlgorithms(authUrl, clientId, clientSecret, tracerUrl):
    token = login(authUrl, clientId, clientSecret)
    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)
    headers = {}
    headers['Authorization'] = 'bearer ' + token
    # headers['Authorization'] = 'Basic XXXXXXXXXXXX'
    # logging.root.debug("GET: " + tracer.path + "api/v1/traces/hashes")
    connection.request("GET", tracer.path + "api/v1/traces/hashes", "", headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer request failed.')
    else:
        response = json.loads(msg)
        return response


# def addFileAsResource(body, filePath, id):
#     body['resources'].append(
#         dict(id = id,
#              contentType = 'HASH',
#              hashType = 'SHA3_256',
#              hash = hash.getHashOfFile(filePath)))

# def addFilesFromDirectoryAsResources(body, datasetDirPath, dirRelativePath):
#     dirPath = os.path.join(datasetDirPath, dirRelativePath)
#     for name in os.listdir(dirPath):
#         filePath = os.path.join(dirPath, name)
#         fileRelativePath = os.path.join(dirRelativePath, name)
#         if os.path.isdir(filePath): addFilesFromDirectoryAsResources(body, datasetDirPath, fileRelativePath)
#         else:                       addFileAsResource(body, filePath, hash.getHashOfString(fileRelativePath))

def getResources(datasetDirPath, indexFileName, eformsFileName, studies = None, studiesHashes = None):
    resources = []
    logging.root.debug('Calculating SHAs...')
    indexHash, imagesHash, clinicalDataHash = hash.getHashesOfDataset(datasetDirPath, indexFileName, eformsFileName, studies, studiesHashes)
    resources.append(
        dict(id = 'index',
             contentType = 'HASH',       # contentTypes: FILE_DATA, HTTP_FTP, HASH
             hashType = 'SHA3_256',
             hash = indexHash)) 
    resources.append(
        dict(id = 'images',
             contentType = 'HASH',
             hashType = 'SHA3_256',
             hash = imagesHash))
    resources.append(
        dict(id = 'clinicalData',
                 contentType = 'HASH',
                 hashType = 'SHA3_256',
                 hash = clinicalDataHash))

    # for study in dataset["studies"]:
    #     for serie in study["series"]:
    #         serieRelativeDirPath = os.path.join(study['path'], serie)
    #         #addFilesFromDirectoryAsResources(body, datasetDirPath, serieRelativeDirPath)
    #         serieDirPath = os.path.join(datasetDirPath, serieRelativeDirPath)
    #         serieHash = hash.getHashOfSerie(serieDirPath)
    #         resources.append(
    #             dict(id = hash.getHashOfString(serieRelativeDirPath),
    #                 contentType = 'HASH',
    #                 hashType = 'SHA3_256',
    #                 hash = serieHash))
    return resources


def traceDatasetCreation(authUrl, clientId, clientSecret, tracerUrl, datasetDirPath, indexFileName, eformsFileName, 
                         datasetId, userId, datasetStudies = None, studiesHashes = None):
    '''
    "studiesHashes" is an optional array that will be filled with the hashes of studies
    '''
    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)
    headers = {}
    headers['Content-Type'] = 'application/json;charset=UTF-8'

    body = dict(
        userId = userId, 
        userAction = 'CREATE_DATASET', 
        datasetId = datasetId,
        resources = getResources(datasetDirPath, indexFileName, eformsFileName, datasetStudies, studiesHashes)
    )

    # if dataset["previousId"] != None:
    #     body['userAction'] = 'CREATE_VERSION_DATASET'
    #     body['previousId'] = dataset["previousId"]
    
    payload = json.dumps(body)

    token = login(authUrl, clientId, clientSecret)
    headers['Authorization'] = 'bearer ' + token
    # headers['Authorization'] = 'Basic XXXXXXXXXXXX'

    logging.root.debug("Calling tracer...")
    logging.root.debug("BODY: " + payload)
    logging.root.debug("============================")
    connection.request("POST", tracer.path + "api/v1/traces", payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 204 and httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    else:
        logging.root.debug('Tracer call success.')
        #response = json.loads(msg)
        #print(response)

def getOriginalResourcesFromTracer(authUrl, clientId, clientSecret, tracerUrl, datasetId):
    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)
    headers = {}
    token = login(authUrl, clientId, clientSecret)
    headers['Authorization'] = 'bearer ' + token
    payload = ""
    # get original resources from Tracer
    logging.root.debug('Getting trace from Tracer...')
    connection.request("GET", tracer.path + "api/v1/traces?datasetId=" + datasetId + "&userAction=CREATE_DATASET", payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    logging.root.debug('Tracer call success.')
    response = json.loads(msg)
    try:
        trace = response['traces'][0]['traces'][0]
        traceId = trace['id']
    except (Exception) as e:
        logging.root.error('Tracer response unexpected: %s' % (response))
        raise TraceException('Internal server error: tracer response unexpected.')

    logging.root.debug('Getting details of trace %s from Tracer...' % traceId)
    connection.request("GET", tracer.path + "api/v1/traces/" + traceId, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    logging.root.debug('Tracer call success.')
    response = json.loads(msg)
    try:
        trace = response['traces'][0]['traces'][0]
        if trace['datasetId'] != datasetId or trace['userAction'] != "CREATE_DATASET": raise Exception()
        indexHash = imagesHash = clinicalDataHash = None
        for resource in trace['traceResources']:
            if resource['id'] == 'index': indexHash = resource['contentHash']
            if resource['id'] == 'images': imagesHash = resource['contentHash']
            if resource['id'] == 'clinicalData': clinicalDataHash = resource['contentHash']
        return indexHash, imagesHash, clinicalDataHash
    except (Exception) as e:
        logging.root.error('Tracer response unexpected: %s' % (response))
        raise TraceException('Internal server error: tracer response unexpected.')

    

def checkHash(originalHash, currentHash, name):
    if originalHash != currentHash: 
        logging.root.error('Dataset resource hash mismatch: %s' % name)
        logging.root.error('Original (from Tracer): %s \nCalculated now: %s' % (originalHash, currentHash))
        return False
    return True

def checkDatasetIntegrity(authUrl, clientId, clientSecret, tracerUrl, datasetId, datasetDirPath, indexFileName, eformsFileName) -> str | None:
    indexHash0, imagesHash0, clinicalDataHash0 = getOriginalResourcesFromTracer(authUrl, clientId, clientSecret, tracerUrl, datasetId)
    indexHash, imagesHash, clinicalDataHash = hash.getHashesOfDataset(datasetDirPath, indexFileName, eformsFileName)
    if not checkHash(indexHash0,        indexHash,        'index'):        return 'index'
    if not checkHash(imagesHash0,       imagesHash,       'images'):       return 'images'
    if not checkHash(clinicalDataHash0, clinicalDataHash, 'clinicalData'): return 'clinicalData'
    logging.root.info('Dataset integrity OK.')
    return None


def traceDatasetsAccess(authUrl, clientId, clientSecret, tracerUrl, datasetsIds, userId, toolName, toolVersion):
    token = login(authUrl, clientId, clientSecret)

    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)

    headers = {}
    headers['Authorization'] = 'bearer ' + token
    headers['Content-Type'] = 'application/json;charset=UTF-8'
    body = dict(
        userId = userId,
        userAction = 'USE_DATASETS',
        datasetsIds = datasetsIds,
        toolName = toolName,
        toolVersion = toolVersion )

    payload = json.dumps(body)
    logging.root.debug("BODY: " + payload)
    connection.request("POST", tracer.path + "api/v1/traces", payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 204 and httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    else:
        logging.root.debug('Tracer call success.')
        #response = json.loads(msg)
        #print(response)
    
def traceDatasetUpdate(authUrl, clientId, clientSecret, tracerUrl, datasetId, userId, updateDetails):
    token = login(authUrl, clientId, clientSecret)

    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)

    headers = {}
    headers['Authorization'] = 'bearer ' + token
    headers['Content-Type'] = 'application/json;charset=UTF-8'
    body = dict(
        userId = userId,
        userAction = 'UPDATE_DATASET',
        datasetId = datasetId,
        details = updateDetails )

    payload = json.dumps(body)
    logging.root.debug("BODY: " + payload)
    connection.request("POST", tracer.path + "api/v1/traces", payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 204 and httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    else:
        logging.root.debug('Tracer call success.')
        #response = json.loads(msg)
        #print(response)

