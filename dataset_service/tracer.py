import os
import logging
import urllib.parse
import urllib.error
import http.client
import json
import time
from dataset_service import auth, hash

class TraceException(Exception):
    pass
    
def check_connection(authClient: auth.AuthClient, tracerUrl):
    logging.root.info("Checking connection to tracer-service on %s..." % tracerUrl)
    try:
        retries = 0
        while retries < 5:
            try:
                hash_codes = getSupportedHashAlgorithms(authClient, tracerUrl)
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

def getSupportedHashAlgorithms(authClient: auth.AuthClient, tracerUrl):
    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)
    headers = {}
    headers['Authorization'] = 'bearer ' + authClient.get_token()
    # headers['Authorization'] = 'Basic XXXXXXXXXXXX'
    # logging.root.debug("GET: " + tracer.path + "api/v1/traces/hashes")
    try:
        connection.request("GET", tracer.path + "api/v1/traces/hashes", "", headers)
        res = connection.getresponse()
        httpStatusCode = res.status
        msg = res.read()
    finally: 
        connection.close()
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

def _getResources(datasetDirPath, indexFileName, eformsFileName, hashOperator: hash.datasetHashesOperator, 
                  studies = None, studiesHashes = None, notifyProgress = None):
    resources = []
    logging.root.debug('Calculating SHAs...')
    indexHash, imagesHash, clinicalDataHash = hashOperator.getHashesOfDataset(datasetDirPath, indexFileName, eformsFileName, 
                                                                              studies, studiesHashes, notifyProgress)
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


def traceDatasetCreation(authClient: auth.AuthClient, tracerUrl, datasetDirPath, indexFileName, eformsFileName, 
                         datasetId, userId, hashOperator, datasetStudies = None, studiesHashes = None, notifyProgress = None):
    '''
    "studiesHashes" is an optional array that will be filled with the hashes of studies.
    "notifyProgress" is an optional function which accepts one arg of type str.
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
        resources = _getResources(datasetDirPath, indexFileName, eformsFileName, hashOperator, datasetStudies, studiesHashes, notifyProgress)
    )
    # if dataset["previousId"] != None:
    #     body['userAction'] = 'CREATE_VERSION_DATASET'
    #     body['previousId'] = dataset["previousId"]
    
    if notifyProgress != None: 
        stop = notifyProgress('Notifying the creation to the Tracer...')
        if stop: return

    payload = json.dumps(body)
    headers['Authorization'] = 'bearer ' + authClient.get_token()
    # headers['Authorization'] = 'Basic XXXXXXXXXXXX'

    logging.root.debug("Calling tracer...")
    logging.root.debug("BODY: " + payload)
    logging.root.debug("============================")
    try:
        connection.request("POST", tracer.path + "api/v1/traces", payload, headers)
        res = connection.getresponse()
        httpStatusCode = res.status
        msg = res.read()
    finally:
        connection.close()
    if httpStatusCode != 204 and httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    else:
        logging.root.debug('Tracer call success.')
        #response = json.loads(msg)
        #print(response)

def getOriginalResourcesFromTracer(authClient: auth.AuthClient, tracerUrl, datasetId):
    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)
    headers = {}
    headers['Authorization'] = 'bearer ' + authClient.get_token()
    payload = ""
    # get original resources from Tracer
    logging.root.debug('Getting trace from Tracer (datsetId: %s)...' % datasetId)
    try:
        connection.request("GET", tracer.path + "api/v1/traces?datasetId=" + datasetId + "&userAction=CREATE_DATASET", payload, headers)
        res = connection.getresponse()
        httpStatusCode = res.status
        msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    except Exception as ex:
        connection.close()
        raise ex
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
    try:
        res = connection.getresponse()
        httpStatusCode = res.status
        msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    finally:
        connection.close()
    if httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    logging.root.debug('Tracer call success.')
    response = json.loads(msg)
    try:
        trace = response['traces'][0]['trace']
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

def checkStudiesHashes(studiesHashes0, studiesHashes):
    goodCount = 0
    wrongCount = 0
    for s in studiesHashes:
        studyId = s['studyId']
        if s['hash'] == studiesHashes0[studyId]:
            goodCount += 1
            if goodCount < 4: logging.root.info('Good study: '+studyId)
        else:
            wrongCount += 1
            if wrongCount < 4: logging.root.info('Wrong study: '+studyId)

    logging.root.info('%d/%d studies wrong' % (wrongCount, len(studiesHashes)))
    

def checkDatasetIntegrity(authClient: auth.AuthClient, tracerUrl, datasetId, datasetDirPath, indexFileName, eformsFileName, 
                          studiesHashes0, hashOperator: hash.datasetHashesOperator) -> str | None:
    indexHash0, imagesHash0, clinicalDataHash0 = getOriginalResourcesFromTracer(authClient, tracerUrl, datasetId)
    studiesHashes = []
    indexHash, imagesHash, clinicalDataHash = hashOperator.getHashesOfDataset(datasetDirPath, indexFileName, eformsFileName, studiesHashes=studiesHashes)
    if not checkHash(clinicalDataHash0, clinicalDataHash, 'clinicalData'): return 'clinicalData'
    if not checkHash(indexHash0,        indexHash,        'index'):        return 'index'
    if not checkHash(imagesHash0,       imagesHash,       'images'):       
        checkStudiesHashes(studiesHashes0, studiesHashes)
        return 'images'
    logging.root.info('Dataset integrity OK.')
    return None


def traceDatasetsAccess(authClient: auth.AuthClient, tracerUrl, datasetsIds, userId, toolName, toolVersion):
    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)

    headers = {}
    headers['Authorization'] = 'bearer ' + authClient.get_token()
    headers['Content-Type'] = 'application/json;charset=UTF-8'
    body = dict(
        userId = userId,
        userAction = 'USE_DATASETS',
        datasetsIds = datasetsIds,
        toolName = toolName,
        toolVersion = toolVersion )

    payload = json.dumps(body)
    logging.root.debug("BODY: " + payload)
    try:
        connection.request("POST", tracer.path + "api/v1/traces", payload, headers)
        res = connection.getresponse()
        httpStatusCode = res.status
        msg = res.read()
    finally:
        connection.close()
    if httpStatusCode != 204 and httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    else:
        logging.root.debug('Tracer call success.')
        #response = json.loads(msg)
        #print(response)
    
def traceDatasetUpdate(authClient: auth.AuthClient, tracerUrl, datasetId, userId, updateDetails):
    tracer = urllib.parse.urlparse(tracerUrl)
    if tracer.hostname is None: raise Exception('Wrong tracerUrl.')
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)

    headers = {}
    headers['Authorization'] = 'bearer ' + authClient.get_token()
    headers['Content-Type'] = 'application/json;charset=UTF-8'
    body = dict(
        userId = userId,
        userAction = 'UPDATE_DATASET',
        datasetId = datasetId,
        details = updateDetails )

    payload = json.dumps(body)
    logging.root.debug("BODY: " + payload)
    try:
        connection.request("POST", tracer.path + "api/v1/traces", payload, headers)
        res = connection.getresponse()
        httpStatusCode = res.status
        msg = res.read()
    finally:
        connection.close()
    if httpStatusCode != 204 and httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    else:
        logging.root.debug('Tracer call success.')
        #response = json.loads(msg)
        #print(response)

