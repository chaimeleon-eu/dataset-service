import os
import logging
import urllib.parse
import http.client
import json
import base64
import hashlib

class TraceException(Exception):
    pass
    
def login(oidc_url, client_id, client_secret):
    logging.root.debug("Logging into the tracer...")
    auth = urllib.parse.urlparse(oidc_url)
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

def getHashOfString(s: str) -> str:
    return base64.b64encode(hashlib.sha3_256(bytes(s, 'utf-8')).digest()).decode('ascii')
    # return base64.b64encode(hashlib.blake2s(bytes(s, 'utf-8')).digest()).decode('ascii')

def getHashOfFile(filePath: str) -> str:
    # Same result in one line, but requires loading the complete file content in memory, 
    # which can be a problem on big files.
    #return hashlib.sha256(open(filePath).read()).hexdigest()

    #Better using a buffer
    BUF_SIZE = 65536  # 64kb
    #sha = hashlib.sha256()
    # if sys.version_info < (3, 6): import sha3
    sha = hashlib.sha3_256()

    with open(filePath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data: break
            sha.update(data)

    #return sha.hexdigest()    This is common but tracer-service wants binary codified to base64
    return base64.b64encode(sha.digest()).decode('ascii')

def addFileAsResource(body, filePath, id):
    body['resources'].append(
        dict(id = id,
             contentType = 'HASH',
             hashType = 'SHA3_256',
             hash = getHashOfFile(filePath)))

def addFilesFromDirectoryAsResources(body, datasetDirPath, dirRelativePath):
    dirPath = os.path.join(datasetDirPath, dirRelativePath)
    for name in os.listdir(dirPath):
        filePath = os.path.join(dirPath, name)
        fileRelativePath = os.path.join(dirRelativePath, name)
        if os.path.isdir(filePath): addFilesFromDirectoryAsResources(body, datasetDirPath, fileRelativePath)
        else:                       addFileAsResource(body, filePath, getHashOfString(fileRelativePath))

def traceDatasetCreation(authUrl, clientId, clientSecret, tracerUrl, datasetsDirPath, datasetDirName, dataset, userId):
    tracer = urllib.parse.urlparse(tracerUrl)
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)
    headers = {}
    headers['Content-Type'] = 'application/json;charset=UTF-8'

    body = dict(
        userId = userId, 
        userAction = 'CREATE_DATASET', 
        datasetId = dataset["id"],
        resources = [
            dict(id = 'datasetList',
                 contentType = 'HASH',       # contentTypes: FILE_DATA, HTTP_FTP, HASH
                 hashType = 'SHA3_256',
                 hash = ''),
            dict(id = 'clinicalData',
                 contentType = 'HASH',
                 hashType = 'SHA3_256',
                 hash = '')
        ])
            # dict(id = 'clinicalData',
            #      contentType = 'FILE_DATA',
            #      name = 'clinicalData',
            #      resourceType = 'PATIENT_INFO',
            #      data = '')

    # if dataset["previousId"] != None:
    #     body['userAction'] = 'CREATE_VERSION_DATASET'
    #     body['previousId'] = dataset["previousId"]

    # studiesListStr = ''
    # for study in dataset["studies"]:
    #     studiesListStr += study["studyId"] + ","
    # body['resources']['datasetList']['data'] = base64.b64encode(bytes(studiesListStr, 'utf-8')).decode('ascii')

    # Calculate hashes
    logging.root.debug('Calculating SHAs...')
    datasetDirPath = os.path.join(datasetsDirPath, datasetDirName)
    for study in dataset["studies"]:
        for serie in study["series"]:
            serieRelativeDirPath = os.path.join(study['path'], serie)
            addFilesFromDirectoryAsResources(body, datasetDirPath, serieRelativeDirPath)

    # body['resources'][0]['data'] = base64.b64encode(bytes(json.dumps(dataset["studies"]), 'utf-8')).decode('ascii')  # list of studies
    # body['resources'][1]['data'] = base64.b64encode(bytes(json.dumps(dataset["subjects"]), 'utf-8')).decode('ascii')  # clinical data
    body['resources'][0]['hash'] = getHashOfString(json.dumps(dataset["studies"]))  # list of studies
    body['resources'][1]['hash'] = getHashOfString(json.dumps(dataset["subjects"]))  # clinical data

    payload = json.dumps(body)
    #logging.root.debug("BODY: " + payload)
    #logging.root.debug("============================")
    with open(os.path.join("/tmp", datasetDirName + ".json") , 'w') as outputStream:
        json.dump(body, outputStream)

    token = login(authUrl, clientId, clientSecret)
    headers['Authorization'] = 'bearer ' + token
    # headers['Authorization'] = 'Basic XXXXXXXXXXXX'

    logging.root.debug("Calling tracer...")
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


def traceDatasetsAccess(authUrl, clientId, clientSecret, tracerUrl, datasetsIds, userId, toolName, toolVersion):
    token = login(authUrl, clientId, clientSecret)

    tracer = urllib.parse.urlparse(tracerUrl)
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

