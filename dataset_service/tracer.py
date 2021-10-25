from array import array
import logging
import urllib.parse
import http.client
import json
import base64

class TraceException(Exception):
    pass
    
def login(oidc_url, client_id, client_secret):
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

def traceDatasetCreation(authUrl, clientId, clientSecret, tracerUrl, dataset, userId):
    token = login(authUrl, clientId, clientSecret)

    tracer = urllib.parse.urlparse(tracerUrl)
    connection = http.client.HTTPSConnection(tracer.hostname, tracer.port)

    headers = {'Authorization': 'bearer ' + token}
    headers = {'Content-Type': 'application/json;charset=UTF-8'}
    body = dict(
        userId = userId, 
        userAction = 'CREATE_DATASET', 
        datasetId = dataset["id"],
        resources = [
            dict(id = 'datasetList',
                 contentType = 'FILE_DATA',       # contentTypes: FILE_DATA, HTTP_FTP, HASH
                 name = 'datasetList',
                 resourceType = 'IMAGING_DATA',
                 data = ''),
            dict(id = 'clinicalData',
                 contentType = 'FILE_DATA',
                 name = 'clinicalData',
                 resourceType = 'PATIENT_INFO',
                 data = '')
        ])
    if dataset["previousId"] != None:
        body['userAction'] = 'CREATE_VERSION_DATASET'
        body['previousId'] = dataset["previousId"]

    studiesListStr = ''
    for study in dataset["studies"]:
        studiesListStr += study["studyId"] + ","
    body['resources']['datasetList']['data'] = base64.b64encode(bytes(studiesListStr, 'utf-8'))
    payload = json.dumps(body)
    connection.request("POST", tracer.path, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200:
        logging.root.error('Tracer error. Code: %d %s' % (httpStatusCode, res.reason))
        raise TraceException('Internal server error: tracer call failed.')
    else:
        logging.root.debug('Tracer call success.')
        #response = json.loads(msg)
        #print(response)
