from array import array
from codecs import encode
import os
import logging
import urllib.parse
import http.client
import json
import base64
import hashlib

class PidException(Exception):
    pass
    
def createDeposition(connection, url_path, accessToken, dataset, dataset_link_format, community, grant):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    headers['Content-Type'] = 'application/json;charset=UTF-8'
    dataset_link = dataset_link_format % dataset["id"]
    body = {
        'metadata': {
            'upload_type': 'other',
            'title': dataset["name"],
            'description': '''
                <p>Selection of studies for dataset: 
                <a href="%s">%s</a><br />
                Use the link on the dataset ID to access the full contents in a computation platform.</p>
                <p>---</p>
                %s
                <p>---</p>
                <p>
                <strong>Studies count: </strong>%d<br />
                <strong>Subjects count: </strong>%d<br />
                <strong>Age range: </strong>Between %d %s and %d %s<br />
                <strong>Sex: </strong>%s<br />
                <strong>Body part(s): </strong>%s<br />
                <strong>Modality: </strong>%s<br />
                </p>
                ''' % (dataset_link, dataset["id"], dataset["description"], 
                       dataset["studiesCount"], dataset["subjectsCount"],
                       dataset["ageLow"], dataset["ageUnit"][0], dataset["ageHigh"], dataset["ageUnit"][1],
                       ' ,'.join(dataset["sex"])[:-2], 
                       ' ,'.join(dataset["bodyPart"])[:-2], 
                       ' ,'.join(dataset["modality"])[:-2]),
            'creators': [{'name': dataset["authorName"]}],
            'access_right': 'open',
            'license': 'cc-by',
            'related_identifiers': [
                {'identifier': dataset_link, 
                 'relation': 'describes', 
                 'resource_type': 'dataset'}],
            'communities': [{'identifier': community}],
            'grants': [{'id': grant}]
        }
    }

    payload = json.dumps(body)
    logging.root.debug("BODY: " + payload)
    connection.request("POST", url_path + "api/deposit/depositions", payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 201:
        logging.root.error('Zenodo error. Code: %d %s' % (httpStatusCode, res.reason))
        logging.root.error(msg)
        raise PidException('Internal server error: Zenodo call to create deposition failed.')

    response = json.loads(msg)
    bucket_url = response["links"]["bucket"]
    publish_url = response["links"]["publish"]
    return bucket_url, publish_url

def uploadFile(connection, bucket_path, accessToken, indexFileJsonContentStr):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    # headers['Content-Type'] = 'application/json;charset=UTF-8'
    # headers['Content-Type'] = 'text/plain'
    headers['Content-Type'] = 'application/octet-stream'
    payload = encode(indexFileJsonContentStr)
    #logging.root.debug(payload)
    connection.request("PUT", bucket_path + "/index.json", payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200 and httpStatusCode != 201:
        logging.root.error('Zenodo error. Code: %d %s' % (httpStatusCode, res.reason))
        logging.root.error(msg)
        raise PidException('Internal server error: Zenodo call to upload failed.')

def publishDeposition(connection, publish_path, accessToken):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    payload = None
    connection.request("POST", publish_path, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 202:
        logging.root.error('Zenodo error. Code: %d %s' % (httpStatusCode, res.reason))
        logging.root.error(msg)
        raise PidException('Internal server error: Zenodo call to publish failed.')

    response = json.loads(msg)
    return response["doi_url"]

def generateIndexJson(studies): 
    # Clear paths and urls, not relevant to Zenodo
    for study in studies:
        study["path"] = "-"
        study["url"] = "-"
    # And dump to the outputString
    return json.dumps(studies)

def getZenodoDOI(url, accessToken, dataset, studies, dataset_link_format, community, grant):
    zenodo = urllib.parse.urlparse(url)
    connection = http.client.HTTPSConnection(zenodo.hostname, zenodo.port)

    logging.root.debug('Creating deposition in Zenodo...')
    bucket_url, publish_url = createDeposition(connection, zenodo.path, accessToken, dataset, dataset_link_format, community, grant)
    # bucket_url example: "https://zenodo.org/api/files/568377dd-daf8-4235-85e1-a56011ad454b"
    logging.root.debug('Zenodo creation of deposition success.')

    logging.root.debug('Uploading index.json...')
    indexFileJsonContentStr = generateIndexJson(studies)
    bucket = urllib.parse.urlparse(bucket_url)
    # previous connection can be used: host and port in bucket_url should be the same 
    uploadFile(connection, bucket.path, accessToken, indexFileJsonContentStr)
    logging.root.debug('Zenodo uploading success.')

    logging.root.debug('Publishing deposition in Zenodo...')
    publish = urllib.parse.urlparse(publish_url)
    # previous connection can be used: host and port in bucket_url should be the same 
    doi_url = publishDeposition(connection, publish.path, accessToken)
    logging.root.debug('Zenodo deposition published successfully.')

    return doi_url
