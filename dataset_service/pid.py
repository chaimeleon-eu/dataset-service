from codecs import encode
import logging
import urllib.parse
import http.client
import json
import io
from xhtml2pdf import pisa

# REST API spec: https://developers.zenodo.org/

class PidException(Exception):
    pass
    
def _getDepositionMetadata(dataset, creator, dataset_link_format, community: str = '', grant: str = ''):
    dataset_link = dataset_link_format % dataset["id"]
    communities = []
    if community != '': communities.append({'identifier': community})
    grants = []
    if grant != '': grants.append({'id': grant})
    return {
        'metadata': {
            'upload_type': 'other',
            'title': dataset["name"],
            'description': '''
                <p>Selection of studies for dataset. ID: <a href="%s">%s</a><br />
                Use the link on the dataset ID to access the full contents in a computation platform.
                </p>
                ''' % (dataset_link, dataset["id"]),
            'creators': [{'name': creator}], 
            'access_right': 'open' if dataset["public"] else 'closed',
            'license': 'cc-by',
            'related_identifiers': [
                {'identifier': dataset_link, 
                 'relation': 'describes', 
                 'resource_type': 'dataset'}],
            'communities': communities,
            'grants': grants
        }
    }

def _createDeposition(connection, url_path, accessToken, dataset, creator, dataset_link_format, community, grant):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    headers['Content-Type'] = 'application/json'  # 'application/json;charset=UTF-8'
    body = _getDepositionMetadata(dataset, creator, dataset_link_format, community, grant)
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
    deposition_id = str(response["id"])
    return bucket_url, deposition_id

def _updateDeposition(connection, url_path, accessToken, dataset, creator, dataset_link_format, community, grant, depositionId):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    headers['Content-Type'] = 'application/json'  # 'application/json;charset=UTF-8'
    body = _getDepositionMetadata(dataset, creator, dataset_link_format, community, grant)
    payload = json.dumps(body)
    logging.root.debug("BODY: " + payload)
    connection.request("PUT", url_path + "api/deposit/depositions/"+depositionId, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200:
        logging.root.error('Zenodo error. Code: %d %s' % (httpStatusCode, res.reason))
        logging.root.error(msg)
        raise PidException('Internal server error: Zenodo call to update deposition failed.')

def _uploadFile(connection, bucket_path, accessToken, fileName, fileContent):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    headers['Content-Type'] = 'application/octet-stream'
    payload = encode(fileContent) if isinstance(fileContent, str) else fileContent
    connection.request("PUT", bucket_path + "/" + fileName, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 200 and httpStatusCode != 201:
        logging.root.error('Zenodo error. Code: %d %s' % (httpStatusCode, res.reason))
        logging.root.error(msg)
        raise PidException('Internal server error: Zenodo call to upload failed.')

def _publishDeposition(connection, url_path, accessToken, deposition_id):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    payload = None
    url = url_path+"api/deposit/depositions/"+deposition_id+"/actions/publish"
    connection.request("POST", url, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 202:
        logging.root.error('Zenodo error. Code: %d %s' % (httpStatusCode, res.reason))
        logging.root.error(msg)
        raise PidException('Internal server error: Zenodo call to publish failed.')

    response = json.loads(msg)
    return response["doi_url"]
 
def _setEditableDeposition(connection, url_path, accessToken, deposition_id):
    headers = {}
    headers['Authorization'] = 'Bearer ' + accessToken
    payload = None
    url = url_path+"api/deposit/depositions/"+deposition_id+"/actions/edit"
    connection.request("POST", url, payload, headers)
    res = connection.getresponse()
    httpStatusCode = res.status
    msg = res.read()  # whole response must be readed in order to do more requests using the same connection
    if httpStatusCode != 201:
        logging.root.error('Zenodo error. Code: %d %s' % (httpStatusCode, res.reason))
        logging.root.error(msg)
        raise PidException('Internal server error: Zenodo call to setEditable failed.')

def _generateDescriptionHtml(dataset, dataset_link_format):
    dataset_link = dataset_link_format % dataset["id"]
    ageRangeStr = "-"
    if dataset["ageLow"] != None:
        ageRangeStr = "Between %d %s and %d %s" % (dataset["ageLow"], dataset["ageUnit"][0], 
                                                   dataset["ageHigh"], dataset["ageUnit"][1])
    htmlStr = '''
        <html>
            <head>
                <style type="text/css">
                    body { font-size: 12px; }
                </style>
            </head>
            <body>
                %s
                <p>---</p>
                <p>
                <strong>ID: </strong>%s</a><br />
                <strong>URL: </strong><a href="%s">%s</a><br />
                <strong>Creation date: </strong>%s<br />
                <strong>License: </strong>%s [%s]<br />
                <strong>Contact info.: </strong>%s<br />
                ---<br />
                <strong>Studies count: </strong>%d<br />
                <strong>Subjects count: </strong>%d<br />
                <strong>Age range: </strong>%s<br />
                <strong>Sex: </strong>%s<br />
                <strong>Body part(s): </strong>%s<br />
                <strong>Modality: </strong>%s<br />
                </p>
            </body>
        </html>
        ''' % (dataset["description"], dataset["id"], dataset_link, dataset_link, 
                dataset["creationDate"], dataset["license"]["title"], dataset["license"]["url"], 
                dataset["contactInfo"], dataset["studiesCount"], dataset["subjectsCount"],
                ageRangeStr,
                ', '.join(dataset["sex"]), 
                ', '.join(dataset["bodyPart"]), 
                ', '.join(dataset["modality"]))
    return htmlStr

def _generateDescriptionPdf(dataset, dataset_link_format):
    htmlString = _generateDescriptionHtml(dataset, dataset_link_format)
    pdf = io.BytesIO()
    pisa_status = pisa.CreatePDF(htmlString, dest=pdf)
    return pdf.getvalue()   # bytes

def _generateIndexJson(studies): 
    index = []
    # Only some properties will copied to the index for zenodo
    for study in studies:
        index.append(dict(studyId = study["studyId"],
                          studyName = study["studyName"],
                          subjectName = study["subjectName"],
                          url = study["url"], 
                          series = study["series"]))
    # And dump to the outputString
    return json.dumps(index)

def getZenodoDOI(url, accessToken, dataset, studies, author, dataset_link_format, community, grant):
    zenodo = urllib.parse.urlparse(url)
    if zenodo.hostname is None: raise PidException('Wrong url.')
    if accessToken == "": raise PidException('Empty Zenodo access token, check the project configuration.')
    connection = http.client.HTTPSConnection(zenodo.hostname, zenodo.port)
    try:
        logging.root.debug('Creating deposition in Zenodo...')
        bucket_url, deposition_id = _createDeposition(connection, zenodo.path, accessToken, dataset, author,
                                                      dataset_link_format, community, grant)
        # bucket_url example: "https://zenodo.org/api/files/568377dd-daf8-4235-85e1-a56011ad454b"
        logging.root.debug('Zenodo creation of deposition success.')
        bucket = urllib.parse.urlparse(bucket_url)

        logging.root.debug('Uploading description file...')
        descriptionFileBytes = _generateDescriptionPdf(dataset, dataset_link_format)
        # previous connection can be used: host and port in bucket_url should be the same 
        _uploadFile(connection, bucket.path, accessToken, "description.pdf", descriptionFileBytes)
        logging.root.debug('Zenodo uploading success.')

        # logging.root.debug('Uploading index.json...')
        # indexFileJsonContentStr = _generateIndexJson(studies)
        # # previous connection can be used: host and port in bucket_url should be the same 
        # _uploadFile(connection, bucket.path, accessToken, "index.json", indexFileJsonContentStr)
        # logging.root.debug('Zenodo uploading success.')

        logging.root.debug('Publishing deposition in Zenodo...')
        doi_url = _publishDeposition(connection, zenodo.path, accessToken, deposition_id)
        logging.root.debug('Zenodo deposition published successfully.')
    finally:
        connection.close()
    return doi_url

def updateZenodoDeposition(url, accessToken, dataset, author, dataset_link_format, community, grant, deposition_id):
    zenodo = urllib.parse.urlparse(url)
    if zenodo.hostname is None: raise PidException('Wrong url.')
    if accessToken == "": raise PidException('Empty Zenodo access token, check the project configuration.')
    connection = http.client.HTTPSConnection(zenodo.hostname, zenodo.port)
    try:
        logging.root.debug('Unlocking deposition in Zenodo (changing to editable mode)...')
        _setEditableDeposition(connection, zenodo.path, accessToken, deposition_id)
        logging.root.debug('Zenodo unlock of deposition success.')

        logging.root.debug('Updating deposition in Zenodo...')
        _updateDeposition(connection, zenodo.path, accessToken, dataset, author,
                        dataset_link_format, community, grant, deposition_id)
        logging.root.debug('Zenodo updating of deposition success.')
        
        logging.root.debug('Publishing deposition in Zenodo...')
        doi_url = _publishDeposition(connection, zenodo.path, accessToken, deposition_id)
        logging.root.debug('Zenodo deposition published successfully.')
    finally:
        connection.close()