import os
import base64
import hashlib
import json
import logging

def _createNewSHA():
    return hashlib.sha3_256()

def _updateSHAWithFile(sha, filePath: str):
    # We use a buffer to avoid load the complete file contents in memory, 
    # which can be a problem on big files.
    BUF_SIZE = 65536  # 64kb
    with open(filePath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data: break
            sha.update(data)

def _updateSHAWithDirectoryContents(sha, dirPath):
    filesList = os.listdir(dirPath)
    # The order is important to obtain the correct hash.
    # Also recomendable: filter by extension (.dcm) if there are other files that can change.
    filesList.sort()
    for name in filesList:
        filePath = os.path.join(dirPath, name)
        if os.path.isdir(filePath): _updateSHAWithDirectoryContents(sha, filePath)
        else:                       _updateSHAWithFile(sha, filePath)

def _bytesToBase64String(b: bytes) -> str:
    return base64.b64encode(b).decode('ascii')

def _getDigestFromSHA(sha):
    #return sha.hexdigest()    This is the common way, but tracer-service wants codified to base64
    return _bytesToBase64String(sha.digest())

def _getHashOfBytes(b: bytes) -> bytes:
    return hashlib.sha3_256(b).digest()
    # return hashlib.blake2s(b).digest()

def _getHashOfString(s: str) -> bytes:
    return _getHashOfBytes(bytes(s, 'utf-8'))

def _getHashOfFile(filePath: str) -> bytes:
    # Same result in one line, but requires loading the complete file content in memory, 
    # which can be a problem on big files.
    #return hashlib.sha256(open(filePath).read()).hexdigest()

    sha = _createNewSHA()
    _updateSHAWithFile(sha, filePath)
    return sha.digest()

def _getHashOfDirectory(dirPath):
    sha = _createNewSHA()
    _updateSHAWithDirectoryContents(sha, dirPath)
    return sha.digest()

def _getHashOfSerie(serieDirPath):
    return _getHashOfDirectory(serieDirPath)

def _getHashOfStudy(series, studyDirPath):
    sha = _createNewSHA()
    for serie in series:
        serieDirPath = os.path.join(studyDirPath, serie["folderName"])
        serieHash = _getHashOfSerie(serieDirPath)
        sha.update(serieHash)
    return sha.digest()

def _getHashOfDatasetImages(datasetDirPath, studies, hashes = None ):
    sha = _createNewSHA()
    total = len(studies)
    count = 0
    for study in studies:
        count += 1
        studyDirPath = os.path.join(datasetDirPath, study['path'])
        logging.root.debug('Calculating SHA of study (%d/%d) [%s] ...' % (count, total, studyDirPath))
        studyHash = _getHashOfStudy(study["series"], studyDirPath)
        if hashes != None: hashes.append(_bytesToBase64String(studyHash))
        sha.update(studyHash)
    return sha.digest()

def getHashOfString(s):
    return _bytesToBase64String(_getHashOfString(s))

def getHashOfFile(filePath):
    return _bytesToBase64String(_getHashOfFile(filePath))

def getHashOfSerie(serieDirPath):
    return _bytesToBase64String(_getHashOfSerie(serieDirPath))

def getHashOfDatasetImages(datasetDirPath, studies, hashes = None):
    return _bytesToBase64String(_getHashOfDatasetImages(datasetDirPath, studies, hashes))

def getHashesOfDataset(datasetDirPath, indexFileName, eformsFileName, studies = None, hashes = None):
    '''
    "studies" is an optional array (just for optimization), if it is None, the studies will be read from the index file.
    "hashes" is an optional (empty) array that will be filled with the hashes of studies.
    '''
    indexFilePath = os.path.join(datasetDirPath, indexFileName)
    eformsFilePath = os.path.join(datasetDirPath, eformsFileName)
    if studies is None:
        # we have to read studies from index file
        with open(indexFilePath, 'rb') as f:
            contentBytes = f.read()
        studies = json.loads(contentBytes)
        indexHash = _bytesToBase64String(_getHashOfBytes(contentBytes))
    else: 
        indexHash = getHashOfFile(indexFilePath)

    imagesHash = getHashOfDatasetImages(datasetDirPath, studies, hashes)
    clinicalDataHash = getHashOfFile(eformsFilePath)
    return indexHash, imagesHash, clinicalDataHash

