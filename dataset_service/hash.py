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

def _updateSHAWithDirectoryContents(sha, dirPath, notifyProgress = None):
    filesList = os.listdir(dirPath)
    # The order is important to obtain the correct hash.
    # Also recomendable: filter by extension (.dcm) if there are other files that can change.
    filesList.sort()
    for name in filesList:
        filePath = os.path.join(dirPath, name)
        if os.path.isdir(filePath): _updateSHAWithDirectoryContents(sha, filePath, notifyProgress)
        else:                       _updateSHAWithFile(sha, filePath)
        if notifyProgress != None: 
            stop = notifyProgress('')
            if stop: return

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

def _getHashOfDirectory(dirPath, notifyProgress = None):
    sha = _createNewSHA()
    _updateSHAWithDirectoryContents(sha, dirPath, notifyProgress)
    if notifyProgress != None: 
        stop = notifyProgress('')
        if stop: return None
    return sha.digest()

def _getHashOfSeries(seriesDirPath, notifyProgress = None):
    return _getHashOfDirectory(seriesDirPath, notifyProgress)

def _getHashOfStudy(seriesList, studyDirPath, notifyProgress = None):
    sha = _createNewSHA()
    for series in seriesList:
        seriesDirPath = os.path.join(studyDirPath, series["folderName"])
        seriesHash = _getHashOfSeries(seriesDirPath, notifyProgress)
        if seriesHash is None: return None
        sha.update(seriesHash)
    return sha.digest()

def _getHashOfDatasetImages(datasetDirPath, studies, studiesHashes = None, notifyProgress = None):
    sha = _createNewSHA()
    total = len(studies)
    count = 0
    for study in studies:
        count += 1
        studyDirPath = os.path.join(datasetDirPath, study['path'])
        logging.root.debug('Calculating SHA of study (%d/%d) [%s] ...' % (count, total, studyDirPath))
        if notifyProgress != None and (count == 1 or count % 2 == 0):
            notifyProgress('Calculating SHA of study %d of %d) ...' % (count, total), log=False)
        studyHash = _getHashOfStudy(study["series"], studyDirPath, notifyProgress)
        if studyHash is None: return None
        if studiesHashes != None: studiesHashes.append(dict(studyId = study["studyId"], 
                                                            hash = _bytesToBase64String(studyHash)))
        sha.update(studyHash)
    return sha.digest()

def getHashOfString(s):
    return _bytesToBase64String(_getHashOfString(s))

def getHashOfFile(filePath):
    return _bytesToBase64String(_getHashOfFile(filePath))

def getHashOfSeries(serieDirPath):
    seriesHash = _getHashOfSeries(serieDirPath)
    if seriesHash is None: raise Exception()
    return _bytesToBase64String(seriesHash)

def getHashOfDatasetImages(datasetDirPath, studies, studiesHashes = None, notifyProgress = None):
    imagesHash = _getHashOfDatasetImages(datasetDirPath, studies, studiesHashes, notifyProgress)
    if imagesHash is None: return None
    return _bytesToBase64String(imagesHash)

def getHashesOfDataset(datasetDirPath, indexFileName, eformsFileName, studies = None, studiesHashes = None, notifyProgress = None):
    '''
    "studies" is an optional array (just for optimization), if it is None, the studies will be read from the index file.
    "studiesHashes" is an optional (empty) array that will be filled with the ids and hashes of studies.
    "notifyProgress" is an optional function which accepts one arg of type str.
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

    imagesHash = getHashOfDatasetImages(datasetDirPath, studies, studiesHashes, notifyProgress)
    if imagesHash is None: return None, None, None
    clinicalDataHash = getHashOfFile(eformsFilePath)
    return indexHash, imagesHash, clinicalDataHash

