import os
import base64
import hashlib
import json
import logging
from datetime import datetime
from .storage import DB, DBDatasetsOperator

class sha3:
    def __init__(self, b: bytes=b''):
        self._sha = hashlib.sha3_256(b)

    def updateWithFile(self, filePath: str):
        # We use a buffer to avoid load the complete file contents in memory, 
        # which can be a problem on big files.
        BUF_SIZE = 65536  # 64kb
        with open(filePath, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data: break
                self._sha.update(data)

    def updateWithDirectoryContents(self, dirPath, notifyProgress = None):
        filesList = os.listdir(dirPath)
        # The order is important to obtain the correct hash.
        # Also recomendable: filter by extension (.dcm) if there are other files that can change.
        filesList.sort()
        for name in filesList:
            filePath = os.path.join(dirPath, name)
            if os.path.isdir(filePath): self.updateWithDirectoryContents(filePath, notifyProgress)
            else:                       self.updateWithFile(filePath)
            if notifyProgress != None: 
                stop = notifyProgress('')
                if stop: return

    def updateWithBytes(self, b: bytes):
        self._sha.update(b)

    def getDigest(self):
        #return sha.hexdigest()    This is the common way, but tracer-service wants codified to base64
        #return hashOperator._bytesToBase64String(self._sha.digest())
        return self._sha.digest()


def _bytesToBase64String(b: bytes) -> str:
    return base64.b64encode(b).decode('ascii')

def _getHashOfBytes(b: bytes) -> bytes:
    return sha3(b).getDigest()
    # return hashlib.blake2s(b).digest()

def _getHashOfString(s: str) -> bytes:
    return _getHashOfBytes(bytes(s, 'utf-8'))

def _getHashOfFile(filePath: str) -> bytes:
    # Same result in one line, but requires loading the complete file content in memory, 
    # which can be a problem on big files.
    #return hashlib.sha256(open(filePath).read()).hexdigest()

    sha = sha3()
    sha.updateWithFile(filePath)
    return sha.getDigest()

def _getHashOfDirectory(dirPath, notifyProgress = None):
    sha = sha3()
    sha.updateWithDirectoryContents(dirPath, notifyProgress)
    if notifyProgress != None: 
        stop = notifyProgress('')
        if stop: return None
    return sha.getDigest()

def getHashOfString(s):
    return _bytesToBase64String(_getHashOfString(s))

def getHashOfFile(filePath):
    return _bytesToBase64String(_getHashOfFile(filePath))


class datasetHashesOperator:
    def __init__(self, dbconfig, series_hash_cache_life_days):
        self.log = logging.root
        self.dbconfig = dbconfig
        self.series_hash_cache_life_days = series_hash_cache_life_days

    def _getHashOfSeries(self, studyId, studyDirPath, seriesDirName, notifyProgress = None):
        with DB(self.dbconfig) as db:
            seriesHash, last_time_calculated = DBDatasetsOperator(db).getSeriesHashCache(studyId, seriesDirName)
            if seriesHash != None and last_time_calculated != None \
               and (datetime.now() - last_time_calculated).days <= self.series_hash_cache_life_days: 
                #logging.root.debug('Cached SHA of series: %s' % seriesDirName)
                return seriesHash

        seriesDirPath = os.path.join(studyDirPath, seriesDirName)
        newSeriesHash = _getHashOfDirectory(seriesDirPath, notifyProgress)
        if newSeriesHash is None: return None   # the process has been stopped
        # let's take the chance to warn if the hash has been altered since last time calculated (outdated cache)
        if seriesHash != None and base64.b64encode(newSeriesHash) != base64.b64encode(seriesHash): 
            logging.root.warn('Altered SHA of series (in residual cache): %s' % seriesDirName)
        # Anotate the new hash or refresh the last time calculated
        with DB(self.dbconfig) as db:
            DBDatasetsOperator(db).setSeriesHashCache(studyId, seriesDirName, newSeriesHash, datetime.now())
        return newSeriesHash

    def _getHashOfStudy(self, studyId, seriesList, studyDirPath, notifyProgress = None):
        sha = sha3()
        for series in seriesList:
            seriesHash = self._getHashOfSeries(studyId, studyDirPath, series["folderName"], notifyProgress)
            if seriesHash is None: return None   # the process has been stopped
            sha.updateWithBytes(seriesHash)
        return sha.getDigest()

    def _getHashOfDatasetImages(self, datasetDirPath, studies, studiesHashes = None, notifyProgress = None):
        sha = sha3()
        total = len(studies)
        count = 0
        for study in studies:
            count += 1
            studyDirPath = os.path.join(datasetDirPath, study['path'])
            logging.root.debug('Calculating SHA of study (%d/%d) [%s] ...' % (count, total, studyDirPath))
            if notifyProgress != None and (count == 1 or count % 2 == 0):
                notifyProgress('Calculating SHA of study %d of %d) ...' % (count, total), log=False)
            studyHash = self._getHashOfStudy(study["studyId"], study["series"], studyDirPath, notifyProgress)
            if studyHash is None: return None   # the process has been stopped
            if studiesHashes != None: studiesHashes.append(dict(studyId = study["studyId"], 
                                                                hash = _bytesToBase64String(studyHash)))
            sha.updateWithBytes(studyHash)
        return sha.getDigest()

    def getHashOfSeries(self, datasetDirPath, studyId, studyPath, seriesDirName):
        studyDirPath = os.path.join(datasetDirPath, studyPath)
        seriesHash = self._getHashOfSeries(studyId, studyDirPath, seriesDirName)
        if seriesHash is None: raise Exception()
        return _bytesToBase64String(seriesHash)

    def getHashOfDatasetImages(self, datasetDirPath, studies, studiesHashes = None, notifyProgress = None):
        imagesHash = self._getHashOfDatasetImages(datasetDirPath, studies, studiesHashes, notifyProgress)
        if imagesHash is None: return None   # the process has been stopped
        return _bytesToBase64String(imagesHash)

    def getHashesOfDataset(self, datasetDirPath, indexFileName, eformsFileName, studies = None, studiesHashes = None, notifyProgress = None):
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

        imagesHash = self.getHashOfDatasetImages(datasetDirPath, studies, studiesHashes, notifyProgress)
        if imagesHash is None: return None, None, None   # the process has been stopped
        clinicalDataHash = getHashOfFile(eformsFilePath)
        return indexHash, imagesHash, clinicalDataHash

