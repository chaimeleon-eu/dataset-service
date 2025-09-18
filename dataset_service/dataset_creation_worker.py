import os
import logging
import time
from pathlib import Path
import json
from .auth import AuthClient, LoginException
from .storage import DB, DBDatasetsOperator, DBProjectsOperator
from .hash import datasetHashesOperator
from .config import Config
from . import dataset as dataset_file_system
from . import tracer as tracer

class WrongInputException(Exception): pass

class dataset_creation_worker:

    def __init__(self, config: Config, datasetId: str):
        self.log = logging.root
        self.config = config
        self.datasetId = datasetId

    def updateProgress(self, message: str, log = True) -> bool:
        ''' Returns True if the user have canceled the process and so all current tasks must stop.
            The message can be empty string to avoid changing the status message and just to know whether to continue o cancel.
        '''
        if self.stopping: return True
        if message != "":
            if log: self.log.debug(message)
            with DB(self.config.db) as db:
                DBDatasetsOperator(db).setDatasetCreationStatus(self.datasetId, "running", message)
        return False

    def _endProgress(self, errorMessage: str | None = None):
        with DB(self.config.db) as db:
            dbdatasets = DBDatasetsOperator(db)
            if errorMessage is None:    # end successfully
                dbdatasets.deleteDatasetCreationStatus(self.datasetId)
            else:                       # end with error
                dbdatasets.setDatasetCreationStatus(self.datasetId, "error", errorMessage)

    def _cancelProgress(self):
        self._endProgress(errorMessage="Canceled by user")

    stopping = False
    def stop(self):
        self.stopping = True
        with DB(self.config.db) as db:
            DBDatasetsOperator(db).setDatasetCreationStatus(self.datasetId, "running", "Canceling...")

    WAIT_FOR_DATASET_INTERVAL_SECONDS = 5
    WAIT_FOR_DATASET_MAX_SECONDS = 120

    def _getDataset(self, db):
        dataset = None
        seconds_waiting = 0
        while dataset is None and seconds_waiting <= self.WAIT_FOR_DATASET_MAX_SECONDS:
            self.log.info("Waiting %d seconds for the dataset appearing in DB..." % self.WAIT_FOR_DATASET_INTERVAL_SECONDS)
            time.sleep(self.WAIT_FOR_DATASET_INTERVAL_SECONDS)
            seconds_waiting += self.WAIT_FOR_DATASET_INTERVAL_SECONDS
            dataset = DBDatasetsOperator(db).getDataset(self.datasetId)
        return dataset

    @staticmethod
    def _checkPath(basePath: str, relativePath: str):
        ''' Ensures relativePath is in fact a subpath of basePath. Raises an exception if wrong path.
            This is to avoid a malicious user try to access system directories with "..", absolute paths like "/etc" or symbolic links.
        '''
        base = Path(basePath).resolve()  # Resolve symbolic links and returns absolute path
        path = (base / relativePath).resolve()
        if not base in path.parents:
            raise WrongInputException("Wrong path: " + str(relativePath))
        
    def _checkStudiesPaths(self, datasetDirPath, studies):
        if self.config.self.datalake_mount_path != '':
            for study in studies:
                self._checkPath(self.config.self.datalake_mount_path, study['pathInDatalake'])
                for serie in study["series"]:
                    self._checkPath(self.config.self.datalake_mount_path, os.path.join(study['pathInDatalake'], serie["folderName"]))
        for study in studies:
            self._checkPath(datasetDirPath, study['subjectName'])
    
    def _removeSeriesMissingInDatalake(self, studies):
        studiesToDelete = []
        for study in studies:
            seriesToDelete = []
            for serie in study["series"]:
                seriePathInDatalake = os.path.join(self.config.self.datalake_mount_path, study['pathInDatalake'], serie['folderName'])
                if not os.path.exists(seriePathInDatalake):
                    self.log.warn("The directory '%s' does not exist. That series will not be included in the dataset." % seriePathInDatalake)
                    seriesToDelete.append(serie)
            for serie in seriesToDelete: 
                study["series"].remove(serie)
            if len(study["series"]) == 0:
                self.log.warn("The study with id '%s' does not have any series. It will not be included in the dataset." % study["studyId"])
                studiesToDelete.append(study)
        for study in studiesToDelete:
            studies.remove(study)

    def _writeIndexFile(self, studies, indexFilePath):
        # dataset["studies"] contains all the information we want to save in the index.json file,
        # but we have to take only some of the properties for each study and set paths relative to the dataset directory
        outStudies = []
        for study in studies:
            subjectDirName = study['subjectName']
            studyDirName = os.path.basename(study['pathInDatalake'])
            studyPath = os.path.join(subjectDirName, studyDirName)  # example of studyPath: 17B76FEW/TCPEDITRICOABDOMINOPLVICO20150129
            s = { 'studyId': study['studyId'],
                  'studyName': study['studyName'],
                  'subjectName': study['subjectName'],
                  'path': studyPath,
                  'series': [],
                  'url': study['url'] }
            for series in study['series']:
                s['series'].append({
                    'folderName': series['folderName'],
                    'tags': series['tags'] })
            outStudies.append(s)
        # and dump to the index file in the dataset directory
        with open(indexFilePath, 'w') as outputStream:
            json.dump(outStudies, outputStream)

    def run(self):
        auth_client = AuthClient(self.config.auth.client.auth_url, self.config.auth.client.client_id, self.config.auth.client.client_secret)
        try:
            if self.config.self.datasets_mount_path == '':
                self.log.warn("datasets_mount_path is empty: there is nothing to do by this job.")
                self._endProgress()
                return

            dataset_file_system.check_file_system(self.config.self.datalake_mount_path, self.config.self.datasets_mount_path)

            if self.config.tracer.url == '':
                self.log.warn("tracer.url is empty: actions will not be notified to the tracer-service.")
            else: 
                tracer.check_connection(auth_client, self.config.tracer.url)

            datasetDirName = self.datasetId
            datasetDirPath = os.path.join(self.config.self.datasets_mount_path, datasetDirName)
            studiesTmpFilePath = os.path.join(datasetDirPath, self.config.self.studies_tmp_file_name)

            with DB(self.config.db) as db:
                dataset = self._getDataset(db)
                if dataset is None: raise Exception("dataset not found in database")
                datasetStudies, total = DBDatasetsOperator(db).getStudiesFromDataset(self.datasetId)
            if total > 0:  
                # This is true only when the creation of dataset has been interrupted previously 
                # and the studies are already stored in DB. 
                # The creation has been relaunched and it is not required to read, do the checks and save studies in DB again.
                dataset["studies"] = datasetStudies
            else:  
                # this is the normal case
                with open(studiesTmpFilePath, 'rb') as f:
                    dataset["studies"] = json.load(f)

                # Security check: review all data sent by the user which is involved in path constructions
                stop = self.updateProgress("Checking studies paths...")
                if stop: self._cancelProgress(); return
                self._checkStudiesPaths(datasetDirPath, dataset["studies"])

                # File system checks
                if self.config.self.datalake_mount_path != '':
                    stop = self.updateProgress("Checking for missing series in datalake...")
                    if stop: self._cancelProgress(); return
                    self._removeSeriesMissingInDatalake(dataset["studies"])

                stop = self.updateProgress('Creating studies in DB...')
                if stop: self._cancelProgress(); return
                with DB(self.config.db) as db:
                    dbdatasets = DBDatasetsOperator(db)
                    for study in dataset["studies"]:
                        if not "studyName" in study: study["studyName"] = "-"
                        dbdatasets.createOrUpdateStudy(study, self.datasetId)

            isExternalDataset = (dataset['project'] == self.config.self.external_datasets_project_code)

            if dataset["sizeInBytes"] is None:  # This condition is just to skip collecting again metadata if it's
                                                # already there because of relaunch of the dataset creation.
                stop = self.updateProgress("Scanning dataset for collecting metadata...")
                if stop: self._cancelProgress(); return
                eformsFilePath = os.path.join(datasetDirPath, self.config.self.eforms_file_name)
                # Collect metadata doing some checks and adding as new properties to dataset, to the studies inside, 
                # and to the series inside the studies.
                dataset_file_system.collectMetadata(dataset, self.config.self.datalake_mount_path, eformsFilePath)
                if not isExternalDataset:
                    # Security check: It is important to check the subprojectId to avoid create dataset with studies from other project
                    with DB(self.config.db) as db:
                        subprojectsIDs = DBProjectsOperator(db).getSubprojectsIDs(dataset["project"])
                    if not dataset["subprojectId"] in subprojectsIDs:
                        raise Exception("Security check failed: the subprojectId '%s' obtained from DICOM files (tag 70D1,2000)" % dataset["subprojectId"]
                                        + "is not in the project where dataset is being created: " 
                                        + "%s (%s)" % (dataset["project"], json.dumps(subprojectsIDs)))
                with DB(self.config.db) as db:
                    DBDatasetsOperator(db).updateDatasetAndStudyMetadata(dataset)
            
            indexFilePath = os.path.join(datasetDirPath, self.config.self.index_file_name)
            if not os.path.exists(indexFilePath):  # Again this condition is just to skip in case of relaunching
                                                   # if the next step (index file creation) is done or at least started (the file exists)
                stop = self.updateProgress("Creating symbolic links...")
                if stop: self._cancelProgress(); return
                dataset_file_system.create_dataset(self.config.self.datasets_mount_path, datasetDirName, 
                                                   self.config.self.datalake_mount_path, dataset["studies"])

            stop = self.updateProgress('Writing INDEX file: ' + self.config.self.index_file_name)
            if stop: self._cancelProgress(); return
            self._writeIndexFile(dataset["studies"], indexFilePath)

            authorId = dataset["authorId"]
            # let's free the memory because it's not required anymore and the next steps still may take long time
            del dataset
            
            if self.config.tracer.url != '' and not isExternalDataset:
                studiesHashes = []
                stop = self.updateProgress("Calculating the hashes of the dataset...")
                if stop: self._cancelProgress(); return
                hashesOperator = datasetHashesOperator(self.config.db, self.config.self.series_hash_cache_life_days)
                tracer.traceDatasetCreation(auth_client, self.config.tracer.url, 
                                            datasetDirPath, self.config.self.index_file_name, self.config.self.eforms_file_name, 
                                            self.datasetId, authorId, hashesOperator, None, studiesHashes, self.updateProgress)

                stop = self.updateProgress("Saving hashes in database...")
                if stop: self._cancelProgress(); return
                # Save the hash of each study in the DB just for being able to know which studies have been changed 
                # in the unusual case in which the general hash of the dataset stored in the tracer has changed.
                with DB(self.config.db) as db:
                    dbdatasets = DBDatasetsOperator(db)
                    for studyHash in studiesHashes:
                        dbdatasets.setDatasetStudyHash(self.datasetId, studyHash["studyId"], studyHash["hash"])
        
            # delete studies temporal file
            os.unlink(studiesTmpFilePath)

            stop = self.updateProgress("Dataset creation finished.")
            if stop: self._cancelProgress(); return
            self._endProgress()
        except (WrongInputException, tracer.TraceException, dataset_file_system.DatasetException, LoginException) as e:
            self._endProgress(errorMessage=str(e))
        except Exception as e:
            self._endProgress(errorMessage="Unexpected error")
            self.log.exception(e)
