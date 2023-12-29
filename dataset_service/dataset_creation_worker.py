import os
import logging
from dataset_service.auth import AuthClient, LoginException
from dataset_service.storage import DB
import dataset_service.dataset as dataset_file_system
import dataset_service.tracer as tracer

class dataset_creation_worker:

    def __init__(self, config, datasetId):
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
                db.setDatasetCreationStatus(self.datasetId, "running", message)
        return False

    def _endProgress(self, errorMessage: str | None = None):
        with DB(self.config.db) as db:
            if errorMessage is None:    # end successfully
                db.deleteDatasetCreationStatus(self.datasetId)
            else:                       # end with error
                db.setDatasetCreationStatus(self.datasetId, "error", errorMessage)

    def _cancelProgress(self):
        self._endProgress(errorMessage="Canceled by user")

    stopping = False
    def stop(self):
        self.stopping = True
        with DB(self.config.db) as db:
            db.setDatasetCreationStatus(self.datasetId, "running", "Canceling...")

    def run(self):
        auth_client = AuthClient(self.config.auth.client.auth_url, self.config.auth.client.client_id, self.config.auth.client.client_secret)
        try:
            if self.config.self.datasets_mount_path == '':
                logging.root.warn("datasets_mount_path is empty: there is nothing to do by this job.")
                self._endProgress()
                return

            dataset_file_system.check_file_system(self.config.self.datalake_mount_path, self.config.self.datasets_mount_path)

            if self.config.tracer.url == '':
                logging.root.warn("tracer.url is empty: actions will not be notified to the tracer-service.")
            else: 
                tracer.check_connection(auth_client, self.config.tracer.url)

            datasetDirName = self.datasetId
            datasetDirPath = os.path.join(self.config.self.datasets_mount_path, datasetDirName)

            with DB(self.config.db) as db:
                dataset = db.getDataset(self.datasetId)
                if dataset is None: raise Exception("dataset not found in database")
                datasetStudies, total = db.getStudiesFromDataset(self.datasetId)
                dataset["studies"] = datasetStudies

                stop = self.updateProgress("Scanning dataset for collecting metadata...")
                if stop: self._cancelProgress(); return
                eformsFilePath = os.path.join(datasetDirPath, self.config.self.eforms_file_name)
                dataset_file_system.collectMetadata(dataset, self.config.self.datalake_mount_path, eformsFilePath)
                for study in dataset["studies"]:
                    db.updateStudyMetadata(study)
                db.updateDatasetMetadata(dataset)
                
            stop = self.updateProgress("Creating symbolic links...")
            if stop: self._cancelProgress(); return
            
            dataset_file_system.create_dataset(self.config.self.datasets_mount_path, datasetDirName, 
                                               self.config.self.datalake_mount_path, datasetStudies)

            if self.config.tracer.url != '':
                studiesHashes = []
                stop = self.updateProgress("Calculating the hashes of the dataset...")
                if stop: self._cancelProgress(); return
                tracer.traceDatasetCreation(auth_client, self.config.tracer.url, 
                                            datasetDirPath, self.config.self.index_file_name, self.config.self.eforms_file_name, 
                                            self.datasetId, dataset["authorId"], None, studiesHashes, self.updateProgress)

                stop = self.updateProgress("Saving hashes in database...")
                if stop: self._cancelProgress(); return
                # Save the hash of each study in the DB just for being able to know which studies have been changed 
                # in the unusual case in which the general hash of the dataset stored in the tracer has changed.
                with DB(self.config.db) as db:
                    for studyHash in studiesHashes:
                        db.setDatasetStudyHash(self.datasetId, studyHash["studyId"], studyHash["hash"])
        
            stop = self.updateProgress("Dataset creation finished.")
            if stop: self._cancelProgress(); return
            self._endProgress()

        except (tracer.TraceException, dataset_file_system.DatasetException, LoginException) as e:
            self._endProgress(errorMessage=str(e))
        except Exception as e:
            self._endProgress(errorMessage="Unexpected error")
            self.log.exception(e)
