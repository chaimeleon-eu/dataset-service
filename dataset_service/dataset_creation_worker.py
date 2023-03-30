import os
import logging
import json
from dataset_service.storage import DB
import dataset_service.dataset as dataset_file_system
import dataset_service.tracer as tracer

class dataset_creation_worker:

    def __init__(self, config, datasetId):
        self.log = logging.root
        self.config = config
        self.datasetId = datasetId

    def updateProgress(self, message: str):
        with DB(self.config.db) as db:
            db.setDatasetCreationStatus(self.datasetId, "running", message)

    def endProgress(self, errorMessage: str | None = None):
        with DB(self.config.db) as db:
            if errorMessage is None:    # end successfully
                db.deleteDatasetCreationStatus(self.datasetId)
            else:                       # end with error
                db.setDatasetCreationStatus(self.datasetId, "error", errorMessage)

    #STOP = False
    def stop(self):
        #To do
        #STOP = True
        pass

    def run(self):
        datasetDirName = ''
        try:
            if self.config.self.datasets_mount_path == '':
                logging.root.warn("datasets_mount_path is empty: there is nothing to do by this job.")
                self.endProgress()
                return

            dataset_file_system.check_file_system(self.config.self.datalake_mount_path, self.config.self.datasets_mount_path)

            if self.config.tracer.url == '':
                logging.root.warn("tracer.url is empty: actions will not be notified to the tracer-service.")
            else: 
                tracer.check_connection(self.config.tracer.auth_url, self.config.tracer.client_id, self.config.tracer.client_secret, self.config.tracer.url)

            self.log.debug('Creating symbolic links...')
            self.updateProgress("Creating symbolic links")
            with DB(self.config.db) as db:
                dataset = db.getDataset(self.datasetId)
                if dataset is None: raise Exception("dataset not found in database")
                datasetStudies, total = db.getStudiesFromDataset(self.datasetId)

            datasetDirName = self.datasetId
            dataset_file_system.create_dataset(self.config.self.datasets_mount_path, datasetDirName, 
                                            self.config.self.datalake_mount_path, datasetStudies)
            
            datasetDirPath = os.path.join(self.config.self.datasets_mount_path, datasetDirName)

            if self.config.tracer.url != '':
                studiesHashes = []
                self.updateProgress("Calculating the hash of dataset")
                tracer.traceDatasetCreation(self.config.tracer.auth_url, self.config.tracer.client_id, self.config.tracer.client_secret, self.config.tracer.url, 
                                            datasetDirPath, self.config.self.index_file_name, self.config.self.eforms_file_name, 
                                            self.datasetId, dataset["authorId"], None, studiesHashes, self.updateProgress)
                # Save the hash of each study in the DB just for be able to get which studies have been changed 
                # in the unusual case in which the general hash of the dataset stored in the tracer has changed.
                with DB(self.config.db) as db:
                    for studyHash in studiesHashes:
                        db.setDatasetStudyHash(self.datasetId, studyHash["studyId"], studyHash["hash"])
        
            self.endProgress()

        except (tracer.TraceException, dataset_file_system.DatasetException) as e:
            self.endProgress(errorMessage=str(e))
            #if datasetDirName != '': dataset_file_system.remove_dataset(self.config.self.datasets_mount_path, datasetDirName)
        except Exception as e:
            self.endProgress(errorMessage="unexpected error")
            self.log.exception(e)
            #if datasetDirName != '': dataset_file_system.remove_dataset(self.config.self.datasets_mount_path, datasetDirName)
