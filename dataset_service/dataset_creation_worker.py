import os
import logging
import json
from dataset_service.storage import DB
import dataset_service.dataset as dataset_file_system
import dataset_service.tracer as tracer

LOG = logging.root

#STOP = False
def stop():
    #To do
    #STOP = True
    pass

def run(CONFIG, datasetId):
    if CONFIG is None: raise Exception()
    with DB(CONFIG.db) as db:
        datasetDirName = ''
        try:
            if CONFIG.self.datasets_mount_path == '':
                logging.root.warn("datasets_mount_path is empty: there is nothing to do by this job.")
                with DB(CONFIG.db) as db:
                    db.deleteDatasetCreationStatus(datasetId)
                return

            dataset_file_system.check_file_system(CONFIG.self.datalake_mount_path, CONFIG.self.datasets_mount_path)

            if CONFIG.tracer.url == '':
                logging.root.warn("tracer.url is empty: actions will not be notified to the tracer-service.")
            else: 
                tracer.check_connection(CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url)

            LOG.debug('Creating symbolic links...')
            db.setDatasetCreationStatus(datasetId, "running", "Creating symbolic links")
            dataset = db.getDataset(datasetId)
            if dataset is None: raise Exception("dataset not found in database")
            datasetStudies, total = db.getStudiesFromDataset(datasetId)

            datasetDirName = datasetId
            dataset_file_system.create_dataset(CONFIG.self.datasets_mount_path, datasetDirName, 
                                               CONFIG.self.datalake_mount_path, datasetStudies)
            
            datasetDirPath = os.path.join(CONFIG.self.datasets_mount_path, datasetDirName)

            if CONFIG.tracer.url != '':
                studiesHashes = []
                # Note this tracer call is inside of "with db" because if tracer fails the database changes will be reverted (transaction rollback).
                db.setDatasetCreationStatus(datasetId, "running", "Calculating the hash of dataset")
                tracer.traceDatasetCreation(CONFIG.tracer.auth_url, CONFIG.tracer.client_id, CONFIG.tracer.client_secret, CONFIG.tracer.url, 
                                            datasetDirPath, CONFIG.self.index_file_name, CONFIG.self.eforms_file_name, 
                                            datasetId, dataset["authorId"], None, studiesHashes)
                # Save the hash of each study in the DB just for be able to get which studies have been changed 
                # in the unusual case in which the general hash of the dataset stored in the tracer has changed.
                for studyHash in studiesHashes:
                    db.setDatasetStudyHash(datasetId, studyHash["studyId"], studyHash["hash"])
        
            db.deleteDatasetCreationStatus(datasetId)

        except (tracer.TraceException, dataset_file_system.DatasetException) as e:
            db.setDatasetCreationStatus(datasetId, "error", str(e))
            #if datasetDirName != '': dataset_file_system.remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
        except Exception as e:
            db.setDatasetCreationStatus(datasetId, "error", "unexpected error")
            LOG.exception(e)
            #if datasetDirName != '': dataset_file_system.remove_dataset(CONFIG.self.datasets_mount_path, datasetDirName)
