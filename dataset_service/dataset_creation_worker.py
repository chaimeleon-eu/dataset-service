import os
import logging
import pydicom
from dataset_service import dicom
from dataset_service.auth import AuthClient, LoginException
from dataset_service.storage import DB
import dataset_service.dataset as dataset_file_system
import dataset_service.tracer as tracer

class dataset_creation_worker:

    def __init__(self, config, datasetId):
        self.log = logging.root
        self.config = config
        self.datasetId = datasetId

    @staticmethod
    def getMetadataFromFirstDicomFile(serieDirPath):
        for name in os.listdir(serieDirPath):
            if name.lower().endswith(".dcm"):
                dicomFilePath = os.path.join(serieDirPath, name)
                dcm = pydicom.dcmread(dicomFilePath)
                age = (dcm[dicom.AGE_TAG].value if dicom.AGE_TAG in dcm else None)
                sex = (dcm[dicom.SEX_TAG].value if dicom.SEX_TAG in dcm else None)
                bodyPart = (dcm[dicom.BODY_PART_TAG].value if dicom.BODY_PART_TAG in dcm else None)
                modality = (dcm[dicom.MODALITY_TAG].value  if dicom.MODALITY_TAG in dcm else None)
                #datasetType = dcm[dicom.DATASET_TYPE_TAG].value    it seems very similar to modality
                return age, sex, bodyPart, modality
        return None, None, None, None

    def collectMetadata(self, dataset):
        differentSubjects = set()
        studiesCount = 0
        maxAge = "000D"
        minAge = "999Y"
        sexList = set()
        bodyPartList = set()
        modalityList = set()
        seriesTagsList = set()
        for study in dataset["studies"]:
            studiesCount += 1
            if not study["subjectName"] in differentSubjects: 
                differentSubjects.add(study["subjectName"])
            if len(study['series']) == 0: continue
            if self.config.self.datalake_mount_path != '':
                seriePathInDatalake = os.path.join(self.config.self.datalake_mount_path, study['pathInDatalake'], study['series'][0]['folderName'])
                age, sex, bodyPart, modality = self.getMetadataFromFirstDicomFile(seriePathInDatalake)
                if (age is None or sex is None or bodyPart is None or modality is None):
                    # sometimes first serie is special, try with the second if exists
                    if len(study['series']) > 1:
                        seriePathInDatalake = os.path.join(self.config.self.datalake_mount_path, study['pathInDatalake'], study['series'][1]['folderName'])
                        age, sex, bodyPart, modality = self.getMetadataFromFirstDicomFile(seriePathInDatalake)
                if age != None:
                    minAge = min(minAge, age, key=lambda x: dicom.getAgeInDays(x))
                    maxAge = max(maxAge, age, key=lambda x: dicom.getAgeInDays(x))
                if sex != None: sexList.add(sex)
                if bodyPart != None: bodyPartList.add(bodyPart) 
                if modality != None: modalityList.add(modality)
            for series in study["series"]:
                seriesTagsList.update(series["tags"])
                
        dataset["studiesCount"] = studiesCount
        dataset["subjectsCount"] = len(differentSubjects)
        dataset["ageLow"] = (minAge if minAge != "999Y" else None)
        dataset["ageHigh"] = (maxAge if maxAge != "000D" else None)
        dataset["sex"] = list(sexList)
        dataset["bodyPart"] = list(bodyPartList)
        dataset["modality"] = list(modalityList)
        dataset["seriesTags"] = list(seriesTagsList)
        self.log.debug("  -studiesCount: %s" % dataset["studiesCount"])
        self.log.debug("  -subjectsCount: %s" % dataset["subjectsCount"])
        self.log.debug("  -ageLow: %s" % dataset["ageLow"])
        self.log.debug("  -ageHigh: %s" % dataset["ageHigh"])
        self.log.debug("  -sex: %s" % dataset["sex"])
        self.log.debug("  -bodyPart: %s" % dataset["bodyPart"])
        self.log.debug("  -modality: %s" % dataset["modality"])
        self.log.debug("  -seriesTags: %s" % dataset["seriesTags"])
        
    # def collectMetadata2(self, dataset):
    #     if self.config.self.datalake_mount_path == '': return
    #     from pydicom.fileset import FileSet
    #     fs = FileSet()
    #     for study in dataset["studies"]:
    #         for serie in study["series"]:
    #             seriePathInDatalake = os.path.join(self.config.self.datalake_mount_path, study['pathInDatalake'], study['series'][0])
    #             fs.add(seriePathInDatalake)
    #     values = fs.find_values(["StudyInstanceUID", "PatientID", (0x0010, 0x1010), (0x0010, 0x0040), (0x0008, 0x0016), (0x0018, 0x0015), (0x0008, 0x0060)])
    #     dataset["studiesCount"] = len(values["StudyInstanceUID"])
    #     dataset["subjectsCount"] = len(values["PatientID"])
    #     # dataset["ageLow"] = reduce(lambda x, y: min(x, getAgeInYears(y)), values[0x0010, 0x1010])
    #     # dataset["ageHigh"] = reduce(lambda x, y: max(x, getAgeInYears(y)), values[0x0010, 0x1010])
    #     dataset["ageLow"] = min(values[0x0010, 0x1010], key=lambda x: dicom.getAgeInDays(x))
    #     dataset["ageHigh"] = max(values[0x0010, 0x1010], key=lambda x: dicom.getAgeInDays(x))
    #     dataset["sex"] = values[0x0010, 0x0040]
    #     dataset["datasetType"] = values[0x0008, 0x0016]
    #     dataset["bodyPart"] = values[0x0018, 0x0015]
    #     dataset["modality"] = values[0x0008, 0x0060]

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
        datasetDirName = ''
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

            with DB(self.config.db) as db:
                dataset = db.getDataset(self.datasetId)
                if dataset is None: raise Exception("dataset not found in database")
                datasetStudies, total = db.getStudiesFromDataset(self.datasetId)
                dataset["studies"] = datasetStudies

                stop = self.updateProgress("Scanning dataset for collecting metadata...")
                if stop: self._cancelProgress(); return
                self.collectMetadata(dataset)
                db.updateDatasetMetadata(dataset)
                
            stop = self.updateProgress("Creating symbolic links...")
            if stop: self._cancelProgress(); return
            datasetDirName = self.datasetId
            dataset_file_system.create_dataset(self.config.self.datasets_mount_path, datasetDirName, 
                                               self.config.self.datalake_mount_path, datasetStudies)
            
            datasetDirPath = os.path.join(self.config.self.datasets_mount_path, datasetDirName)

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
