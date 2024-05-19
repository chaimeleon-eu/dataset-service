import shutil
import logging
from dataset_service.POSIX import *
from dataset_service import dicom, eform

class DatasetException(Exception):
    pass

def check_file_system(datalake_mount_path, datasets_mount_path):
    logging.root.info("Checking mount paths...")
    if os.path.isdir(datalake_mount_path):
        logging.root.info("OK - datalake_mount_path: " + datalake_mount_path)
    else: 
        e = Exception("datalake_mount_path is not an existing directory: " + datalake_mount_path)
        logging.root.exception(e)
        raise e
    if os.path.isdir(datasets_mount_path):
        logging.root.info("OK - datasets_mount_path: " + datasets_mount_path)
    else: 
        e = Exception("datasets_mount_path is not an existing directory: " + datasets_mount_path)
        logging.root.exception(e)
        raise e

def remove_dataset(datasets_dir_path, dataset_dir_name):
    dataset_dir_path = os.path.join(datasets_dir_path, dataset_dir_name)
    shutil.rmtree(dataset_dir_path, ignore_errors=True)

def create_dataset_dir(datasets_dir_path, dataset_dir_name):
    '''Extracted from create_dataset() because the directory has to be previously created to write some files (index, eforms)'''
    owner_uid = 0
    owner_gid = 0
    dataset_dir_path = os.path.join(datasets_dir_path, dataset_dir_name)
    # Create dataset directory
    create_dir(dataset_dir_path, uid=owner_uid, gid=owner_gid, permissions=0o700)
    # Now only root have access. The access to normal users will be granted later with ACLs.

def adjust_file_permissions_in_datalake(datalake_dir_path, studies):
    subjectsSeen = set()
    usersSeen = set()
    for study in studies:
        study_dir_path = os.path.join(datalake_dir_path, study['pathInDatalake'])
        # Ensure only root have access at level of study. 
        # The access to normal users will be granted later with ACLs.
        chmod(study_dir_path, 0o700)

        # Ensure all the people have read access at the lower levels (series dirs and dicom files).
        # The access control with ACLs is done at the study level, not required also in lower levels.
        for name in os.listdir(study_dir_path):
            f = os.path.join(study_dir_path, name)
            if os.path.isdir(f): chmod_recursive(f, dirs_permissions=0o705, files_permissions=0o604)
            else: chmod(f, 0o604)

        # Ensure all people have access to the upper levels in datalake (subject dir and user dir)
        subject_dir_path = os.path.dirname(study_dir_path)
        #   subjectDirPathInDatalake example: /mnt/cephfs/datalake/blancagomez/17B76FEW_Neuroblastoma
        if not subject_dir_path in subjectsSeen:
            chmod(subject_dir_path, 0o705)
            subjectsSeen.add(subject_dir_path)
            qp_user_dir_path = os.path.dirname(subject_dir_path)
            #   userDirPathInDatalake example: /mnt/cephfs/datalake/blancagomez
            if not qp_user_dir_path in usersSeen:
                chmod(qp_user_dir_path, 0o705)
                usersSeen.add(qp_user_dir_path)


def create_dataset(datasets_dir_path, dataset_dir_name, datalake_dir_path, studies):
    '''
    Creates the dataset directory, the subject directories and the symbolic links to the study directories (in the datalake).
    PARAMS:
        datasets_dir_path           # Folder to store the datasets. Example: "/mnt/cephfs/datasets"
        dataset_dir_name            # Dataset name. Example: "3d44ba8a-16df-11ec-ac5f-00155d6b30e6"
        datalake_dir_path           # Folder where datalake is mounted. Example: "/mnt/cephfs/datalake"
        studies                     # List of studies selected for the dataset
    '''
    owner_uid = 0
    owner_gid = 0
    dataset_dir_path = os.path.join(datasets_dir_path, dataset_dir_name)
    # Create dataset directory
    #os.mkdir(dataset_dir)
    create_dir(dataset_dir_path, uid=owner_uid, gid=owner_gid, permissions=0o700)
    # Now only root have access. The access to normal users will be granted later with ACLs.
    
    for study in studies:
        subjectDirName = study['subjectName']
        subjectDirPath = os.path.join(dataset_dir_path, subjectDirName)
        if not os.path.exists(subjectDirPath):
            create_dir(subjectDirPath, uid=owner_uid, gid=owner_gid, permissions=0o705)
            # At this level all the people have read access, the control with ACLs is done in the upper level.

        # study['pathInDatalake'] example: blancagomez/17B76FEW_Neuroblastoma/TCPEDITRICOABDOMINOPLVICO20150129/
        studyDirName = os.path.basename(study['pathInDatalake'])
        linkLocation = os.path.join(subjectDirPath, studyDirName)
        linkDestination = os.path.join(datalake_dir_path, study['pathInDatalake'])
        #   linkLocation example: /mnt/cephfs/datasets/myDataset/17B76FEW/TCPEDITRICOABDOMINOPLVICO20150129
        if os.path.islink(linkLocation): 
            continue   # already created (it can happen when the process is interrupted and relaunched)
        ok = symlink(linkDestination, linkLocation, target_is_directory=True, uid=owner_uid, gid=owner_gid)
        if not ok: 
            logging.root.error("Error creating symlink: " + linkLocation + " -> " + linkDestination)
            raise DatasetException("Error creating symlink")
            
    adjust_file_permissions_in_datalake(datalake_dir_path, studies)

def give_access_to_dataset(datasets_dir_path, dataset_dir_name, datalake_dir_path, pathsOfStudies, acl_gid):
    '''
    Applies the ACL rules to the files.
    PARAMS:
        datasets_dir_path           # Folder to store the datasets. Example: "/mnt/cephfs/datasets"
        dataset_dir_name            # Dataset name. Example: "3d44ba8a-16df-11ec-ac5f-00155d6b30e6"
        datalake_dir_path           # Folder where datalake is mounted. Example: "/mnt/cephfs/datalake"
        pathsOfStudies              # List of paths in datalake to studies selected for the dataset
        acl_gid                     # GID to apply ACL_permissions. Example: "1000" (dataset_group) 
    '''
    acl_permissions = 'rx'
    dataset_dir_path = os.path.join(datasets_dir_path, dataset_dir_name)
    # ACL to the dataset folder 
    ok = set_acl_group(str(acl_gid), acl_permissions, dataset_dir_path, recursive=False)
    if not ok: 
        logging.root.error("Error in set acl to: " + dataset_dir_path)
        raise DatasetException("Error in set acl.")

    for pathInDatalake in pathsOfStudies:
        # pathInDatalake example: blancagomez/01_Neuroblastoma_4_Neuroblastoma/TCPEDITRICOABDOMINOPLVICO20150129/
        linkDestination = os.path.join(datalake_dir_path, pathInDatalake)
        # ACL to the destination study directory (not the symbolic link)
        ok = set_acl_group(str(acl_gid), acl_permissions, linkDestination, recursive=False) 
        if not ok: 
            logging.root.error("Error in set acl to: " + linkDestination)
            raise DatasetException("Error in set acl.")

        # # ACL to the directory that contains the file
        # ok &= set_acl_group(str(acl_gid), str(acl_permissions), os.path.dirname(linkDestination), recursive=False) 
    
def remove_access_to_dataset(datasets_dir_path, dataset_dir_name, acl_gid):
    dataset_dir_path = os.path.join(datasets_dir_path, dataset_dir_name)
    # ACL of the dataset folder 
    ok = delete_acl_group(str(acl_gid), dataset_dir_path, recursive=False)
    if not ok: 
        logging.root.error("Error in delete acl to: " + dataset_dir_path)
        raise DatasetException("Error in delete acl.")

def remove_access_to_studies(datalake_dir_path, pathsOfStudies, acl_gid):
    for pathInDatalake in pathsOfStudies:
        # pathInDatalake example: blancagomez/01_Neuroblastoma_4_Neuroblastoma/TCPEDITRICOABDOMINOPLVICO20150129/
        linkDestination = os.path.join(datalake_dir_path, pathInDatalake)
        # ACL to the destination study directory (not the symbolic link)
        ok = delete_acl_group(str(acl_gid), linkDestination, recursive=False) 
        if not ok: 
            logging.root.error("Error in delete acl to: " + linkDestination)
            raise DatasetException("Error in delete acl.")

def invalidate_dataset(datasets_dir_path, dataset_dir_name):
    '''
    Removes the ACL rules that enables accessing the files from users.
    PARAMS:
        datasets_dir_path           # Folder to store the datasets. Example: "/mnt/cephfs/datasets"
        dataset_dir_name            # Dataset name. Example: "3d44ba8a-16df-11ec-ac5f-00155d6b30e6"
    '''
    dataset_dir_path = os.path.join(datasets_dir_path, dataset_dir_name)
    ok = clean_acl(dataset_dir_path, recursive=False)
    if not ok: 
        logging.root.error("Error in delete acl entries to: " + dataset_dir_path)
        raise DatasetException("Error in delete acl entries.")

    ## Delete ACLs in directories of studies
    ## It is complex because many datasets can include the same study.

def _getFirstDicomFile(DirPath) -> dicom.Dicom | None:
    for fileName in os.listdir(DirPath):
        if fileName.lower().endswith(".dcm"):
            dicomFilePath = os.path.join(DirPath, fileName)
            return dicom.Dicom(dicomFilePath)
    return None

def _checkMetadataItemAndSave(study, item, newValue, filename):
    if newValue is None: return
    if study[item] is None:  # First series with that item included
        study[item] = newValue
    else:   # the item is included in other series, let's check if the value is the same
        if newValue != study[item]:
            raise Exception("The %s in that series differs from other series: %s" % (item, filename))

def _addMetadataFromDicomOfASeriesToStudyAndSeries(dcm: dicom.Dicom, study, series):
    ageInDays, ageUnit = dcm.getAge()
    if ageInDays != None:
        if study["ageInDays"] != None:
            if ageInDays != study["ageInDays"]:
                raise Exception("The patientAge in that series differs from other series: " + dcm.getFileName())
        else:
            study["ageInDays"], study["ageUnit"] = ageInDays, ageUnit
    
    _checkMetadataItemAndSave(study, "sex", dcm.getSex(), dcm.getFileName())
    _checkMetadataItemAndSave(study, "studyDate", dcm.getStudyDate(), dcm.getFileName())
    _checkMetadataItemAndSave(study, "diagnosis", dcm.getDiagnosis(), dcm.getFileName())
    #dcm.getDatasetType()    it seems very similar to modality

    series["bodyPart"] = dcm.getBodyPart()
    series["modality"] = dcm.getModality()
    series["manufacturer"] = dcm.getManufacturer()

def _readStudyMetadataFromFirstDicomFileOfAllSeries(studyPathInDatalake, study):
    study["ageInDays"] = None
    study["sex"] = None
    study["studyDate"] = None
    study["diagnosis"] = None
    for series in study['series']:
        seriesDirName = series['folderName']
        seriesDirPathInDatalake = os.path.join(studyPathInDatalake, seriesDirName)
        dcm = _getFirstDicomFile(seriesDirPathInDatalake)
        if dcm != None:
            _addMetadataFromDicomOfASeriesToStudyAndSeries(dcm, study, series)
        else:
            logging.root.warning("There is a series without any dicom file"
                + "[studyId: %s, seriesFolderPath: %s]" % (study["studyId"], seriesDirPathInDatalake))
            series["bodyPart"], series["modality"], series["manufacturer"] = None, None, None

def _completeStudyMetadataWithSubjectEform(study, eform: eform.Eform):
    if not "diagnosisYear" in study: study["diagnosisYear"] = None
    if not "ageInDays" in study: study["ageInDays"] = None
    if not "sex" in study: study["sex"] = None
    if eform is None: return
    if eform.diagnosisYear != None: study["diagnosisYear"] = eform.diagnosisYear
    if eform.ageInDays != None:     study["ageInDays"] = eform.ageInDays
    if eform.ageUnit != None:       study["ageUnit"] = eform.ageUnit
    if eform.sex != None:           study["sex"] = eform.sex


MAX_AGE_VALUE = 500*365
MAX_YEAR_VALUE = 65536

def _obtainStudyAggregatedSet(study, property) -> set:
    aggregatedSet = set()
    for series in study['series']:
        aggregatedSet.add(series[property])
    return aggregatedSet
def _aggregateItemToCountDict(countDict: dict, newItem: str|None):
    # if newItem is None, then None is added as a key to the countDict and is managed as any other item
    if newItem in countDict:
        countDict[newItem] += 1
    else: countDict[newItem] = 1
def _aggregateItemsToCountDict(countDict: dict, newItems: set[str|None]):
    for item in newItems:
        _aggregateItemToCountDict(countDict, item)
def _getValuesAndCountsFromCountDict(countDict: dict[str|None, int]) -> tuple[list[str|None], list[int]]:
    # remove and reinsert the None key if exists in order to move it to the end
    NoneCount = countDict.pop(None, None)
    if NoneCount != None: countDict[None] = NoneCount
    # Since python 3.7, keys() and values() order is guaranteed to be insertion order.
    values = list(countDict.keys())
    counts = list(countDict.values())
    return values, counts

def _getTotalStudySizeInBytes(studyPathInDatalake, study):
    studySize = 0
    for series in study['series']:
        seriesDirName = series['folderName']
        seriesDirPathInDatalake = os.path.join(studyPathInDatalake, seriesDirName)
        for fileName in os.listdir(seriesDirPathInDatalake):
            studySize += os.path.getsize(os.path.join(seriesDirPathInDatalake, fileName))
    study["sizeInBytes"] = studySize

def collectMetadata(dataset, datalake_mount_path, eformsFilePath):
    differentSubjects = set()
    studiesCount = 0
    minAgeInDays, maxAgeInDays = MAX_AGE_VALUE, 0
    minAgeUnit, maxAgeUnit = None, None
    ageNullCount = 0
    sexDict = {}
    bodyPartDict = {}
    modalityDict = {}
    manufacturerDict = {}
    minDiagnosisYear, maxDiagnosisYear = MAX_YEAR_VALUE, 0
    diagnosisYearNullCount = 0
    seriesTagsList = set()
    totalSizeInBytes = 0
    subjects = eform.Eforms(eformsFilePath)
    for study in dataset["studies"]:
        studiesCount += 1
        if not study["subjectName"] in differentSubjects: 
            differentSubjects.add(study["subjectName"])
        if len(study['series']) == 0: continue

        studyPathInDatalake = os.path.join(datalake_mount_path, study['pathInDatalake'])
        _readStudyMetadataFromFirstDicomFileOfAllSeries(studyPathInDatalake, study)
        _getTotalStudySizeInBytes(studyPathInDatalake, study)
        _completeStudyMetadataWithSubjectEform(study, subjects.getEform(study["subjectName"]))

        #Agregate metadata of this study
        if study["ageInDays"] != None:
            if study["ageInDays"] < minAgeInDays:
                minAgeInDays = study["ageInDays"]
                minAgeUnit = study["ageUnit"]
            if study["ageInDays"] > maxAgeInDays:
                maxAgeInDays = study["ageInDays"]
                maxAgeUnit = study["ageUnit"]
        else: ageNullCount += 1
        _aggregateItemToCountDict(sexDict, study["sex"])
        if study["diagnosisYear"] != None:
            if study["diagnosisYear"] < minDiagnosisYear:
                minDiagnosisYear = study["diagnosisYear"]
            if study["diagnosisYear"] > maxDiagnosisYear:
                maxDiagnosisYear = study["diagnosisYear"]
        else: diagnosisYearNullCount += 1
        studyBodyParts = _obtainStudyAggregatedSet(study, "bodyPart")
        studyModalities = _obtainStudyAggregatedSet(study, "modality")
        studyManufacturers = _obtainStudyAggregatedSet(study, "manufacturer")
        _aggregateItemsToCountDict(bodyPartDict, studyBodyParts)
        _aggregateItemsToCountDict(modalityDict, studyModalities)
        _aggregateItemsToCountDict(manufacturerDict, studyManufacturers)
        for series in study["series"]:
            seriesTagsList.update(series["tags"])
        totalSizeInBytes += study["sizeInBytes"]

    # include the size of eforms file
    totalSizeInBytes += os.path.getsize(eformsFilePath)

    dataset["studiesCount"] = studiesCount
    dataset["subjectsCount"] = len(differentSubjects)
    dataset["ageLowInDays"], dataset["ageLowUnit"] = (minAgeInDays, minAgeUnit) if minAgeInDays != MAX_AGE_VALUE else (None, None)
    dataset["ageHighInDays"], dataset["ageHighUnit"] = (maxAgeInDays, maxAgeUnit) if maxAgeInDays != 0 else (None, None)
    dataset["ageNullCount"] = ageNullCount
    dataset["sex"], dataset["sexCount"] = _getValuesAndCountsFromCountDict(sexDict)
    dataset["diagnosisYearLow"] = minDiagnosisYear if minDiagnosisYear != MAX_YEAR_VALUE else None
    dataset["diagnosisYearHigh"] = maxDiagnosisYear if maxDiagnosisYear != 0 else None
    dataset["diagnosisYearNullCount"] = diagnosisYearNullCount
    dataset["bodyPart"], dataset["bodyPartCount"] = _getValuesAndCountsFromCountDict(bodyPartDict)
    dataset["modality"], dataset["modalityCount"] = _getValuesAndCountsFromCountDict(modalityDict)
    dataset["manufacturer"], dataset["manufacturerCount"] = _getValuesAndCountsFromCountDict(manufacturerDict)
    dataset["seriesTags"] = list(seriesTagsList)
    dataset["sizeInBytes"] = totalSizeInBytes
    logging.root.debug("  -studiesCount: %s" % dataset["studiesCount"])
    logging.root.debug("  -subjectsCount: %s" % dataset["subjectsCount"])
    logging.root.debug("  -sex: %s" % dataset["sex"])
    logging.root.debug("  -bodyPart: %s" % dataset["bodyPart"])
    logging.root.debug("  -modality: %s" % dataset["modality"])
    
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
