import shutil
from dataset_service.POSIX import *
import logging
import dataset_service.utils

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
        ok = symlink(linkDestination, linkLocation, target_is_directory=True, uid=owner_uid, gid=owner_gid)
        if not ok: 
            logging.root.error("Error creating symlink (probably already exists): " + linkLocation + " -> " + linkDestination)
            raise DatasetException("Error creating symlink (probably already exists)")
            
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
