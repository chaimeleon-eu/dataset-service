from dataset_service.POSIX import *
import logging
import dataset_service.utils

class DatasetException(Exception):
    pass

def create_dataset (datasets_dir_path, dataset_dir_name, datalake_dir_path, studies):
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
    dataset_dir = os.path.join(datasets_dir_path, dataset_dir_name)
    # Create dataset directory
    #os.mkdir(dataset_dir)
    create_dir(dataset_dir, uid=owner_uid, gid=owner_gid, permissions=0o700)
    # Now only root have access. The access to normal users will be granted later with ACLs.
    
    for study in studies:
        # study['path'] example: blancagomez/01_Neuroblastoma_4_Neuroblastoma/TCPEDITRICOABDOMINOPLVICO20150129/
        studyDirName = os.path.basename(study['path'])
        subjectDirName = study['subjectName']
        subjectDirPath = os.path.join(dataset_dir, subjectDirName)
        if not os.path.exists(subjectDirPath):
            create_dir(subjectDirPath, uid=owner_uid, gid=owner_gid, permissions=0o705)
            # At this level all the people have read access, the control with ACLs is done in the upper level.

        linkLocation = os.path.join(subjectDirPath, studyDirName)
        linkDestination = os.path.join(datalake_dir_path, study['path'])
        ok = symlink(linkDestination, linkLocation, target_is_directory=True, uid=owner_uid, gid=owner_gid)
        if not ok: 
            logging.root.error("Error creating symlink: " + linkLocation + " -> " + linkDestination)
            raise DatasetException("Error creating symlink")
        chmod(linkDestination, 0o700)
        # Ensure only root have access. The access to normal users will be granted later with ACLs.
        for name in os.listdir(linkDestination):
            chmod_recursive(os.path.join(linkDestination, name), dirs_permissions=0o705, files_permissions=0o604)
            # At this level all the people have read access, the control with ACLs is done in the upper level.


def give_access_to_dataset (datasets_dir_path, dataset_dir_name, datalake_dir_path, studies, acl_gid):
    '''
    Applies the ACL rules to the files.
    PARAMS:
        datasets_dir_path           # Folder to store the datasets. Example: "/mnt/cephfs/datasets"
        dataset_dir_name            # Dataset name. Example: "3d44ba8a-16df-11ec-ac5f-00155d6b30e6"
        datalake_dir_path           # Folder where datalake is mounted. Example: "/mnt/cephfs/datalake"
        studies                     # List of studies selected for the dataset
        acl_gid                     # GID to apply ACL_permissions. Example: "1000" (dataset_group) 
    '''
    acl_permissions = 'rx'
    dataset_dir = os.path.join(datasets_dir_path, dataset_dir_name)
    # ACL to the dataset folder 
    ok = set_acl_group(str(acl_gid), acl_permissions, dataset_dir, recursive=False)
    if not ok: 
        logging.root.error("Error in set acl to: " + dataset_dir)
        raise DatasetException("Error in set acl.")

    for study in studies:
        # study['path'] example: blancagomez/01_Neuroblastoma_4_Neuroblastoma/TCPEDITRICOABDOMINOPLVICO20150129/
        linkDestination = os.path.join(datalake_dir_path, study['path'])
        # ACL to the destination study directory (not the symbolic link)
        ok = set_acl_group(str(acl_gid), acl_permissions, linkDestination, recursive=False) 
        if not ok: 
            logging.root.error("Error in set acl to: " + linkDestination)
            raise DatasetException("Error in set acl.")

        # # ACL to the directory that contains the file
        # ok &= set_acl_group(str(acl_gid), str(acl_permissions), os.path.dirname(linkDestination), recursive=False) 
    

def invalidate_dataset(datasets_dir_path, dataset_dir_name, rootfs_dir, relative_paths_list):
    '''
    Removes the ACL rules that enables accessing the files from users.
    PARAMS:
        datasets_dir_path           # Folder to store the datasets. Example: "/mnt/cephfs/datasets"
        dataset_dir_name            # Dataset name. Example: "3d44ba8a-16df-11ec-ac5f-00155d6b30e6"
        rootfs_dir                  # Mount point of the data for applying ACL to files. Example: "/mnt/cephfs/"
        relative_paths_list         # Relative paths from $rootfs_mount_point to create the symbolic links. Example: "[ persona1/estudio1, persona2/estudio1 ]"
    '''
    return False
    dataset_dir = os.path.join(datasets_dir_path, dataset_dir_name)
    ok = delete_all_acls(dataset_dir, recursive=False)
    if not ok: 
        logging.root.error("Error in delete acls to: " + dataset_dir)
        raise DatasetException("Error in delete acls.")

    ## Delete ACLs in directories of studies
    ## It is complex because many datasets can include the same study.
    ## To do
    # for src in relative_paths_list:
    #     path = os.path.join(rootfs_dir, src)
    #     # Remove ACL from file
    #     ok = delete_acl_group (str(acl_gid), path, recursive=False)
    #     if not ok: 
    #         logging.root.error("Error in delete acl to: " + path)
    #         raise DatasetException("Error in delete acl.")
    #     # # Remove ACL from directory
    #     # ok = delete_acl_group (str(acl_gid), os.path.dirname(path), recursive=False)
