from dataset_service.POSIX import *
import logging
import dataset_service.utils

class DatasetException(Exception):
    pass

'''
Creates the dataset directory, populates him with the symbolic links and applies the ACL rules to the files.
PARAMS:
    datasets_dir_path           # Folder to store the datasets. Example: "/mnt/cephfs/datasets"
    dataset_dir_name            # Dataset name. Example: "3d44ba8a-16df-11ec-ac5f-00155d6b30e6"
    datalake_dir_path           # Folder where datalake is mounted. Example: "/mnt/cephfs/datalake"
    studies                     # List of studies selected for the dataset"
    acl_permissions             # Example: "rx"
    acl_gid                     # GID to apply ACL_permissions. Example: "1000" (dataset_group)
    owner_uid = -1              # UID de quien lo crea. Example: "0" (root)    
    owner_gid = -1              # GID de quien lo crea. Example: "0" (root)   
'''
def create_dataset (datasets_dir_path, dataset_dir_name, datalake_dir_path, studies, acl_permissions, acl_gid, owner_uid=-1, owner_gid=-1):
    dataset_dir = os.path.join(datasets_dir_path, dataset_dir_name)
    # Create dataset directory
    #os.mkdir(dataset_dir)
    create_dir(dataset_dir, uid=owner_uid, gid=owner_gid)
    
    for study in studies:
        # study['path'] example: blancagomez/01_Neuroblastoma_4_Neuroblastoma/TCPEDITRICOABDOMINOPLVICOCONCONTRASTE20150129/
        studyDirName = os.path.basename(study['path'])
        subjectDirName = study['subjectName']
        subjectDirPath = os.path.join(dataset_dir, subjectDirName)
        if not os.path.exists(subjectDirPath):
            create_dir(subjectDirPath, uid=owner_uid, gid=owner_gid)

        linkLocation = os.path.join(subjectDirPath, studyDirName)
        linkDestination = os.path.join(datalake_dir_path, study['path'])
        ok = symlink(linkDestination, linkLocation, target_is_directory=True, uid=owner_uid, gid=owner_gid)
        if not ok: 
            logging.root.error("Error creating symlink: " + linkLocation + " -> " + linkDestination)
            raise DatasetException("Error creating symlink")
        
        # ACL to the destination study directory (not the symbolic link)
        ok = set_acl_group(str(acl_gid), str(acl_permissions), linkDestination, recursive=True) 
        if not ok: 
            logging.root.error("Error in set acl to: " + linkDestination)
            raise DatasetException("Error in set acl.")

        # # ACL to the directory that contains the file
        # ok &= set_acl_group(str(acl_gid), str(acl_permissions), os.path.dirname(linkDestination), recursive=False) 

    # ACL to the dataset folder 
    ok = set_acl_group(str(acl_gid), str(acl_permissions), dataset_dir, recursive=True)
    if not ok: 
        logging.root.error("Error in set acl to: " + dataset_dir)
        raise DatasetException("Error in set acl.")
    


'''
Invalidate a dataset removing the correspondent ACL rule that enables accessing the files from a group (the dataset).
PARAMS:
    rootfs_dir                  # Mount point of the data for applying ACL to files. Example: "/mnt/cephfs/"
    relative_paths_list         # Relative paths from $rootfs_mount_point to create the symbolic links. Example: "[ persona1/estudio1, persona2/estudio1 ]"
    acl_gid                     # GID to apply ACL_permissions. Example: "1000" (dataset_group)
'''
def invalidate_dataset(rootfs_dir, relative_paths_list, acl_gid):
    for src in relative_paths_list:
        path = os.path.join(rootfs_dir, src)
        # Remove ACL from file
        ok = delete_acl_group (str(acl_gid), path, recursive=False)
        if not ok: 
            logging.root.error("Error in delete acl to: " + path)
            raise DatasetException("Error in delete acl.")
        # # Remove ACL from directory
        # ok = delete_acl_group (str(acl_gid), os.path.dirname(path), recursive=False)


