from POSIX import *
import utils

'''
Creates the dataset directory, populates him with the symbolic links and applies the ACL rules to the files.
PARAMS:
    parent_dir                  # Folder to store the datasets. Example: "/mnt/cephfs/datasets"
    dataset_name                # Dataset name. Example: "3d44ba8a-16df-11ec-ac5f-00155d6b30e6"
    rootfs_dir                  # Mount point of the data for applying ACL to files. Example: "/mnt/cephfs/"
    rootfs_mount_point          # Mount point of the data to create the symbolic links. Example: "/mnt/rootfs/"
    relative_paths_list         # Relative paths from $rootfs_mount_point to create the symbolic links. Example: "[ persona1/estudio1, persona2/estudio1 ]"
    acl_permissions             # Example: "rx"
    acl_gid                     # GID to apply ACL_permissions. Example: "1000" (dataset_group)
    owner_uid = -1              # UID de quien lo crea. Example: "0" (root)    
    owner_gid = -1              # GID de quien lo crea. Example: "0" (root)   
    
'''
def create_dataset (parent_dir, dataset_name, rootfs_dir, rootfs_mount_point, relative_paths_list, acl_permissions, acl_gid, owner_uid=-1, owner_gid=-1):
    status = False
    dataset_dir = os.path.join(parent_dir, dataset_name)
    
    # Create dataset directory
    if create_dir(dataset_dir, uid=owner_uid, gid=owner_gid):
        status = True

        # Create symlinks
        counter = 1
        for src in relative_paths_list:
            
            symlink_name = 'f%d-%s' % ( counter, os.path.basename(src) ) # f1-estudio1, f2-estudio1
            counter += 1
            
            symlink_path = os.path.join(dataset_dir, symlink_name)
            file_path = os.path.join(rootfs_dir, src)
            mounted_src = os.path.join(rootfs_mount_point, src)

            ok = symlink(mounted_src, symlink_path, target_is_directory=False)
            
            if ok:
                # Change ownership of the symbolic link file
                ok &= chown(symlink_path, uid=owner_uid, gid=owner_gid, follow_symlinks=False, recursive=False)
                #ok &= chmod(symlink_path, permissions=0o700) # Changing permissions is not possible in symbolic links
                
                

                # ACL to the file (not the symbolic link)
                ok &= set_acl_group(str(acl_gid), str(acl_permissions), file_path, recursive=False) 

                # ACL to the directory that contains the file
                ok &= set_acl_group(str(acl_gid), str(acl_permissions), os.path.dirname(file_path), recursive=False) 

                status &= ok
            
        #Change ownership of the symbolic link files in the dataset directory 
        #status &= chown(dataset_dir, uid=owner_uid, gid=owner_gid, follow_symlinks=False, recursive=True) -> NOT WORK

        # ACL to the dataset folder 
        status &= set_acl_group(str(acl_gid), str(acl_permissions), dataset_dir, recursive=True) 

    return status  


'''
Invalidate a dataset removing the correspondent ACL rule that enables accessing the files from a group (the dataset).
PARAMS:
    rootfs_dir                  # Mount point of the data for applying ACL to files. Example: "/mnt/cephfs/"
    relative_paths_list         # Relative paths from $rootfs_mount_point to create the symbolic links. Example: "[ persona1/estudio1, persona2/estudio1 ]"
    acl_gid                     # GID to apply ACL_permissions. Example: "1000" (dataset_group)

'''
def invalidate_dataset(rootfs_dir, relative_paths_list, acl_gid):
    status = True

    for src in relative_paths_list:
        path = os.path.join(rootfs_dir, src)
        # Remove ACL from file
        status &= delete_acl_group (str(acl_gid), path, recursive=False)
        # Remove ACL from directory
        status &= delete_acl_group (str(acl_gid), os.path.dirname(path), recursive=False)

    return status


