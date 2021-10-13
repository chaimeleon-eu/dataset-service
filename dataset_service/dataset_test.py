from POSIX import *
from dataset import *

import utils

parent_dir= "/opt/ubuntu/serlophug/cephfs/testing/datasets"
dataset_name="dataset_prueba"
rootfs_dir="/opt/ubuntu/serlophug/cephfs/testing/"                  
rootfs_mount_point="/mnt/rootfs/" 
relative_paths_list= ['persona1/estudio1','persona2/estudio2', 'persona5/estudio3']         # Relative paths from $rootfs_mount_point to create the symbolic links. Example: "[ persona1/estudio1, persona2/estudio1 ]"
acl_permissions= "rx"           
acl_gid = "1500"
owner_uid = "0"
owner_gid = "0" 

'''
MAIN
'''
def create():
    global parent_dir, dataset_name, rootfs_dir, rootfs_mount_point, relative_paths_list, acl_permissions, acl_gid, owner_uid, owner_gid
     
    print("\n---------------------------------------------- \n")
    print("DATASET NAME: " + dataset_name)
    print("DATASET GID: " + acl_gid)
    print("DATASET DIR: " + parent_dir + "/" + dataset_name)

    print("\n---------------------------------------------- \n")
    status = create_dataset(parent_dir, dataset_name, rootfs_dir, rootfs_mount_point, relative_paths_list, acl_permissions, acl_gid, owner_uid=owner_uid, owner_gid=owner_gid)    
    print("Dataset %s created succsessfully --> %r" % ( dataset_name, bool(status) ))

    print("\n---------------------------------------------- \n")
    dataset_path = os.path.join(parent_dir, dataset_name)
    print ( ('Symlinks created in dataset: %s') % (", ".join( str(x) for x in os.listdir(dataset_path))) )
    print("ACL rules from files in dataset:")
    # Check ACL from files in dataset
    for src in relative_paths_list:
        path = os.path.join(rootfs_dir, src)
        print (" ".join( str(x) for x in get_acl(path)))

    print("\n---------------------------------------------- \n")

def delete():
    global parent_dir, dataset_name, rootfs_dir, rootfs_mount_point, relative_paths_list, acl_permissions, acl_gid, owner_uid, owner_gid
    dataset_path = os.path.join(parent_dir, dataset_name)

    status = invalidate_dataset(rootfs_dir, relative_paths_list, acl_gid)
    print("Dataset %s invalidated succsessfully --> %r" % (dataset_name, bool(status) ))

    print("\n---------------------------------------------- \n")
    print("ACL rules from files in dataset:")
    # Check ACL from files in dataset
    for src in relative_paths_list:
        path = os.path.join(rootfs_dir, src)
        print ( " ".join( str(x) for x in get_acl(path)) )
    
    print("\n---------------------------------------------- \n")
    print("Removing dataset (directory with the symbolic links)....")
    
    for src in os.listdir(dataset_path):
        path = os.path.join(dataset_path, src)
        utils.execute_cmd("rm %s" % (path))
    utils.execute_cmd("rmdir %s" % (dataset_path))

    print("\n---------------------------------------------- \n")


#
#delete()
create()


parent_dir= "/opt/ubuntu/serlophug/cephfs/testing/datasets"
dataset_name="dataset_prueba2"
rootfs_dir="/opt/ubuntu/serlophug/cephfs/testing/"                  
rootfs_mount_point="/mnt/rootfs/" 
relative_paths_list= ['persona1/estudio2','persona3/estudio2', 'persona6/estudio3']         # Relative paths from $rootfs_mount_point to create the symbolic links. Example: "[ persona1/estudio1, persona2/estudio1 ]"
acl_permissions= "rx"           
acl_gid = "1501"
owner_uid = "0"
owner_gid = "0" 

create()
#delete()