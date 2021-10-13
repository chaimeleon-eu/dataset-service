from POSIX import *

dir_tmp = '/home/serlophug/'
file_tmp_acl = '/home/serlophug/mpi-example.yaml'

print ("Retriving ACL rules:")
print(get_acl(file_tmp_acl))

print ("Setting the ACL for GID=10000 to RX")
print(set_acl_group("10000", "rx", file_tmp_acl))

print ("Retriving ACL:")
print(get_acl(file_tmp_acl))

print ("Cleaning ACL rules")
print(clean_acl(file_tmp_acl))



print(create_dir(dir_tmp+'dir1', uid=1000, gid=1000))

print(symlink(file_tmp_acl, dir_tmp+'dir1/file' ))

print(create_dir(dir_tmp+'dir1/dir2', uid=1000, gid=1000))

print(create_dir(dir_tmp+'dir1/dir2/dir3', uid=1000, gid=1000))

print(symlink(file_tmp_acl, dir_tmp+'dir1/dir2/file2' ))

print(symlink(file_tmp_acl, dir_tmp+'dir1/dir2/dir3/file3' ))

print(chown(dir_tmp+'dir1/dir2/', uid=111, gid=111, recursive=True, follow_symlinks=False))

print(create_dir(dir_tmp+'dir1/dir2/dir3/dir4', uid=1000, gid=1000))
