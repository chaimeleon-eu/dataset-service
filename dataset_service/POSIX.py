import os
import stat

from dataset_service.utils import execute_cmd

def create_dir(path, uid=-1, gid=-1, permissions=0o700):    
    if not os.path.isdir(path):
        os.mkdir(path, permissions) 
        # Change ownership
        if (uid!=-1 and gid!=-1):
            chown(path, uid=int(uid), gid=int(gid)) 

def chmod(path, permissions=0o700):
    os.chmod(path, permissions)
    
def chown(path, uid=-1, gid=-1, recursive=True, follow_symlinks=False):
    os.chown(path, int(uid), int(gid), follow_symlinks=follow_symlinks)
    if recursive:
        for name in os.listdir(path):
            complete_path = os.path.join(path, name)
            if os.path.islink(complete_path):
                chown(path, int(uid), int(gid), recursive=False, follow_symlinks=follow_symlinks)   
            elif os.path.isfile(complete_path):
                chown(path, int(uid), int(gid), recursive=False)   
            elif os.path.isdir(complete_path):
                chown(complete_path, int(uid), int(gid), recursive=True, follow_symlinks=follow_symlinks)
    
def symlink(src, dst, target_is_directory=False, uid=-1, gid=-1):
    status = False
    if not os.path.isfile(dst):
        if os.symlink(src, dst, target_is_directory)==None:
            status = True
            # Change ownership
            if (uid!=-1 and gid!=-1):
                chown(dst, uid=int(uid), gid=int(gid), recursive=False, follow_symlinks=False) 
    return status

def get_acl(path):
    cmd = "getfacl " + path
    output, status = execute_cmd(cmd)
    return output, status

def set_acl_group(group, permissions, path, recursive=True):
    cmd = 'setfacl -'
    if recursive:
        cmd+='R'
    cmd += 'm "g:' + group + ':'+ permissions + '" ' + path
    output, status = execute_cmd(cmd)
    if len(output) > 1:
        return False
    return True

def delete_acl_group (group, path, recursive=True):
    cmd = 'setfacl -'
    if recursive:
        cmd+='R'
    cmd += 'x "g:' + group + '" ' + path
    output, status = execute_cmd(cmd)
    if len(output) > 1:
        return False
    return True

def clean_acl(path, recursive=True):
    cmd = 'setfacl -b '
    if recursive:
        cmd+='-R '
    cmd += path
    output, status = execute_cmd(cmd)
    return True
