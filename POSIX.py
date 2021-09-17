import os
import stat

from utils import execute_cmd

def create_dir(path, uid=-1, gid=-1, permissions=0o700):
    # Create dir    
    status = False

    if not os.path.isdir(path):
        try: 
            os.mkdir(path, permissions) 
            status = True
        except OSError as error: 
            print("POSIX.create_dir error:" + str(error)) 
            status = False

    
    # Change ownership
    if (uid!=-1 and gid!=-1 and status):
        status = chown(path, uid=int(uid), gid=int(gid)) 

    return status

def chmod(path, permissions=0o700):
    try:
        os.chmod(path, permissions)
    except OSError as error: 
        print("POSIX.chmod error:" + str(error))
        return False
    return True

def chown(path, uid=-1, gid=-1, recursive=True, follow_symlinks=False):
    status = True
    try:
        os.chown(path, int(uid), int(gid), follow_symlinks=follow_symlinks)
        if recursive:
            for name in os.listdir(path):
                complete_path = os.path.join(path, name)
                if os.path.islink(complete_path):
                    status &= chown(path, int(uid), int(gid), recursive=False, follow_symlinks=follow_symlinks)   
                elif os.path.isfile(complete_path):
                    status &= chown(path, int(uid), int(gid), recursive=False)   
                elif os.path.isdir(complete_path):
                    status &= chown(complete_path, int(uid), int(gid), recursive=True, follow_symlinks=follow_symlinks)
                   
    except OSError as error: 
        print("POSIX.chown error:" + str(error))
        return False
    return True

def symlink(src, dst, target_is_directory=False):
    status = False
    if not os.path.isfile(dst):
        if os.symlink(src, dst, target_is_directory)==None:
            status = True
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
