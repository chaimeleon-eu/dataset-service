import subprocess
import shlex
import urllib.parse

def execute_cmd(cmd_string, fetch=True):
    '''
    fetch: if True, return a list. Otherwise, return string.
    '''  
    #print ('Executing "' + str(cmd_string) + '"' )
    args = shlex.split(str(cmd_string))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    rc = p.returncode
    output = err
    if rc == 0:
        output= out
    if fetch:
        output = output.decode("utf-8").split('\n')
    return output, rc

def download_url(url, destinationFilePath, maxFileSizeMBytes=None):
    url = shlex.quote(url)                # quote for securing agains injection attack
    destinationFilePath = shlex.quote(destinationFilePath)
    if maxFileSizeMBytes is None:
        output, status = execute_cmd("wget -O " + destinationFilePath + " " + url)
        return
    
    # max_file_size_bytes = str(maxFileSizeMBytes*1024*1024)
    # output, status = execute_cmd("curl --max-filesize "+ max_file_size_bytes +" -o " + destinationFilePath + " " + url)
        # The problem with curl is --max-filesize is ignored when the server does not include the "content-length header"
    # output, status = execute_cmd("wget -Q8m -O " + destinationFilePath + " " + url)
        # The problem with wget is -Q (quota) seems not working for one file, it is for web download with many files
    # So, let's use the ulimit command from bash shell...
    max_file_size_kb = str(maxFileSizeMBytes*1024)
    bash_cmd = shlex.quote("(ulimit -f "+max_file_size_kb+"; wget -O " + destinationFilePath + " " + url + ")")
    output, status = execute_cmd("bash -c "+bash_cmd)
    return 

def is_valid_url(url: str, empty_path_allowed: bool = True) -> bool:
    try:
        res = urllib.parse.urlparse(url)
        if not all([res.scheme in ['http','https'], res.netloc]): return False
        if empty_path_allowed: return True
        else: return bool(res.path)
    except Exception:
        return False
