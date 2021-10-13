import subprocess
import shlex

'''
    fetch: if True, return a list. Otherwise, return string
'''  

def execute_cmd(cmd_string, fetch=True):
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