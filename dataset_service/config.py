import logging
import yaml
import sys

def check_config_items (parent_key, ok_config, config):
    #global LOG
    ok = True
    items = config.keys()
    for k,v in ok_config.items():
        if k not in items:
            #print("k = %s, items = %s"%(k, str(items)))
            print("%s not defined in %s section. Required values=%s, obtained=%s"%(k, parent_key, ok_config.keys(), items)) 
            ok = False
            break
        
        if type(v) == type(config.get(k)):
            if type(v) == dict:
                ok = check_config_items(k, v, config.get(k))
        else:
            ok = False
            print("type of %s['%s'] must be %s" % (parent_key, k, type(v)) )
        
        if not ok:
            break        
    return ok

class Config:
    creation_dict = None
    def __init__(self, dictionary={}):
        self.creation_dict = dictionary
        for k, v in dictionary.items():
            if type(v) == dict:
                setattr(self, k, Config(v))
            else:
                setattr(self, k, v)


