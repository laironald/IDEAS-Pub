import os
import sys
import time

import ConfigParser
import IDEAS.config as _iCfg
try:
    from IDEAS.db import MySQL
except:
    pass

#---------------------------------------------
# GET CONFIGS

def get_config(filename="config.cfg"):
    if os.path.isfile(filename):
        cfg = ConfigParser.ConfigParser()
        cfg.read(filename)
        for s in cfg.sections():
            if cfg.items(s):
                if s not in _iCfg.config:
                    _iCfg.config[s] = {}
                for k,v in cfg.items(s):
                    _iCfg.config[s][k] = v
    return _iCfg.config
    
def get_mysql(filename="config.cfg", key="mysql"):
    try:
        return MySQL.MySQL(get_config(filename)[key])
    except:
        pass

#---------------------------------------------
# NUMERICAL TYPES

def isdate(string):
    # date like 1/1/2001 (per CSV formatting)
    if len(string.split("/")) != 3:
        return False
    for s in string.split("/"):
        if not(s.isdigit()):
            return False
    return True

def isnumber(string):
    import re
    p = re.compile('\d+(\.\d+)?')
    s = p.match(string)
    if not s:
        return False
    else:
        return s.group() == s.string

def time_mysql(string):
    try:
        if string=="":
            return ""
        if len(string.split("/")[-1]) == 4:
            t = time.strptime(string, "%m/%d/%Y")
        elif len(string.split("/")[-1]) == 2:
            t = time.strptime(string, "%m/%d/%y")
        return "{year}-{month}-{day}".format(
            year=t.tm_year, month=t.tm_mon, day=t.tm_mday)
    except: #catch weird ones like 31/11/12
        print " * Error?", string
        return ""
        

#---------------------------------------------
# UX Related

def statusbar(current, total, adjust=1., size=25, sleep=None, ppct=None):
    import time
    if total > 0:
        pct = min(1.0, float(current+adjust)/total)
    else:
        return
    if ppct:
        if int(100*pct) == int(100*ppct):
            return ppct

    sys.stdout.write("\rstatus [{bar}]  {i}%".format(
        i=int(100*pct), 
        bar="="*(int(size*pct))+" "*(size-int(size*pct))))
    sys.stdout.flush()
    if sleep:
        time.sleep(sleep)
    if (current+adjust)>=total:
        print ""
    return pct
#---------------------------------------------

