import os
import sys
import glob
import IDEAS.base
from IDEAS.db import SQLite
from IDEAS.db.MySQL import MySQLdb
from structure_v1 import APIbase
from structure_v1 import APIquery

def ingest_all(extension="csv", **args):
    #we sort because we want object to come before the one with __
    # NEW mode
    # if a file is object but has an extension it means we append this data
    # let's explore this...
    for filename in sorted(glob.glob("*."+extension)):
        if filename != "schema.csv":
            print filename
            ingest(filename, **args)

def backup_cmd(project, mysql=None, filename="output.sql", tar=True):
    """
    This let's us quickly create a SQL file for database uploading
    """
    import os
    api = APIquery(project, mysql=mysql)
    tables = api.tables()
    config = api.mysql.config
    os.system("mysqldump -h {h} -u {u} -p{p} {db} {tbl} > {filename}".format(
        h=config["host"], 
        u=config["user"], 
        p=config["password"], 
        db=config["database"],
        tbl=" ".join(tables),
        filename=filename))
    if tar:
        os.system("tar -czf {fname}.tar.gz {fname}".format(fname=filename))
        os.system("rm {fname}".format(fname=filename))
    #os.system executes command

def remove_tables(project, mysql=None):
    """
    Remove all tables related to a certain project
    """
    api = APIquery(project, mysql=mysql)
    tables = api.tables()
    for tbl in tables:
        api.mysql.delete(tbl)
    clear_cache(project)

def clear_cache(project, mysql=None):
    api = APIquery(project, mysql=mysql)
    api.mysql.c.execute("""
      DELETE FROM IDEAS_Cached WHERE client="{project}"
      """.format(project=project))

def guess_columns(filename, schema_file="schema.csv", header=True):
    """
    Tables a file and attempts to guess what the tablenames are
    """
    
    sqlite = SQLite.SQLite()
    base = APIbase()

    fields = {}
    col_types = {}
    #if a schema file exists, grab format from that file
    if os.path.exists(schema_file):
        csv=sqlite.csvInput(schema_file, iter=True)
        for c in csv:
            if c[0] not in fields:
                fields[c[0]] = {"variable":None, "type":None, "col_type":{}}
            fields[c[0]][c[1]] = c[2:]
        for key in fields:
            key = fields[key]
            for i in xrange(len(key["variable"])):
                key["col_type"][base._key_convert(key["variable"][i])["var"]] = key["type"][i]
        
        key = filename.split(".csv")[0]
        if key in fields:
            col_types = fields[key]["col_type"]
    
    reserved_words = ["order", "seq_num"]
    sqlite.conn.create_function("tmsql", 1, IDEAS.base.time_mysql)
    csv=sqlite.csvInput(filename, iter=True)
    header = csv.next()
    header = [base._key_convert(h)["var"].lower() for h in header]
    data = []
    for i,row in enumerate(csv):
        if i == 1000:
            break
        data.append(row)
    

    #sqlite.insert(data=data, field=header, reserved_words=reserved_words)
    #cols = [c.lower() for c in sqlite.columns()]
    #for c in cols:
    for i,c in enumerate(header):
        if c not in col_types:
            c = c.lower()
            vals = [x[i] for x in data if len(x) > i]
            if all([x.isdigit() for x in vals if x!=""]):
                col_type = "integer"
            elif all([IDEAS.base.isnumber(x) for x in vals if x!=""]):
                col_type = "real"
            elif all([IDEAS.base.isdate(x) for x in vals if x!=""]):
                col_type = "date"
            else:
                len_vals = [len(x) for x in vals]
                stats = ( max(len_vals), float(sum(len_vals))/len(len_vals) ) 
                if (stats[0] > 100 and stats[1] > 75) or stats[0] > 255:
                    col_type = "text"
                elif stats[0] > 100:
                    col_type = "varchar(256)" 
                elif stats[0] > 50:
                    col_type = "varchar(100)" 
                else:
                    col_type = "varchar(50)"
            col_types[c] = col_type
                    
    return header, col_types


def ingest(filename, tbl=None, tbl_type=None, column_type=None,
           obj_append=False, override=False, skip=False, 
           schema_file="schema.csv"):
    """ 
    Ingest the data into a API friendly form
    The first column must the primary id (for now)

    Args:
        tbl: name of table, if not specified, will default to filename
        tbl_type: Meta, Link or "Other".  If not specified, will attempt
          to guess based on filename
        obj_append: this tells us that this file is merely an append of object
        override: if we ingest a new table, are we removing the previous?
    """

    sqlite = SQLite.SQLite()
    base = APIbase()
    if not tbl:
        tbl = base._jsplit(os.path.split(filename)[1], fr=0, to=-1)
    if not tbl_type:
        tbl_type = base._parse(tbl)["type"].lower()
        
    if tbl_type in ("object", "legend"):
        if base._parse(tbl)["comment"]:
            obj_append = True
            tbl = base._jsplit(tbl, delimiter="_", to=-2)

    # GENERATE A VARIABLE LIST FROM THE FILENAME
    var_list = [x.lower()+"_id" for x in tbl.split("_")][1:-1]

    # CREATE TABLE 
    mysql = IDEAS.base.get_mysql()

    if not obj_append and skip and tbl in mysql.tables():
        print " * Table SKIPPED"
        return
    #if append and the base object doesn't exist. Then this is weird!
    elif obj_append and tbl not in mysql.tables():
        print " * Object Table", tbl, "Needed"
        return 

    cols, col_types = guess_columns(filename, schema_file)
    schema = ["`{c}` {ctype}".format(c=c, ctype=col_types[c]) for c in cols]

    #add a sequence automatically to link
    if tbl_type in base.dict_type["attr"]:
        schema.insert(1, "seq_num INTEGER")

    #-----------------------------------------

    def reformat(col_type, value):
        value = value.strip()
        if col_type == "date":
            return IDEAS.base.time_mysql(value)
        elif col_type in ("real", "integer"):
            if value == "":
                return None
            else:
                return float(value)
        else:
            return MySQLdb.escape_string(value)

    #-----------------------------------------

    mysql.chgTbl(tbl)
    if override and not obj_append:
        mysql.delete(tbl)

    if obj_append:
        obj_cols = mysql.columns(table=tbl)
        for i,c in enumerate(cols):
            if c not in obj_cols:
                #add columns that don't exist
                mysql.c.execute("""
                    ALTER TABLE {tbl} ADD COLUMN {schema}
                    """.format(tbl=tbl, schema=schema[i]))
    elif not mysql.tables(lookup=tbl):
        mysql.c.execute("CREATE TABLE {tbl} ({schema})".format(
            tbl=tbl, schema=", ".join(schema)))

    # use the field names to determine what to index or just the first column
    if tbl_type in base.dict_type["all"]:
        if tbl_type in base.dict_type["main"]:
            unique = True
        else:
            unique = False

        if var_list:
            mysql.index(var_list, unique=unique)
            for v in var_list:
                mysql.index([v], unique=unique)
        else:
            mysql.index([cols[0]], unique=True)
            
    #ppct = -1
    #scount = len(data)
    #print " count:", scount

    csv = sqlite.csvInput(filename, iter=True)
    header = csv.next()
    #for i,r in enumerate(sqlite.fetch(iter=True)):
    for i,r in enumerate(csv):
        r = [reformat(col_types[cols[i2]],r2) for i2,r2 in enumerate(r)]
        #if not r[0]:
            #ppct = IDEAS.base.statusbar(i, scount, ppct=ppct)
            #continue 
        if i%100==0:
            sys.stdout.write("\r * {i}".format(i=i))
            sys.stdout.flush()
            mysql.commit()
        if tbl_type in base.dict_type["main"] and obj_append:
            mysql.c.execute("""
                SELECT * FROM {tbl} WHERE {col}='{value}'""".format(
                    tbl=tbl, col=cols[0], value=r[0]))
            r_old = mysql.c.fetchone()
            if not r_old:
                mysql.insert(r, field=cols)
            else:
                #if the key exists, update the data
                update = []
                for j,v in enumerate(r[1:]):
                    mysql.c.execute("""
                        UPDATE {tbl} SET {f}="{v}" WHERE {_id}="{_v}"
                        """.format(tbl=tbl, f=cols[j+1], v=v, _id=cols[0], _v=r[0]))
            
        elif tbl_type in base.dict_type["main"]:
            mysql.c.execute("""
                SELECT * FROM {tbl} WHERE {col}='{value}'""".format(
                    tbl=tbl, col=cols[0], value=r[0]))
            r_old = mysql.c.fetchone()
            if not r_old:
                mysql.insert(r)
            else:
                r_new = []
                for j in range(0, len(r)):
                    r_new.append(r_old[j] and r_old[j] or r[j])
                mysql.insert(r_new, ignore=False)
        elif tbl_type in base.dict_type["attr"]:
            r_new = list(r)
            mysql.c.execute("""
                SELECT count(*) FROM {tbl} WHERE {col}='{value}'""".format(
                    tbl=tbl, col=cols[0], value=r[0]))
            r_new.insert(1, mysql.c.fetchone()[0])
            mysql.insert(r_new)
        else:
            mysql.insert(r)
        #ppct = IDEAS.base.statusbar(i, scount, ppct=ppct)
    print ""
    mysql.commit()
    mysql.close()
    sqlite.close()

    #for r in sqlite.fetch(iter=True):
    #print cols, schema


