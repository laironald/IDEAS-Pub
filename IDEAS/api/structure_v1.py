"""
Considerations:
  Critical:
    Data errors: if one occurs, we could notify the user immediately!
    Now it creates some memory locks...
    ie. http://localhost:5000/anu/person/I9726?debug --- locks the data!

  Caching? Is there something we can do to improve the speed of the application?
   ie. some dynamic queries are slow... how do we pre-calculate these items?
       rule should be first time calc can be slower (its to be expected) --
       but future iterations shouldn't regenerate? this could create some issues
       and really expand our datasets?
    *we made a mini memcache like thing, but now we need to do something for 
     the chainable functions
    *grouping likely is difficult to cache. think about this as its going
     to be a messy thing to deal with. perhaps return more results as a way
     to sorta deal with it? (in the short term)

  Data comparison errors
    * In ingest process, build in mechanisms to test for problematic missing data**
      ie. Pub_Person has 22557 unique Pub_ID but Pub_Meta finds only 21861 of those
  chain_order/chain_group can probably be optimized...?
   * these two share similar code. refactoring probably in order
  no geographic based searching right now (its not hard, just don't have data)
  no text based searching implemented right now
   * likely basic regex or something similar as first pass then SOLR?
  grouping -- can we do distinct? for example how many distinct inventors have x
  change size of json return? currently 25 records
  comparison of dates? how do we do this... between 2009-01-01 and 2010-02-05?
    the - confuses things in this context

Decisions:
  Allowing {"top":2} to translate to {"seq_num + 1":"<=2"}
   * for query_filter, chain_order and query_group
  Pages to start from 1 rather than 0 (reflected in chain_fetch)
  Grouping returns results based on descending count
   * can further chain grouping

Next: create APIcache
"""

# by importing IDEAS.base, we also automatically load mysql
import IDEAS.base
import IDEAS.lib.inflect
import IDEAS.lib.sqlparse as sqlparse
import copy, datetime, math, re
import jsonConvert

#enables us to some grammar based things
grammar = IDEAS.lib.inflect.engine()
version = "v1"

class APIcache:
    """ 
    build a cache for the data
    key, value, size, date, freq
    """
    _mega = 1024**2
    max_cache = _mega * 100
    #max_cache = 100000
    cache_name = "IDEAS_Cached"
    expire = "14 DAY"
    #expire = "10 SECOND"

    def __init__(self, mysql=None, cache_name=None, max_cache=None, expire=None):
        """
        Initialize our Caching mechanism
        
        Args: 
          mysql: database connection
          cache_name: useful if we want to change the cache_name
          max_cache:  useful if we want to change the max_cache
          expire: useful if we want to change the expire time
        """
        self.mysql = mysql
        #----------------- RESET OUR VARIABLES IF DESIRED
        if cache_name:
            self.cache_name = cache_name
        if max_cache:
            self.max_cache = max_cache
        if expire:
            self.expire = expire    
        #-----------------         
        cache_name = self.cache_name
            
        if not self.mysql.tables(cache_name):
            self.mysql.c.execute("""
              CREATE TABLE IF NOT EXISTS {cache_name} (
                id      MEDIUMINT NOT NULL AUTO_INCREMENT,
                version VARCHAR(64),
                client  VARCHAR(128),
                obj     VARCHAR(64),
                path    VARCHAR(512),
                value   MEDIUMTEXT,
                size    INTEGER,
                date    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expire  TIMESTAMP,
                freq    INTEGER DEFAULT 0,
                PRIMARY KEY (id)
              )""".format(cache_name=cache_name)) #"""
            self.mysql.index(["version", "client", "obj", "path"], 
                table=cache_name, unique=True)
            self.mysql.index(["client"], table=cache_name)
            self.mysql.index(["obj"], table=cache_name)
            self.mysql.index(["path"], table=cache_name)
            self.mysql.index(["size"], table=cache_name)
            self.mysql.index(["freq"], table=cache_name)

    def _get_sum(self):
        """
        Get total size of cached data (based on string length)
        """
        self.mysql.c.execute("""
            SELECT sum(size) FROM {cache_name}
            """.format(cache_name=self.cache_name))
        cache_total = self.mysql.c.fetchone()[0]
        if not cache_total:
            cache_total = 0        
        return cache_total

    def _where_path(self, version, client, obj, path):
        """ 
        Since we have tons of vars, returns a convenient where string
        """
        return """
            version="{v}" AND client="{c}" AND obj="{o}" AND path="{p}"
            """.format(v=version, c=client, o=obj, p=path)

    def _reset_cache(self, reset=True):
        """
        Reset the cache
        """
        self.mysql.c.execute(""" 
            DROP TABLE IF EXISTS {cache_name}
            """.format(cache_name=self.cache_name))
        if reset:
            self.__init__(self.mysql)                

    #NOTE VALUES ARE NOT GETTING DELETED. WTF!?
    def fetch(self, version, client, obj, path):
        """ 
        Check against the cache to see if data exists

        Args:
          version: version of the data to check against
          client: client to check against
          obj:  primary object
          path: this item with above creates the key
        """
        cache_name = self.cache_name
        w_path = self._where_path(version, client, obj, path)
        try:
            self.mysql.c.execute("""
                SELECT count(*) FROM {cache_name} WHERE {w_path}
                """.format(cache_name=cache_name, w_path=w_path)) #"""
        except:
            #if structure changed, replace it and start a new
            self._reset_cache()
            return None

        if self.mysql.c.fetchone()[0] == 0:
            #does the key exist? if not... no cache available
            return None
        else:
            self.mysql.c.execute("""
                SELECT  count(*) 
                  FROM  {cache_name} 
                 WHERE  {w_path} AND now()>expire
                """.format(cache_name=cache_name, w_path=w_path)) #"""
            if self.mysql.c.fetchone()[0] > 0:
                #if key exist but expired, delete it
                self.mysql.c.execute("""
                    DELETE FROM {cache_name} WHERE {w_path}
                    """.format(cache_name=cache_name, w_path=w_path))
                return None
            else:
                #just return it
                self.mysql.c.execute("""
                    UPDATE {cache_name} SET freq = freq + 1 WHERE {w_path}
                    """.format(cache_name=cache_name, w_path=w_path)) #"""
                self.mysql.c.execute("""
                    SELECT value FROM {cache_name} WHERE {w_path}
                    """.format(cache_name=cache_name, w_path=w_path)) #"""
                return self.mysql.c.fetchone()[0]

    def insert(self, version, client, obj, path, value):
        """ 
        Insert data into the cache. Resets count if it exists

        Args:
          version: version of the data to check against
          client: client to check against
          obj:  primary object
          path: this item with above creates the key
          value: the data to store
        """
        cache_name = self.cache_name
        cache_size = len(value)
        cache_total = self._get_sum()
        w_path = self._where_path(version, client, obj, path)
    
        #delete stuff from cache if cache_size is surpassed
        #  lazy way to do this, should eventually incorporate frequency
        #  as an adjustment
        if cache_total + cache_size > self.max_cache:
            self.mysql.c.execute("""
                DELETE FROM {cache_name} WHERE now()>expire
                """.format(cache_name=cache_name))
            cache_total = self._get_sum()
            self.mysql.c.execute("""
                SELECT id, size FROM {cache_name} ORDER BY expire
                """.format(cache_name=cache_name))
            id_list = self.mysql.c.fetchall()
            for id_num in id_list:
                if cache_total + cache_size > self.max_cache:
                    cache_total = cache_total - id_num[1]
                    self.mysql.c.execute("""
                        DELETE FROM {cache_name} WHERE id={id_num}
                        """.format(cache_name=cache_name, id_num=id_num[0]))
                else:
                    break

        self.mysql.c.execute(""" 
            REPLACE INTO {cache_name} 
                    (version, client, obj, path, value, size) 
             VALUES (%s, %s, %s, %s, %s, %s)
            """.format(cache_name=cache_name), 
            [version, client, obj, path, value, cache_size])
        self.mysql.c.execute("""
            UPDATE  {cache_name} 
               SET  freq = freq + 1,
                    expire = ADDDATE(date, INTERVAL {expire})
             WHERE  {w_path}
            """.format(cache_name=cache_name, 
                       w_path=w_path, expire=self.expire)) #"""

#-----------------------------------------------------

class APIbase:
    pagesize = 50
    keywords = ["seq_num", "all", "top"]
    # delete META in next version
    dict_type = {
        "all": ["link", "list", "legend", "object", "meta"],
        "object": "object",
        "legend": "legend",
        "main": ["legend", "object", "meta"],
        "attr": ["link", "list"],
        "relation": {0:"sibling", -1:"parent", 1:"child"},
        None: "" #add this since get_type returns None
    }
    
    debug = False
    chain = 0
    chainobj = None
    sql_string = ""

    def __init__(self):
        pass

    def _islist(self, data):
        return type(data).__name__ in ('list', 'tuple')

    def _isstr(self, data):
        return type(data).__name__ in ('str', 'unicode')

    tbl_parse = {}
    def _parse(self, table):
        """
        Splits up the name of the table into its project, objects and type
        the primary delimter is _ but further seperated by __ or -
        
        Args:
          table: name of table that we are passing in
        Returns: A dictionary with the keys project, objects and type
        Tested? Yes

        Check out the testing to see the inputs and outputs
        """

        def object_split(string):
            string = string.split("_")[-1]
            for typ in self.dict_type["all"]:
                if string.find(typ) == 0:
                    return {"type":typ, "descrip":string[len(typ):]}
            return False
            
        table = table.lower().replace("-", "__")
        table = table.split("__")
        #its a caching routine. for minor speed optimizations
        # CACHING HAS AN ERROR? TODO
        #if "__".join(table) in self.tbl_parse:
        #    return self.tbl_parse["__".join(table)]
        
        index = 0
        if len(table) == 2 and \
           table[1].count("_") >= 2 and object_split(table[1]):
            index = 1
        elif len(table) == 3:
            index = 1

        struct = {"super":"", "comment":""}
        if index > 0:
            struct["super"] = table[0]
        cont = table[index].split("_")
        struct.update({"project": cont[0], "objects": cont[1:-1]})
        struct.update(object_split(cont[-1]))
        if len(table) > (index + 1):
            struct["comment"] = table[-1]

        #Caching appears to have issues. TODO
        #self.tbl_parse["__".join(table)] = struct
        return struct

    def _param_where(self, key, value):
        """
        Takes a key and its value, converts it to something usable by SQL
        Currently supports between (1-10), greater/less, ><= (>10), IN (1,2,3)
          not equal (<>)
        
        Args:
          key: the name of the variable being considered
          value: the condition applied to the value
        Returns: the SQL code
        Tested? Yes
        """
        key = self._key_convert(key)["key"]
        value = str(value).replace("'", "\'")
        if value.count(",") > 0:
            query = "{key} IN ('{value}')"
            value = "','".join(value.split(","))
        elif re.match("^[0-9.]+-[0-9.]+$", value):
            query = "{key} BETWEEN '{value}'"
            value = "' AND '".join(value.split("-"))
            #if we want ALPHANUMERIC BETWEEN MAYBE >AB-<BC ?
        elif re.match("^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{4}-[0-9]{2}-[0-9]{2}$", 
             value):
            query = "{key} BETWEEN '{value}'"
            value = "' AND '".join([
                self._jsplit(value, "-", to=3), 
                self._jsplit(value, "-", fr=3)]) 
            #this let's us test a date range
        elif re.match("^[=><]+", value):
            sym = re.findall("^[=><]+", value)[0]
            query = "{key}"+sym+"'{value}'"
            value = value[len(sym):]
        # this is a basic search
        elif value.find("*") >= 0:
            value = "%".join(value.split("*"))
            query = "{key} LIKE '{value}'".format(key=key, value=value)
        else:
            query = "{key}='{value}'"
        return query.format(key=key, value=value)

    def _jsplit(self, string, delimiter=".", fr=None, to=None):
        """ 
        Convenience function
        """
        return delimiter.join(string.split(delimiter)[fr:to])

    def _is_leg(self, obj, table):
        return self.get_type(obj) == "legend" and \
               self._parse(table)["type"] == "legend"

    def _key_convert(self, key, delim="__"):
        """
        Standardizes keys and breaks it into its many parts
        Args:
          key: the key value that we would like to standardize
        Returns:
          A dictionary that shows the aggregate function (if exists),
          the original key input, the primary variable manipulated and
          sub-options

        Tested? Yes
        """
        #check for aggregates like year(end_date)
        key = delim.join(key.split("."))
        #opt is like var DESC, the DESC is the opt
        if len(key.split(" ")) == 1:
            opt = ""
        else:
            opt = key.split(" ")[1]
        agg = re.findall("([a-z]+)[(](.+)[)]", key)
        if agg:
            return {"agg":agg[0][0], "var":agg[0][1], "key":key, "opt":opt}
        else:
            return {"agg":"", "var":key.split(" ")[0], "key":key, "opt":opt}

    
#-----------------------------------------------------

class APIquery(APIbase):

    def __init__(self, project, mysql=None):
        self.globals = {}
        self.setProject(project)
        if not mysql:
            mysql = IDEAS.base.get_mysql()
        self.mysql = mysql

    def __del__(self):
        self.mysql.close()

    #-------------------------------------BACKGROUND FUNCTIONS

    def setProject(self, project):
        project = project.lower()
        if project not in self.globals:
            self.globals[project] = {}
            self.globals[project]["tables"] = {}
        self.project = project

    def setObject(self, obj):
        obj = obj.lower()
        if grammar.singular_noun(obj):
            obj = grammar.singular_noun(obj)
        self.obj = obj

    def setPagesize(self, pagesize):
        self.pagesize = pagesize

    def setDebug(self, debug=True):
        self.debug = debug

    #-------------------------------------FORMATTED OUTPUT

    def json_output(self, data):
        import json
        """
        Returns the output for a web query
        """
        #http://stackoverflow.com/questions/455580/json-datetime-between-python-and-javascript
        #dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.date) else None
        def dthandler(obj):
            if type(obj).__name__ in ("date", "datetime"):
                return obj.isoformat()
            elif type(obj).__name__ in ("Decimal"):
                if float(obj) == int(obj):
                    return int(obj)
                else:
                    return float(obj)
            else:
                return None

        return json.dumps(data, indent=4, encoding="latin-1", default=dthandler)

    def csv_massage(self, data, header=False):
        """
        Prepares data and massages it into a form for CSV injestion
        """
        #note do we want to add in the count stuff?
        for data_list in data:
            keys = data_list.keys()
            for data_key in keys:
                if self._islist(data_list[data_key]):
                    if header:
                        for item in data_list[data_key]:
                            for i in range(0,5):
                                if type(item).__name__ in "dict":
                                    for item_key, value in item.items():
                                        k = data_key+"_"+str(i)+"."+item_key
                                        data_list[k] = value
                                else:
                                    data_list[data_key+"_"+str(i)] = item 
                    else:    
                        for i,item in enumerate(data_list[data_key]):
                            if type(item).__name__ in "dict":
                                for item_key, value in item.items():
                                    k = data_key+"_"+str(i)+"."+item_key
                                    data_list[k] = value
                            else:
                                data_list[data_key+"_"+str(i)] = item 
                    data_list.pop(data_key)
                    
        if header:
            return sorted(data[0].keys())
        else:
            return data
    

    def restructure(self, data, layer=True, output="json"):
        """
        Takes data and restructures it appropriately 
          if layer, (converts var__var:value to {var: {var: "value"}})
        
        Args:
          data: the data to manipulate
          layer: should the data be layered? (per above)
            or should the variables just be represented by "."?
        """
        #return data

        obj = self.chainobj
        obj_type = self.get_type(obj)
        def layered_dict(datum, base=None):
            #if the key exists in base, get rid of it
            if base:
                keys = set(datum.keys()) & set(base)
                for k in keys:
                    datum.pop(k)
            #if the key is a kewyrod, remove it
            for kw in self.keywords:
                if kw in datum:
                    datum.pop(kw)

            keys = datum.keys()
            for k in keys:
                datum_new = datum
                if layer and output not in ("csv", "xls"):
                    for i,n in enumerate(k.split("__")):
                        if n not in datum_new:
                            datum_new[n] = {}
                        if i == k.count("__"):
                            datum_new[n] = datum.pop(k)
                        datum_new = datum_new[n]
                else:
                    datum_new[".".join(k.split("__"))] = datum.pop(k)

        #import json
        #print json.dumps(data, indent=4)

        for data_list in data:
            #awk but sometimes this base data just doesn't exist!
            if self.dict_type[obj_type] in data_list and \
               len(data_list[self.dict_type[obj_type]]) > 0:
                layered_dict(data_list[self.dict_type[obj_type]][0])
                base = data_list[self.dict_type[obj_type]][0].keys()
            else:
                base = []

            for data_key in data_list:
                if data_key == self.dict_type[obj_type]:
                    continue
                if self._islist(data_list[data_key]):
                    for i,v in enumerate(data_list[data_key]):
                        layered_dict(data_list[data_key][i], base=base)
                        keys = data_list[data_key][i].keys()
                        #if nested dictionary the same (example)
                        #  { for.ron: "", for.ron2: ""} => for {ron:"", ron2:""}
                        keyf = list(set([k.split(".")[0] for k in keys]))

                        if len(keyf) == 1:
                            if keyf[0] in (
                                 self._singular(data_key),
                                 obj+"_"+self._singular(data_key),
                                 self._singular(data_key)+"_id"
                               ):
                                if keys[0].count(".") > 0: #nested dict
                                    for key in keys:
                                        value = data_list[data_key][i].pop(key)
                                        new_key = self._jsplit(key, fr=1)
                                        data_list[data_key][i][new_key] = value
                                else:
                                    value = data_list[data_key][i].pop(keyf[0])
                                    data_list[data_key][i] = value
                        #this is for list items
                        elif data_key in data_list[data_key][i] and \
                             len(data_list[data_key][i]) == 1:
                            value = data_list[data_key][i].pop(data_key)
                            data_list[data_key][i] = value
            layered_dict(data_list)
            if self.dict_type[obj_type] in data_list:
                base = data_list.pop(self.dict_type[obj_type])
                if len(base) > 0:
                    data_list.update(base[0])

        return data

    #-------------------------------------HIDDEN METHODS

    def _get_defaults(self, obj="all"):
        """
        Returns relevant tables
        Tested? No
        """
        project = self.project

        if obj not in self.globals[project]["tables"]:
            self.globals[project]["tables"][obj] = self.tables(obj=obj)
        return project, self.globals[project]["tables"][obj]

    def _relabel(self, table, obj):
        """
        Relabel the table names found in the API for later post processing
        
        Args:
          table: the table name
          obj: the related object
        Returns:
          a single keyword which helps summarize our data (assuming context in URL)
        Tested? Yes
        Todo: Make the testing more flexible? Hard to remember what this does!
        """
        parse = self._parse(table)
        if parse["type"] == "link":
            if len(parse["objects"]) > 1:
                parse["objects"].pop(parse["objects"].index(obj))
                return "_".join(parse["objects"])
            #otherwise, if column name is sibling, parent, child -- column name
            elif len(parse["objects"]) == 1:
                cols = self.mysql.columns(table=table)
                cols.pop(cols.index(obj+"_id"))
                cols = [c.replace(obj+"_", "") for c in cols]
                for c in cols:
                    if c in self.dict_type["relation"].values():
                        return c
                
        elif parse["type"] == "list":
            if parse["comment"] != "":
                return parse["comment"]
            else:
                col = self.mysql.columns(table=table)
                #remove keywords and the like
                for k in self.keywords:
                    if k in col:
                        col.pop(col.index(k))
                col.pop(col.index(obj+"_id"))
                return col[0].split("__")[0]
        else:
            return parse["type"]
    
    def _get_table(self, obj, filt, table_list=None):
        """
        Get a specific table based on its filter
        Args: 
          obj: the tables related to the obj specified
          filt: the filter to apply on to the table
          table_list: good if we want to specify the tables
        Returns:
          A list of tables which meet the filter
        Tested? Yes
        """
        project, tables = self._get_defaults(obj=obj)
        if table_list:
            tables = table_list
        return [t for t in tables if t.lower().find(filt.lower()) >= 0]

    def _key_check(self, key, check_list):
        """
        Creates equivalency for key.sub and key__sub.
        Args:
          key: key to check against
          check_list: list of keys that can be accepted
        Returns: True or False whether key is in the check_list
        
        Tested? Yes
        """
        key = key.split(" ")[0]
        if key in check_list or self._key_convert(key)["var"] in check_list:
            return True
        else:
            return False           

    def _sep_id(self, table):
        """
        Takes a table and separates out the variables with _id from the others
        """    
        #get the number of columns with the _id mark for our TEMPORARY TABLE
        dct = {}
        self.mysql.c.execute("DESCRIBE "+table)
        for c in self.mysql.c.fetchall():
            if c[0] == self.chainobj + "_id":
                label = "_id"
            else:
                label = "col"
            if label not in dct:
                dct[label] = []
            dct[label].append(c[0])
        return dct

    def _singular(self, noun):
        """
        Convenience function for singular
        """
        noun = noun.lower()
        if grammar.singular_noun(noun):
            return grammar.singular_noun(noun)
        else:
            return noun
    
    def _in_table(self, key, table, col=None, objs=None):
        """
        Does the key variable exist in the table?
        """
        parsed = self._parse(table)
        if not col:
            col = self.mysql.columns(tbl=table)
        if not objs:
            objs = parsed["objects"]
        if type(objs).__name__ in ('str', 'unicode'):
            objs = [objs]

        if parsed["type"] in self.dict_type["main"] and self._key_check(key, col):
            return key            

        for obj in objs:
            prefix = self._jsplit(key, to=1)
            newkey = self._jsplit(key, fr=1)
            if self._relabel(table, obj) in [prefix, self._singular(prefix)]:
                if not newkey:
                    if self._key_check(prefix, col):
                        return prefix
                    elif self._key_check(prefix+"_id", col):
                        return prefix+"_id"
                    elif self._key_check(self._singular(prefix)+"_id", col):
                        return self._singular(prefix)+"_id"
                if self._key_check(newkey, col):
                    return newkey
                elif self._key_check(key, col):
                    return self._key_convert(key)["var"]

        return None

    def _mysql_debug(self, query, table):
        sql = """
            CREATE TEMPORARY TABLE {table} {query};
            """.format(table=table, query=query)
        self.sql_string += sql
        if self.debug:
            print ""
            print datetime.datetime.now()
            print sql
            print ""
          
        self.mysql.c.execute(sql)
        if self.debug:
            self.mysql.c.execute("SELECT * FROM {table} LIMIT 10".format(table=table))
            print self.mysql.c.fetchall()

    #-------------------------------------TABLE STRUCTURE

    def tables(self, obj="all", parent_delim="-"):
        """
        Returns the tables related to a given project

        Args:
          project: the name of the project (used to obtain data from MySQL)
            *Per our definition of table names, we can indicate parent projects 
            with a delimiter. This allows us to maintain single parent tables 
          parent_delim: deliminator that indicates separation of parent projects    
        Returns: a list of table names relevant to a given project
        Tested? Yes
        """
        obj = self._singular(obj)
        data = []
        project = self.project
        #the for-loop iterates the tables to parse all relevant parent projects    
        for h in xrange(project.count(parent_delim)+1):
            self.mysql.c.execute("""
                SELECT  table_name 
                  FROM  information_schema.tables 
                 WHERE  table_schema="{db}" AND table_name LIKE "{project}_%"
                """.format(db=self.mysql.db, 
                      project=self._jsplit(project, parent_delim, to=h+1)))
            data.extend([x[0] for x in self.mysql.c.fetchall()])
        data = list(set(data))

        if obj != "all":
            data = [d for d in data if obj in self._parse(d)["objects"]]

        self.globals[project]["tables"][obj] = data
        return data

    def get_type(self, obj):
        """
        Returns the type of the object
        Args:
          obj: the name of the object
        Returns: the type of the object
        Tested? Yes
        """
        obj = self._singular(obj)
        project, tables = self._get_defaults(obj=obj) 
        for table in tables:
            if self._parse(table)["type"] == self.dict_type["legend"]:
                return self.dict_type["legend"]
            elif self._parse(table)["type"] == self.dict_type["object"]:
                return self.dict_type["object"]
        return None

    def objects(self, obj="all"):
        """
        Returns the objects related to a given project or a list of table names

        Args:
          project: the name of the project
          tables: a list of table names that can be parsed to obtain the objects
          as_legend: whether to treat this as a legend or not
        Returns: A list of unique object names (can also return legend names)
        Tested? Yes
        """
        obj = self._singular(obj)
        objects = []
        typed = {}
        types = {"object":[], "legend":[]}

        project, tables = self._get_defaults()        
        
        for table in tables:
            classify = None
            if self._parse(table)["type"] == self.dict_type["legend"]:
                classify = "legend"
            elif self._parse(table)["type"] == self.dict_type["object"]:
                classify = "object"

            if classify:
                typed[self._parse(table)["objects"][0]] = classify
                if obj == "all":
                    types[classify].append(self._parse(table)["objects"][0])

        if obj != "all":
            project, tables = self._get_defaults(obj=obj)
            for table in tables:            
                objects.extend(self._parse(table)["objects"])        
            objects = list(set(objects))
            if obj in objects:
                objects.pop(objects.index(obj))
            for o in objects:
                if o in typed:
                    types[typed[o]].append(o)                    

        return types        
        #return self.globals[project]["objects"]

    def columns(self, obj):
        """
        Returns the column names related to a given object and project

        Args:
          obj: the name of the object we want to return the structure of
          tables: a list of table names that can be parsed to obtain the objects
        Returns: A dictionary that contains the table name followed by the data
        Tested? TODO
        """
        obj = self._singular(obj)
        obj_type = self.get_type(obj)
        data = []
        record = {}
        project, tables = self._get_defaults(obj)

        for table in tables:
            if self._is_leg(obj, table) or obj_type == "object":
                label = self._relabel(table, obj)
                self.mysql.c.execute("""
                    SELECT  column_name, data_type
                      FROM  information_schema.columns
                     WHERE  table_schema="{db}" AND table_name LIKE "{table}"
                    """.format(db=self.mysql.db, table=table)) 
                if self._parse(table)["type"] == "link":
                    label = grammar.plural_noun(label) 
                record[label] = [dict(self.mysql.c.fetchall())]
                #not sure if we want this right now, so remove it for now
                #  this would help us maintain structure
                #if 1==0 and len(self._parse(table)["objects"]) > 1:
                #    label = grammar.plural_noun(label)
                #    record[label] = [{"count": "int"}]
                #else:
        data.append(record)
        self.chainobj = obj
        return data


    #-------------------------------------QUERIES CHAINABLE FUNCTIONS


    #-------------------------------------CHAIN FUNCTIONS

    def chain_group(self, action, param, param_opt=[]):
        """
        Allows us to perform ordering, filtering and grouping
        AFTER a grouping has already taken place
        """
        inner = []
        field = []
        table = "chainable"+str(self.chain-1)
        self.mysql.c.execute("DESCRIBE "+table)
        col = [c[0] for c in self.mysql.c.fetchall()]

        #field
        if action in ("order", "filter"):
            field.append("*")
        elif action in ("group"):
            field.append("count(*) AS count__all")
            for p in param_opt:
                p = self._key_convert(p)
                if self._key_check(p["var"], col):
                    if p["agg"]:
                        field.append("{key} AS {agg}__{var}".format(
                            key=p["key"], var=p["var"], agg=p["agg"]))
                    else:
                        field.append(p["var"])            
        
        #from
        if action in ("order", "group"):
            for k in param:
                if self._key_check(k, col):
                    k = self._key_convert(k)
                    inner.append(k["var"])
                    if action in ("group"):
                        if k["var"] == "count__all":
                            field.append("count__all AS count__key")
                        else:
                            field.append(k["var"])
            inner = ",".join(inner)
            inner = action.upper() + " BY " + inner
            if action in ("group"):
                inner += " ORDER BY count__all DESC"
        elif action == "filter":
            for k,v in param.items():
                if self._key_check(k, col):
                    k = self._key_convert(k)
                    inner.append(self._param_where(k["var"], v))
            inner = " AND ".join(inner)
            inner = "WHERE " + inner
        field = ",".join(field)        

        self._mysql_debug("""
            SELECT  {field} 
              FROM  {table} 
                    {inner}
            """.format(field=field, table=table, inner=inner), 
                       "chainable"+str(self.chain))
        self.chain += 1
        return self

    def chain_new(self, obj):
        """
        Begins the chain which initiates queries
        """
        if self.chain > 0:
            for n in xrange(self.chain):
                self.mysql.c.execute("""
                    DROP TEMPORARY TABLE IF EXISTS chainable{n}
                    """.format(n=n))
        self.chain = 0
        self.chainobj = self._singular(obj)
        
        return self

    def chain_fetch(self, page=1, pagesize=None, output="json", layer=True):
        """
        *RON NOTE* -- GET RID OF THIS ENDPOINT LOGIC!
          if just {obj}_id, we throw it into the direct one
          if multiple _id, we throw it into a different one
          if no _id, we throw it into a different one

        Args:
            pagesize: let user specify how many records they want to return
              dangerous function but its there for options
            output: can be:
                json
                csv
                sql
        """
        data = []
        obj = self.chainobj            
        obj_type = self.get_type(obj)
        
        if not pagesize:
            pagesize = self.pagesize

        if self.chain == 0:
            #this might be useful as its own function?
            self.chain_new(obj)
            inner = ["CREATE TEMPORARY TABLE chainable0",
                     "SELECT {obj}_id FROM", 
                     self._get_table(obj, self.dict_type[obj_type])[0]]
            self.mysql.c.execute(" ".join(inner).format(obj=obj))
            self.chain += 1

        cols = self._sep_id("chainable"+str(self.chain-1))
        
        #some page management stuff (might change to page starting @ 1, now @ 0)
        self.mysql.c.execute("""
            SELECT count(*) FROM chainable{n}
            """.format(n=self.chain-1))
        count = self.mysql.c.fetchone()[0]
        pages = int(math.ceil(float(count)/pagesize))
        if not str(page).isdigit(): page = 1
        if int(page) < 1: page = 1
        if int(page) > pages: page = pages
        page = int(page) - 1

        #add additional columns (if necessary)
        metaList = []
        if page == -1: # means empty
            pass
        elif "col" in cols or self.chainobj in ("group"):
            self.mysql.c.execute("""
                SELECT {field} FROM chainable{n} LIMIT {begin},{offset}""".format(
                field=",".join(cols["col"]), n=self.chain-1, 
                begin=page*pagesize, offset=pagesize))
            for row in self.mysql.c.fetchall():
                dct = {}
                for i,r in enumerate(row):
                    dct[cols["col"][i]] = r
                metaList.append(dct)

        if page == -1:
            data = []
        elif "_id" in cols and self.chainobj not in ("group"):
            self.mysql.c.execute("""
                SELECT {obj}_id FROM chainable{n} LIMIT {begin},{offset}""".format(
                obj=obj, n=self.chain-1, 
                begin=page*pagesize, offset=pagesize))
            idList = [x[0] for x in self.mysql.c.fetchall()]
            data = self.query_direct(idList, metaList, pagesize=pagesize)
            data = self.restructure(data, output=output, layer=layer)
        else:
            idList = []
            data = []
            for idx in metaList:
                data.append(idx)                
            data = self.restructure(data, output=output, layer=layer)

        if self.debug: 
            print count, self.chain, idList, metaList
                    
        if output == "json":
            data = {"stats": {"count":count, "page":page+1, "pages":pages},
                    "data": data}
            return self.json_output(data)
        elif output in ("csv", "xls"):
            return jsonConvert.output(data, format=output)
        elif output == "sql":
            return sqlparse.format(self.sql_string, 
                reindent=True, keyword_case="upper")
        
    def chain_order(self, order):
        """
        Args:
          order: (list or string)
            specify the variables you want to order the data by
            you can also toss in things like {seq_num:10} as a dictionary
            to create filters on top of the data
            can also use {"top":2} ==> {"seq_num + 1": "<=2"}
        TODO: This requires a lot of optimization in speed...

        Checklist:
        x if previous chain exists, see if it has any extra variables
        x if previous chain doesn't exist, use original table
        """
        
        #TODO: whats up with this?
        # /helios/patent/group/patent.year/patent.office/order/patent.office-/patent.year?flat
        # why doesn't that query work???
        obj = self.chainobj
        if type(order).__name__ not in ('list', 'tuple'):
            order = [order]

        if obj == "group":
            return self.chain_group("order", order)

        project, tables = self._get_defaults(obj)
        self.chainable_index()
            
        inner = []
        mcol = self.mysql.columns(tbl=self._get_table(obj, 
                 self.dict_type["object"])[0])
        #Adding this adds difficulty in reading but makes it easier
        #to add more complex chains and reduce unnecessary joins
        if self.chain == 0:
            inner = []
        else:
            inner = ["chainable"+str(self.chain-1), "AS", "main"]

        sort = []
        where = []
        order_col = {}
        table_seq = 0
        for i,table in enumerate(tables):
            col = self.mysql.columns(tbl=table)
            if mcol != col:
                col = list(set(col) - set(mcol))
            tableOK = False
            if self.chain == 0 and table_seq == 0:
                alias = "main"
            else:
                alias = "a"+str(i)
            rec = []
            for ordr in order:
                #add in conditions like seq_num=1 
                #which helps us do things like sort ONLY by primary topic
                
                if type(ordr).__name__ in ('dict'):
                    k,v = ordr.items()[0]
                    if k.lower() in "top":
                        k = "seq_num + 1"
                        v = "<=" + v
                        if self._key_check(k, col):         
                            rec.append(alias+"."+self._param_where(k,v))                        
                    else:
                        key = self._in_table(k, table, col)
                        if key:
                            rec.append(alias+"."+self._param_where(key,v))
                            
                else:
                    key = self._in_table(ordr.split()[0], table, col)
                    if key:
                        if len(ordr.split()) == 1:
                            ordr = key
                        else:
                            ordr = key + " " + " ".join(ordr.split()[1:])
                        order_col[self._key_convert(ordr)["key"]] = alias
                        if not tableOK:
                            table_seq += 1
                            tableOK = True

            if tableOK:
                where.extend(rec)
                sort.append(obj+"_id")
                if self.chain > 0 or table_seq > 1:
                    if table == self._get_table(obj, self.dict_type["object"])[0]:
                        inner.append("INNER JOIN")
                    else:
                        inner.append("LEFT JOIN")
                inner.extend([table, "AS", alias])
                
                if self.chain > 0 or table_seq > 1:
                    inner.append("ON main.{obj}_id={alias}.{obj}_id".format(
                        alias=alias, obj=obj))
                
        if where:
            inner.append("WHERE")
            inner.append(" AND ".join(where))

        inner.append("ORDER BY")
        order_query = []
        field = ["distinct(main.{obj}_id) AS {obj}_id".format(obj=obj)]
        for ordr in order_col:
            ordr = self._key_convert(ordr)
            if ordr["agg"]:
                order_query.append("{agg}({a_num}.{var}) {opt}".format(
                    a_num=order_col[ordr["key"]], opt=ordr["opt"],
                    var=ordr["var"], agg=ordr["agg"]))
            else:
                order_query.append("{a_num}.{var} {opt}".format(
                    a_num=order_col[ordr["key"]], 
                    var=ordr["var"], opt=ordr["opt"]))

        if self.chain>0:
            cols = self._sep_id("chainable"+str(self.chain-1))
            if "col" in cols:
                field.extend(["main."+c+" AS "+c for c in cols["col"]])
        inner.append(",".join(order_query))

        self._mysql_debug("""
            SELECT  {field} 
              FROM  {inner}
              """.format(field=",".join(field), inner=" ".join(inner)),
                         "chainable"+str(self.chain))
                
        self.chain += 1
        return self

                
    def chainable_index(self):
        """
        Basic action items that affects each chain
        """
        #I think indexing a temporary table re-sorts it? (so only index prev?)
        if self.chain > 0:
            self.mysql.c.execute("""
                CREATE INDEX idx ON chainable{n} ({obj}_id)
                """.format(obj=self.chainobj, n=self.chain-1))
    
    #-------------------------------------end points
                            
    def query_direct(self, idList, metaList=[], pagesize=None, limit=5, maxlen=256):
        """  
        Queries a given table for meta data related to a specified object ID
        Args:
          obj: The object to query (person/publication/etc.)
          idList: The identifiers of the object to query
          limit: number of items shown on a LINK between objects
            (LET PEOPLE KNOW THE DEFAULTS!)
          maxlen: auto string concatentation for queries greater than 1 return
        Returns:
          Returns a list with a dictionary object containing the object/ID pair
          for each matching ID and its related meta data, or NULL for not found 
        """

        data = []
        obj = self.chainobj
        obj_type = self.get_type(obj)
        project, tables = self._get_defaults(obj)
        if not pagesize:
            pagesize = self.pagesize
        if type(idList).__name__ not in ('list', 'tuple'):
            idList = idList.split(",")
        idList = idList[:pagesize]

        for i,idx in enumerate(idList):
            record = {}            
            for table in tables: 
                if self._is_leg(obj, table) or obj_type == "object":
                    label = self._relabel(table, obj)
                    query = ["SELECT"]
                    cols = self.mysql.columns(table=table)
                    query.append("*")
                    query.extend(["FROM", table, "WHERE", obj+"_id IN ('{value}')"])
                    if "seq_num" in cols:
                        query.append("ORDER BY seq_num")

                    results = []
                    #if linked to more than one object, show the count and modify the label
                    if self._parse(table)["type"] == "link":
                        if len(cols) > 3:
                            #this len(cols)==2 might give us problems but OK for now, I guess
                            #TODO, do we want to include this?
                            #query.append("LIMIT "+str(limit))
                            queryc = copy.deepcopy(query)
                            queryc[1] = "count(*)"
                            self.mysql.c.execute(" ".join(queryc).format(value=idx))
                            results.append({"count": self.mysql.c.fetchone()[0]})
                        label = grammar.plural_noun(label)

                    #add some column names to the records
                    self.mysql.c.execute(" ".join(query).format(value=idx))
                    for res in self.mysql.c.fetchall():
                        rec = {}
                        for j,v in enumerate(res):
                            if len(idList) > 1 and self._isstr(v) and \
                               len(v) > maxlen:
                                rec[cols[j]] = v[:maxlen] + "..."
                            else:                           
                                rec[cols[j]] = v
                        results.append(rec) 

                    record[label] = results
            if metaList:
                record.update(metaList[i])
            data.append(record)
        return data

    #def query_group should NOT be chainable

    #-------------------------------------Chainable

    def query_move(self, to_obj):
        """
        Move from one object to another. This will use the linkage data but 
        will not include physical linkages between the two datasets
        
        Args:
          to_obj: the new object we should (move) towards
        """
        obj = self.chainobj
        to_obj = self._singular(to_obj)
        
        project, tables = self._get_defaults(obj)
        self.chainable_index()

        link_table = self._get_table(obj, to_obj)[0]
        if self.chain == 0:
            self._mysql_debug("""
                SELECT  {to_obj}_id, count(*) as count
                  FROM  {table}
              GROUP BY  {to_obj}_id
              ORDER BY  min(seq_num)
                """.format(to_obj=to_obj, table=link_table),
                "chainable0")
        else:
            self._mysql_debug("""
                SELECT  b.{to_obj}_id AS {to_obj}_id, count(*) as count 
                  FROM  chainable{n0} AS a
            INNER JOIN  {table} AS b
                    ON  a.{obj}_id=b.{obj}_id
              GROUP BY  {to_obj}_id
              ORDER BY  min(b.seq_num)
                """.format(n=self.chain, n0=self.chain-1,
                           obj=obj, to_obj=to_obj, table=link_table),
                "chainable"+str(self.chain))
        self.chainobj = to_obj
        self.chain += 1
        return self

    def query_filter(self, params):
        """  
        Queries a given table by filtering the underlying data
          
        Args:
          obj: object we want to filter
          params: variables and their filters
            note: keywords are rather generic and are first extracted 
              later applied if appropriate
              ex: seq_num keyword can let us search for like top1 topic
        Returns: self (chainable)
        Testing: TODO

        TODO: Filter by something like (topic) count perhaps?
        
        Checklist:
        x if previous chain exists, see if it has any extra variables
        x if previous chain doesn't exist, use original table
        """
        obj = self.chainobj
        obj_type = self.get_type(obj)
        if obj == "group":
            return self.chain_group("filter", params)

        project, tables = self._get_defaults(obj)
        self.chainable_index()

        #extract out the keywords as these are often repeated
        params_kw = {}
        for key in self.keywords:
            if key in params:
                params_kw[key] = params.pop(key)

        #table selector
        #seems a bit confusing though
        query = []
        mcol = self.mysql.columns(tbl=self._get_table(obj, self.dict_type[obj_type])[0])

        for table in tables:
            where = []
            col = self.mysql.columns(tbl=table)
            if mcol != col:
                col = list(set(col) - set(mcol))
            for k,v in params.items():
                key = self._in_table(k, table, col)
                if key:
                    where.append(self._param_where(key, v))

            #if a parameter is in the table, check to see if the param_kw too
            #   a parameter kw is like seq_num which can ONLY be associated
            #   with an associated table
            if where:
                for k,v in params_kw.items():
                    if k.lower() in "top":
                        k = "seq_num + 1"
                        v = "<=" + v
                    if self._key_check(k, col):            
                        where.append(self._param_where(k, v)) 

            if where:
                query.append("SELECT {obj}_id FROM {tbl} WHERE {where}".format(
                    obj=obj, tbl=table, where=" AND ".join(where)))

        #query builder
        inner = ["SELECT", "distinct(a0.{obj}_id) AS {obj}_id"]
        # ADD UNIQUE COLUMNS FROM (b) SO THEY EXIST
        if self.chain>0:
            cols = self._sep_id("chainable"+str(self.chain-1))
            if "col" in cols:
                cols["col"] = ["main."+c+" AS "+c for c in cols["col"]]
                inner.extend([",", ",".join(cols["col"])])
        inner.append("FROM")
        i = 0
        for i,q in enumerate(query):
            if i>0:
                inner.append("INNER JOIN")
            inner.extend(["("+q+")", "AS a"+str(i)])
            if i>0:
                inner.extend(["ON", "a"+str(i-1)+".{obj}_id=a"+str(i)+".{obj}_id"])
        if self.chain > 0:
            inner.extend(["INNER JOIN", "chainable"+str(self.chain-1), 
                          "AS main", "ON", "a"+str(i)+".{obj}_id=main.{obj}_id"])

        self._mysql_debug(" ".join(inner).format(obj=obj),
            "chainable"+str(self.chain))

        self.chain += 1
        return self

    def query_group(self, group, aggr=[]):
        """
        Args:
          group: (list or string)
            specify the variables you want to group the data by
            you can also toss in things like {seq_num:10} as a dictionary
            to create filters on top of the data
            can also use {"top":2} ==> {"seq_num + 1": "<=2"}
          aggr: what are the items we want to aggregate?
            this is in list form: [sum(field), max(field2), etc..]

        Checklist:
        x if previous chain doesn't exist, use original table

        This function is VERY VERY similar to the ORDER function

        **allow chaining and further manipulation of query_groups?
        **right now this is basically a final function**
        """
        obj = self.chainobj
        obj_type = self.get_type(obj)
        if type(group).__name__ not in ('list', 'tuple'):
            group = [group]
        if type(aggr).__name__ not in ('list', 'tuple'):
            aggr = [aggr]

        if obj == "group":
            return self.chain_group("group", group, aggr)

        project, tables = self._get_defaults(obj)
        self.chainable_index()
 
        inner = []
        mcol = self.mysql.columns(tbl=self._get_table(obj, 
                 self.dict_type[obj_type])[0])
        #Adding this adds difficulty in reading but makes it easier
        #to add more complex chains and reduce unnecessary joins
        if self.chain == 0:
            inner = []
        else:
            inner = ["chainable"+str(self.chain-1), "AS", "main"]

        sort = []
        where = []
        group_col = {}
        agg_col = {}
        table_seq = 0
        for i,table in enumerate(tables):
            col = self.mysql.columns(tbl=table)
            if mcol != col:
                col = list(set(col) - set(mcol))
            tableOK = False
            if self.chain == 0 and table_seq == 0:
                alias = "main"
            else:
                alias = "a"+str(i)
            rec = []
            for grp in group:
                #add in conditions like seq_num=1 
                #which helps us do things like sort ONLY by primary topic
                #TODO: IMPROVE THIS
                if type(grp).__name__ in ('dict'):
                    k,v = grp.items()[0]
                    if k.lower() in "top":
                        k = "seq_num + 1"
                        v = "<=" + v
                        if self._key_check(k, col):         
                            rec.append(alias+"."+self._param_where(k,v))                        
                    else:
                        key = self._in_table(k, table, col)
                        if key:
                            rec.append(alias+"."+self._param_where(key,v))
                else:
                    key = self._in_table(grp, table, col)
                    if key:
                        grp = key
                        group_col[self._key_convert(grp)["key"]] = alias
                        if not tableOK:
                            table_seq += 1
                            tableOK = True

            #not in ordering
            for agg in aggr:
                agg = self._key_convert(agg)
                if self._key_check(agg["var"], col):
                    agg_col[agg["key"]] = alias
                    if not tableOK:
                        table_seq += 1
                        tableOK = True

            if tableOK:
                where.extend(rec)
                sort.append(obj+"_id")
                if self.chain > 0 or table_seq > 1:
                    if table == self._get_table(obj, self.dict_type[obj_type])[0]:
                        inner.append("INNER JOIN")
                    else:
                        inner.append("LEFT JOIN")
                inner.extend([table, "AS", alias])
                
                if self.chain > 0 or table_seq > 1:
                    inner.append("ON main.{obj}_id={alias}.{obj}_id".format(
                        alias=alias, obj=obj))
                
        if where:
            inner.append("WHERE")
            inner.append(" AND ".join(where))

        inner.append("GROUP BY")
        group_query = []
        field = []
        for grp in group_col:
            grp = self._key_convert(grp)
            if grp["agg"]:
                group_query.append("{agg}__{var}".format(
                    agg=grp["agg"], var=grp["var"]))
                field.append("{agg}({a_num}.{var}) AS {agg}__{var}".format(
                    a_num=group_col[grp["key"]], 
                    var=grp["var"], agg=grp["agg"]))
            else:
                group_query.append("{a_num}.{var}".format(
                    a_num=group_col[grp["key"]], var=grp["var"]))
                field.append("{a_num}.{var} AS {var}".format(
                    a_num=group_col[grp["key"]], var=grp["var"]))
        for agg in agg_col:
            agg = self._key_convert(agg)
            field.append("{agg}({a_num}.{var}) AS {agg}__{var}".format(
                a_num=agg_col[agg["key"]], var=agg["var"], agg=agg["agg"]))

        inner.append(",".join(group_query))
        inner.append("ORDER BY count__all DESC")
        # most frequent likely matters. let resorting later
        field.append("count(*) as count__all")

        self._mysql_debug("""
            SELECT  {field} 
              FROM  {inner}
              """.format(field=",".join(field), inner=" ".join(inner)),
                         "chainable"+str(self.chain))
                
        self.chain += 1
        self.chainobj = "group"
        return self

