from flask import Flask
from flask import Response
from flask import request
from flask import make_response
from IDEAS.api.structure_v1 import APIcache
from IDEAS.api.structure_v1 import APIquery
import IDEAS.lib.inflect
import IDEAS.base
import urllib

app = Flask(__name__)
#starting to explore FlashAuth
#from flaskext.auth import Auth
#from flaskext.auth import AuthUser
#auth = Auth(app)



grammar = IDEAS.lib.inflect.engine()

#inputs
csv_pagesize = 1000
xls_pagesize = 1000
version = "v1"
keep_get = ["page", "flat"]


@app.errorhandler(404)
def error404(e):
    return Response("404: mosey along now", mimetype="text/plain")


#--------------------------
#
#  GLOBAL FUNCTIONS
#

def _get_GET(param):
    """ 
    Returns the GET parameters in a standardized format 
    """
    param = dict([(k,",".join(v)) for k,v in dict(param).items()])
    if "page" not in param:
        param["page"] = 1
    if "flat" not in param:
        param["layer"] = True
    else:
        param["layer"] = False
    return param

def _get_mysql(params):
    if "debug" in params:
        mysql = IDEAS.base.get_mysql(key="mysql_test")
    else:
        mysql = IDEAS.base.get_mysql(key="mysql")
    return mysql

def _json_callback(json, params):
    if "jsoncallback" in params:
        json = "{callback}({data})".format(
            callback=params["jsoncallback"], data=json) 
    elif "callback" in params:
        json = "{callback}({data})".format(
            callback=params["callback"], data=json) 
    return Response(json, mimetype="application/json")

#--------------------------
#
#  SERVER FUNCTIONS
#


#some structure functions
@app.route('/<client>/objects')
@app.route('/<client>/objects/')
@app.route('/<client>/structure')
@app.route('/<client>/structure/')
def return_objects(client):
    param = _get_GET(request.args)
    mysql = _get_mysql(param)
    res = APIquery(client, mysql)
    json = res.objects()
    mysql.close()
    return _json_callback(res.json_output(json), request.args)

@app.route('/<client>/<obj>/columns')
@app.route('/<client>/<obj>/columns/')
@app.route('/<client>/<obj>/structure')
@app.route('/<client>/<obj>/structure/')
def return_columns(client, obj):
    param = _get_GET(request.args)
    mysql = _get_mysql(param)
    res = APIquery(client, mysql)
    json = {}
    json.update({"schema": res.restructure(res.columns(obj), layer=param["layer"])})
    json.update({"type": res.get_type(obj)})
    json.update(res.objects(obj))
    mysql.close()
    return _json_callback(res.json_output(json), request.args)


@app.route('/<client>/<obj_full>')
@app.route('/<client>/<obj_full>/')
def api_base(client, obj_full):
    return api_server(client, obj_full, "/")


#add in version somewhere here (and figure out we do this mgmt)
@app.route('/<client>/<obj_full>/<path:opt>')
def api_server(client, obj_full, opt):
    #note, now:
    # - page=1 is first page (not 0)
    # - top/x translates to seq_num + 1 <= x
    
    #get params
    # flat -- make file "flat" rather than layered
    # page -- fetch a specific page number
    # debug -- a specific mode
    
    # request.remote_addr
    # import socket
    # socket.gethostbyname_ex("google.com")
    
    param = _get_GET(request.args)
    mysql = _get_mysql(param)
    res = APIquery(client, mysql)
    cache = APIcache(mysql)

    """
        note group has subfunction ==> calc
        so like: group/ron/calc/sum(ron)
    """
    
    code = ""
    keywords = {
        "move":"query_move", "link":"query_move", 
        "filter":"query_filter", "where":"query_filter", 
        "order":"chain_order",  "sort":"chain_order",
        "group":"query_group",
    }

    obj_split = obj_full.split(".")
    obj = obj_split.pop(0)
    if grammar.singular_noun(obj):
        obj = grammar.singular_noun(obj)
    obj_type = "json"
    if len(obj_split):
        obj_type = obj_split.pop(0)
    objects = res.objects()
    objects = objects["object"] + objects["legend"]

    if opt[-1] == "/":
        opt = opt[:-1]
    opts = opt.replace("+"," ").split("/")
    blocks = []


    # -------------------------------------
    #output functions
    def _api_output(data):
        if obj_type in ('code'):
            return Response(data, mimetype='text/plain')
        elif obj_type in ('csv'):
            response = make_response(data)
            response.headers["Content-type"] = "text/csv"
            response.headers["Content-disposition"] = \
                "attachment;filename="+client+"_"+obj+".csv"
            return response
        elif obj_type in ('xls'):
            response = make_response(data)
            response.headers["Content-type"] = "application/vnd.ms-excel"
            response.headers["Content-disposition"] = \
                "attachment;filename="+client+"_"+obj+".xls"
            return response
        elif obj_type in ('sql'):
            return Response(data, mimetype='text/plain')
        else:
            return _json_callback(data, request.args)

    # -------------------------------------
    #keep only select keep (things like key, not necessary)
    param_keep = []
    for k in param:
        if k in keep_get:
            param_keep.append((k, param[k])) 
    param_keep = urllib.urlencode(dict(sorted(param_keep)))


    # -------------------------------------
    #path/cache
    path = opt + "?" + param_keep
    if "nocache" not in param:
        cached_data = cache.fetch(version, client, obj_full, path)
        if cached_data:
            return _api_output(cached_data)
    # -------------------------------------

    #if not cached data
    place = 0
    for i,opt in enumerate(opts):
        if opt.lower() in keywords.keys() and i > 0:
            blocks.append(opts[place:i])
            place = i
    blocks.append(opts[place:])

    if obj_type in ('code'):
        code += ".chain_new('" + str(obj) + "')"
    else:
        res.chain_new(obj)

    # ITERATES TO BUILD A QUERY OFF THE ORIGINAL QUERY STRING
    action = ""
    
    for block in blocks:
        action = block[0].lower()
        if action in keywords.keys():
            options = block[1:]
            if action in ('move', 'link'):
                for opt in options:
                    if grammar.singular_noun(opt) in objects or opt in objects:
                        if obj_type in ('code'):
                            code += ".query_move('" + str(opt) + "')"
                        else:
                            res.query_move(opt)
            elif action in ('filter', 'where'):
                #converts the data to dictionary form
                option_dict = dict([options[2*i:2*(i+1)] 
                    for i in xrange(len(options)/2)])
                if obj_type in ('code'):
                    code += ".query_filter(" + str(option_dict) + ")"
                else:
                    res.query_filter(option_dict)
            elif action in ('order', 'sort', 'group'):
                #TODO HOW DO WE ACCOUNT FOR DICTIONARY STATEMENTS?
                option_list = []
                if "top" in options:
                    topdex = options.index("top")
                    top = {"top": options[topdex+1]}
                    options.pop(topdex)
                    options.pop(topdex)
                else:
                    top = None

                if action in ('order', 'sort'):
                    for opt in options:
                        option_list.extend(opt.split(","))
                    for i,opt in enumerate(option_list):
                        if opt[-1] == "-": 
                            option_list[i] = option_list[i][:-1] + " DESC"
                        elif opt[0] == "-": 
                            option_list[i] = option_list[i][1:] + " DESC"
                    if top:
                        option_list.append(top)

                    if obj_type in ('code'):
                        code += ".chain_order(" + str(option_list) + ")"
                    else:
                        res.chain_order(option_list)
                        
                #TODO HOW DO WE ACCOUNT FOR DICTIONARY STATEMENTS?
                elif action in ('group'):
                    aggr = []
                    if "calc" in options:
                        dex = options.index("calc")
                        options.pop(dex)
                        for opt in options[dex:]:
                            aggr.extend(opt.split(","))
                        options = options[:dex]
                    for opt in options:
                        option_list.extend(opt.split(","))
                    if top:
                        option_list.append(top)

                    if obj_type in ('code'):
                        code += ".query_group(" + str(option_list) 
                        code += ", aggr=" + str(aggr) + ")"
                    else:
                        res.query_group(option_list, aggr=aggr)
        #direct query
        elif len(block) == 1:
            if block[0] != "":
                if obj_type in ('code'):
                    code += ".query_filter(" + str({obj+"_id":block[0]}) + ")"
                else:
                    res.query_filter({obj+"_id":block[0]})

    if obj_type in ('code'):
        code += ".chain_fetch(page=" + str(param["page"]) + ")"
        final_data = code.replace("u'", "'")
    elif obj_type in ('csv'):
        final_data = res.chain_fetch(pagesize=csv_pagesize, output="csv")
    elif obj_type in ('xls'):
        final_data = res.chain_fetch(pagesize=xls_pagesize, output="xls")
    elif obj_type in ('sql'):
        final_data = res.chain_fetch(page=param["page"], output="sql")
    else:
        final_data = res.chain_fetch(page=param["page"], layer=param["layer"])

    if "nocache" not in param:            
        cache.insert(version, client, obj_full, path, final_data)
    mysql.close()
    return _api_output(final_data)


if __name__ == '__main__':
    app.run(debug=True)

