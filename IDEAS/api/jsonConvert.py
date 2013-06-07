from collections import OrderedDict
import csv
import json
import IDEAS.lib.xlwt as xlwt
import cStringIO

"""
File created by Jeffrey Hu
"""

def output(data, format='csv', object=None):
    master = get_master_data(data, object)
    slave = get_slave_data(data, master.values()[0])
    result = OrderedDict(master, **slave)

    if format == 'csv':
        return write_csv(result)
    elif format == 'xls':
        return write_xls(result)


def write_csv(data):
    out = cStringIO.StringIO()
    f = csv.writer(out)
    for val in data.values():
        for row in val:
            f.writerow(row)
        f.writerow('\n')
    return out.getvalue()

def write_xls(data):
    out = cStringIO.StringIO()
    file = xlwt.Workbook(encoding="latin-1")
    for key in data:
        table = file.add_sheet(key, cell_overwrite_ok=True)
        for x in range(0, len(data[key])):
            newdata = data[key][x]
            for y in range(0, len(newdata)):
                if type(newdata).__name__ in ('str', 'unicode'):
                    table.write(x, y, newdata[y].encode("UTF-8"))
                else:
                    table.write(x, y, newdata[y])
    file.save(out)
    return out.getvalue()

def get_master_data(data, object):
    master = {}
    master_value = []
    row_head = []
    index = 0

    for p_dict in data:
        auto_object = True
        if object:
            p_dict.has_key(object + '_id')
            auto_object = False
        row = []
        for key in p_dict:
            if not isinstance(p_dict[key], list):
                if index == 0:
                    if key.find('_id') != -1:
                        if auto_object or key == object + '_id':
                            row_head.insert(0, key)
                    else:
                        row_head.append(key)
                if key.find('_id') != -1:
                    if auto_object or key == object + '_id':
                        row.insert(0, p_dict[key])
                else:
                    row.append(p_dict[key])
        if len(row) > 0:
            master_value.append(row)
        index += 1
    if len(row_head) > 0:
        master_value.insert(0, row_head)

    master_key = master_value[0][0][:-3]

    master[master_key] = master_value

    return master


def get_slave_data(data, master):
    slave = {}
    index = 1

    for p_dict in data:
        for key in p_dict:
            row_head = []
            slave_value = []
            dict_index = 0
            list_index = 0
            if isinstance(p_dict[key], list):
                for dict_or_list in p_dict[key]:
                    row = []
                    if isinstance(dict_or_list, dict):
                        for c_key in dict_or_list:
                            if c_key == 'count':
                                dict_index -= 1
                                continue
                            if dict_index == 0:
                                if c_key.find('_id') != -1:
                                    row_head.insert(0, c_key)
                                else:
                                    row_head.append(c_key)
                            if c_key.find('_id') != -1:
                                row.insert(0, dict_or_list[c_key])
                            else:
                                row.append(dict_or_list[c_key])
                        dict_index += 1
                        #Add master id
                        if len(row) > 0:
                            row.insert(0, master[index][0])
                            slave_value.append(row)
                    else:
                        if list_index == 0:
                            row_head.append(key)
                        row.append(dict_or_list)
                        if len(row) > 0:
                            row.insert(0, master[index][0])
                            slave_value.append(row)
                        list_index += 1
                if len(row_head) > 0:
                    row_head.insert(0, master[0][0])
                    slave_value.insert(0, row_head)
                if len(slave_value) > 0:
                    if slave.has_key(key) == True:
                        slave[key].append(slave_value[1])
                    else:
                        slave[key] = slave_value
        index += 1

    return slave


def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv


def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv

