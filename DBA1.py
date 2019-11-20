#!/usr/bin/env python
# coding: utf-8

# In[1161]:


import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Function, Comparison, Where
from sqlparse.tokens import Keyword, DML, Whitespace


# In[1162]:


import itertools
import sys

# In[1163]:


f = open('/home/crusher/Downloads/files(1)/files/metadata.txt', 'r')


# In[1164]:


tables = {}
line = f.readline()
while line:
    if line == "<begin_table>\n":
        line = f.readline()
        table_name = line[:-1]
        tables[table_name] = {}
        file = open('/home/crusher/Downloads/files(1)/files/%s.csv'%table_name, 'r')
        line = f.readline()
        
        while line not in ["<end_table>\n", "<end_table>"]:
            attr_name = line[:-1]
            tables[table_name][attr_name] = []
            line = f.readline()
        
        for data in file:
            values = data.split(',')
            for attr, value in zip(tables[table_name], values):
                value = value.replace('"', '')
                value = int(value)
                tables[table_name][attr].append(value)
    line = f.readline()


# In[1165]:


tables


# In[1356]:


def cond_list(token):
    conditions = []
    conjunction = None
    where = 0
    for i in range(len(token.tokens)):
        if isinstance(token[i], Comparison):
            conditions.append(token[i])
            i += 1
            if(i >= len(token.tokens)):
                print("Invalid query")
                return [], None
            if token[i].ttype == Whitespace:
                i += 1
                if(i >= len(token.tokens)):
                    print("Invalid query")
                    return [], None
                if str(token[i]) == ';':
                    return conditions, None
                if str(token[i]).lower() == 'and' or str(token[i]).lower() == 'or':
                    conjunction = str(token[i]).lower()
                    i += 1
                    if(i >= len(token.tokens)):
                        print("Invalid query")
                        return [], None
                    if token[i].ttype == Whitespace:
                        i += 1
                        if(i >= len(token.tokens)):
                            print("Invalid query")
                            return [], None
                        if isinstance(token[i], Comparison):
                            conditions.append(token[i])
                            i += 1
                            if(i >= len(token.tokens)):
                                print("Invalid query")
                                return [], None
                            if token[i].ttype == Whitespace:
                                i += 1
                                if(i >= len(token.tokens)):
                                    print("Invalid query")
                                    return [], None
                                if str(token[i]) == ';':
                                    return conditions, conjunction
                                else:
                                    print("Invalid query")
                                    return [], None
                            elif str(token[i]) == ';':
                                return conditions, conjunction
                            else:
                                print("Invalid query")
                                return [], None
                        else:
                            print("Invalid query")
                            return [], None
                    else:
                        print("Invalid query")
                        return [], None
                else:
                    print("Invalid query")
                    return [], None
            elif str(token[i]) == ';':
                return conditions, None
            else:
                print("Invalid query")
                return [], None
            
        elif str(token[i]).lower() == 'where':
            if where == 0:
                where = 1
            else:
                print("Invalid query")
                return [], None
        elif token[i].ttype != Whitespace:
            print("Invalid query")
            return [], None
    print("Invalid query")
    return [], None


# In[1357]:


def getattrlist(token):
    identifier_list = []
    for item in token:
        if isinstance(item, Identifier):
            identifier_list.append(str(item))
    return identifier_list


# In[1358]:


def getattrlist_util(token):
    attr_list = []
    all_attr = 0
    if isinstance(token, IdentifierList):
        attr_list = getattrlist(token)
    elif isinstance(token, Identifier):
        attr_list.append(str(token))
    elif str(token) == '*':
        all_attr = 1
    else:
        print("Error, Expected an identifier got '%s'"%str(token))
        return None, None
    temp = set(attr_list)
    if sorted(list(temp)) != sorted(attr_list):
        print(list(temp), attr_list)
        print("Cannot repeat identifier")
        return None, None
    return attr_list, all_attr


# In[1359]:


def parse(query):
    b = sqlparse.parse(query, encoding=None)
    if len(b) != 1:
        print("Error, some characters after the semicolon")
        return []
    a = b[0]
    distinct = 0
    aggregate = None
    attr_list = []
    table_list = []
    all_attr = 0
    index = 0
    conditions = []
    where = 0
    conjunction = None
    fname = None
    
    #for 0th and 1st index
    if len(a.tokens) < 5 or str(a[index]).lower() != 'select':
        print("Invalid Query")
        return []

    index += 2

    #for attribute names,function or distinct keyword
    if str(a[index]).lower() == 'distinct':
        distinct = 1
        index += 2
        attr_list, all_attr = getattrlist_util(a[index])
        if not attr_list and not all_attr:
            return []
    elif isinstance(a[index], Function):
        [fname, attribute] = str(a[index]).split('(')
        aggregate = fname
        attribute = attribute[:-1]
        attr_list.append(attribute)
    else:
        attr_list, all_attr = getattrlist_util(a[index])
        if not attr_list and not all_attr:
            return []
    
    
    index += 2
    
    
    if len(a.tokens) <= index or str(a[index]).lower() != 'from':
        print("Invalid Query")
        return []

    index += 2
    if len(a.tokens) <= index:
        print("Invalid Query")
        return []

    table_list, temp = getattrlist_util(a[index])
    if not table_list:
        return []
    
    index += 1
    if len(a.tokens) <= index:
        print("Invalid Query")
        return []
    if a[index].ttype == Whitespace:
        index += 1
    elif str(a[index]) == ';':
        return [conditions, fname, attr_list, table_list, distinct, conjunction, where, aggregate, all_attr]
    
    if len(a.tokens) <= index:
        print("Invalid Query")
        return []
    if str(a[index]) == ';':
        return [conditions, fname, attr_list, table_list, distinct, conjunction, where, aggregate, all_attr]
    

    if isinstance(a[index], Where):
        where = 1
        conditions, conjunction = cond_list(a[index])
        if not conditions:
            return []
    return [conditions, fname, attr_list, table_list, distinct, conjunction, where, aggregate, all_attr]


# In[1360]:


def cross_prod(table_list):
    headers = []
    list_tables = []
    for table in table_list:
        if table not in tables:
            print("Table '%s' not found" % table)
            return [], []
        headers.extend('%s.%s'%(table, column) for column in tables[table])
        tb = [tables[table][i] for i in tables[table]]
        correct_table = [[tb[j][i] for j in range(len(tb))] for i in range(len(tb[0]))]
        list_tables.append(correct_table)
    tables_cross = list(itertools.product(*list_tables))
    for i in range(len(tables_cross)):
        flat = [item for sublist in tables_cross[i] for item in sublist]
        tables_cross[i] = flat
    return headers, tables_cross


# In[1361]:


def attr_name_normalize(attr, headers, header_wo_tn):
    if str.isdigit(attr):
        return [attr, 1]
    if '.' in attr:
        if attr not in headers:
            print("Attribute '%s' not present in any specified table" % attr)
            return [None, None]
        return [attr, headers.index(attr)]
    else:
        if attr not in header_wo_tn:
            print("Attribute '%s' not present in any specified table" % attr)
            return [None, None]
        elif header_wo_tn.count(attr) > 1:
            print("Ambiguos attribute name, belongs to more than one table")
            return [None, None]
        else:
            return [headers[header_wo_tn.index(attr)], header_wo_tn.index(attr)]


# In[1362]:


def apply_conds(conds, row, all_index, conjunction):
    #Condition 1
    result = 0
    var1 = conds[0][0]
    operator = conds[0][1]
    var2 = conds[0][2]
    if not str.isdigit(var1):
        var1 = row[all_index[var1]]
    else:
        var1 = int(var1)
    if not str.isdigit(var2):
        var2 = row[all_index[var2]]
    else:
        var2 = int(var2)
    if operator == '=':
        result = var1 == var2
    elif operator == '<':
        result = var1 < var2
    elif operator == '>':
        result = var1 > var2
    elif operator == '<=':
        result = var1 <= var2
    elif operator == '>=':
        result = var1 >= var2
        
    if len(conds) == 1:
        return result
    
    #Condition 2
    next_result = 0
    var1 = conds[1][0]
    operator = conds[1][1]
    var2 = conds[1][2]
    if not str.isdigit(var1):
        var1 = row[all_index[var1]]
    else:
        var1 = int(var1)
    if not str.isdigit(var2):
        var2 = row[all_index[var2]]
    else:
        var2 = int(var2)
    if operator == '=':
        next_result = var1 == var2
    elif operator == '<':
        next_result = var1 < var2
    elif operator == '>':
        next_result = var1 > var2
    elif operator == '<=':
        next_result = var1 <= var2
    elif operator == '>=':
        next_result = var1 >= var2
    
    if conjunction.lower() == 'or':
        return result or next_result
    else:
        return result and next_result


# In[1363]:


def execute(conditions, fname, attr_list, table_list, distinct, conjunction, all_attr, where, aggregate):
    headers, tables_cross = cross_prod(table_list)
    if not headers:
        return []
    header_wo_tn = [column.split('.')[1] for column in headers]
    all_index = {}
    for i in range(len(headers)):
        all_index[headers[i]] = i
    attr_index = {}
    
    for i in range(len(attr_list)):
        if '.' in attr_list[i]:
            [attr_list[i], attr_index[attr_list[i]]] = attr_name_normalize(attr_list[i], headers, header_wo_tn)
            if not attr_list[i]:
                return []
        else:
            [attr_list[i], attr_index[attr_list[i]]] = attr_name_normalize(attr_list[i], headers, header_wo_tn)
            if not attr_list[i]:
                return []
            
    if where:
        conds = []
        
        for condition in conditions:
            identifier1 = None
            identifier2 = None
            operator = None
            for i in condition:
                if isinstance(i, Identifier) or str.isdigit(str(i)):
                    if identifier1 == None:
                        identifier1 = str(i)
                    elif identifier2 == None:
                        identifier2 = str(i)
                    else:
                        print("Invalid comparison statement")
                        return []
                elif str(i) in ['=', '<=', '>=', '<', '>']:
                    if operator == None:
                        operator = str(i)
                    else:
                        print("Invalid comparison statement")
                        return []
                elif i.ttype != Whitespace and str(i).lower() not in ['or', 'and']:
                    print("Invalid comparison statement")
                    return []
            attr1 = attr_name_normalize(identifier1, headers, header_wo_tn)[0]
            if not attr1:
                return []
            attr2 = attr_name_normalize(identifier2, headers, header_wo_tn)[0]
            if not attr2:
                return []
            conds.append([attr1, operator, attr2])
        new_table = []
        for row in tables_cross:
            if apply_conds(conds, row, all_index, conjunction):
                new_table.append(row)
        for i in conds:
            if i[1] == '=':
                if i[2] in attr_index and i[0] in attr_index:
                    headers[attr_index[i[2]]] = None
                    attr_list.remove(i[2])
                    del attr_index[i[2]]
        tables_cross = new_table
    if aggregate:
        output = 0
        if len(attr_list) != 1:
            print("Only one attribute allowed with aggregate functions")
            return []
        if fname.lower() == "sum":
            output = sum(tables_cross[i][attr_index[attr_list[0]]] for i in range(len(tables_cross)))
            return [['%s(%s)'%(fname, attr_list[0])], [output], attr_list]
        elif fname.lower() == "max":
            output = max(tables_cross[i][attr_index[attr_list[0]]] for i in range(len(tables_cross)))
            return [['%s(%s)'%(fname, attr_list[0])], [output], attr_list]
        elif fname.lower() == "min":
            output = min(tables_cross[i][attr_index[attr_list[0]]] for i in range(len(tables_cross)))
            return [['%s(%s)'%(fname, attr_list[0])], [output], attr_list]
        elif fname.lower() == "average":
            l = [tables_cross[i][attr_index[attr_list[0]]] for i in range(len(tables_cross))]
            output = sum(l)/len(l)
            return [['%s(%s)'%(fname, attr_list[0])], [output], attr_list]
        else:
            print("Invalid aggregate function")
            return []
        
    if not all_attr:
        keep_indices = [value for key, value in attr_index.items()]
        remove_indices = [i for i in list(range(len(headers))) if i not in keep_indices]
        remove_indices.reverse()
        for row in tables_cross:
            for index in remove_indices:
                del row[index]
                headers[index] = None
    headers = [element for element in headers if element is not None]
    
    if not distinct:
        return [headers, tables_cross, attr_list]
    else:
        hashmap = {}
        new_table = []
        for row in tables_cross:
            if tuple(row) not in hashmap:
                hashmap[tuple(row)] = 1
                new_table.append(row)
        return [headers, new_table, attr_list]
    


# In[1379]:

if len(sys.argv) < 2:
    print("Invalid query")
else:
    query = sys.argv[1]
    if parse(query):
        [conditions, fname, attr_list, table_list, distinct, conjunction, where, aggregate, all_attr] = parse(query)
        l = execute(conditions, fname, attr_list, table_list, distinct, conjunction, all_attr, where, aggregate)
        if not l:
            pass
        elif aggregate:
            print(l[0][0])
            print(l[1][0])
        elif len(l) == 3:
            attr_list = l[2]
            headers = l[0]
            if l[1]:
                table = l[1]
                if attr_list:
                    attr_index = [headers.index(i) for i in attr_list]
                    table = [[i[ind] for ind in attr_index] for i in table]
                else:
                    attr_list = headers
                print(','.join(attr_list))
                for i in table:
                    for j in range(len(i)):
                        i[j] = str(i[j])
                    print(','.join(i))
            else:
                print(','.join(l[2]))

