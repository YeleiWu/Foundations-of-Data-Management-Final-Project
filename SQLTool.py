# return ['world','...']
def getMySQLDatabases(engine):
    result = engine.execute("show databases").fetchall()
    defaut_tables = ['sys', 'mysql', 'information_schema', 'performance_schema', 'test']
    dbs = [db[0] for db in result if db[0] not in defaut_tables]

    return dbs


# return ['city', 'country', 'countrylanguage']
def getAllTables(engine, db):
    t = engine.execute("show tables from {}".format(db)).fetchall()
    tables = [s[0] for s in t]
    return tables


def getAllFKTables(engine, db, table):


    
    fk_table = []
    fk = engine.execute(
        "select TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME from "
        "INFORMATION_SCHEMA.KEY_COLUMN_USAGE where CONSTRAINT_SCHEMA ='{}' AND REFERENCED_TABLE_NAME = '{}'".format(
            db, table)).fetchall()
    for r in fk:
        if r[0] == table:
            fk_table.append(r[3])
        elif r[3] == table:
            fk_table.append(r[0])
    return fk_table


# return { "city.ID": "country.Capital", ...}
def getAllFK(engine, db):
    fk_dict = dict()
    tables = getAllTables(engine, db)
    for t in tables:
        fk = engine.execute(
            "select TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME from "
            "INFORMATION_SCHEMA.KEY_COLUMN_USAGE where CONSTRAINT_SCHEMA ='{}' AND REFERENCED_TABLE_NAME = '{}'".format(
                db, t)).fetchall()

        if len(fk) > 0:
            for i, _ in enumerate(fk):
                k = fk[i][0] + '.' + fk[i][1]
                v = fk[i][3] + '.' + fk[i][4]
                fk_dict[k] = v
                fk_dict[v] = k
    print('fk_dict:', fk_dict)
    return fk_dict


# return
def getAttributes(engine, db, tables):
    table_attr_dict = dict()
    for t in tables:
        c = engine.execute("show columns from {1}.{0} from {1}".format(t, db)).fetchall()
        columns = [s[0] for s in c]
        table_attr_dict[t] = columns
    return table_attr_dict


def getQueryResult(engine, db,  sql):
    # engine.execute(sql)
    engine.execute('use ' + db)
    print("sql:",sql)
    result = engine.execute(sql).fetchall()
    print(result)
    return [list(r) for r in result]


def getColumnsType(engine, sql_dict):
    columns_dict = {}
    tables = [sql_dict['table']]
    for j in sql_dict['join']:
        tables.append(j.split('join ')[1].split(' ')[0])
    for t in tables:
        columns = engine.execute('SHOW COLUMNS FROM {} FROM {}'.format(t, sql_dict['db']))
        for c in columns:
            column, type = list(c)[:2]
            if len(tables) == 1:
                columns_dict[column] = type
            else:
                columns_dict[t+'.'+column] = type
    return columns_dict


def condition(cond_list, columns_type, cond_init_str):
    if len(cond_list) > 0:
        cond = cond_init_str
        for i, c in enumerate(cond_list):
            conn = c.split(':')[0]
            if conn == 'not':
                conn = 'and not'
            formula = c.split(':')[1]
            var_origin, operator = formula.split(' ')[0].strip(), formula.split(' ')[1].strip()
            if var_origin.find('(') == -1:
                var = var_origin
            else:
                var = var_origin.split('(')[1][:-1]

            value = formula[formula.index(operator) + len(operator):].strip()
            if operator == 'between':
                val1, val2 = value.split(' and ')
                if columns_type[var].find('varchar') != -1 or columns_type[var] in ['text', 'longtext']:
                    value = '\'' + val1 + '\'' + ' and ' + '\'' + val2 + '\''
            elif operator == 'in' or operator == 'not':
                in_list = formula[formula.index('('):][1:-1].split('，')
                if columns_type[var].find('varchar') != -1 or columns_type[var] in ['text', 'longtext']:
                    if operator == 'in':
                        value = '('
                    elif operator == 'not':
                        value = 'in ('
                    for item in in_list:
                        value = value + '\'' + item + '\','
                    value = value[:-1] + ')'
            else:
                if columns_type[var].find('varchar') != -1 or columns_type[var] in ['text', 'longtext']:
                    value = '\'' + value + '\''
            # print(columns_type[var].find('varchar'))
            #
            formula = var_origin + ' ' + operator + ' ' + value
            if i == 0:
                cond = cond + formula + ' '
            else:
                cond = cond + conn + ' ' + formula + ' '
    else:
        cond = ''
    return cond


def getwhere(engine, sql_dict):
    d = {}
    columns_type = getColumnsType(engine, sql_dict)
    where = condition(sql_dict['where'], columns_type, '')
    d['where'] = where
    attribute_list = sql_dict['attribute']
    d['outFields'] = sql_dict['attribute'] if len(attribute_list) > 0 else ['*']
    d['returnGeometry'] = 'true'
    return d
    # s_list = ['QueryParameters = new Query({']
    # columns_type = getColumnsType(engine, sql_dict)
    # where_list = sql_dict['where']
    # where = condition(where_list, columns_type, 'where: ')
    # if where == '':
    #     where = "where: ''"
    # s_list.append(where+',')
    #
    # attribute_list = sql_dict['attribute']
    # if len(attribute_list)==0:
    #     attribute = "['*']"
    # else:
    #     attribute = str(attribute_list)
    # s_list.append('outFields: ' + attribute +',')
    # s_list.append('returnGeometry: true')
    # s_list.append('})')
    # return s_list


def concatSQL(engine, sql_dict):
    db, table = sql_dict['db'], sql_dict['table']
    tables = [table]
    for j in sql_dict['join']:
        t = j.split(' on ')[0].split(' ')[-1]
        tables.append(t)
    columns_type = getColumnsType(engine, sql_dict)
    # print(columns_type)
    print(sql_dict)
    if len(sql_dict['attribute']) == 0:
        if len(sql_dict['group']) > 0:
            sql_dict['attribute'] = sql_dict['group']
        else:
            sql_dict['attribute'].append('*')
    attribute = 'select ' + ','.join(sql_dict['attribute'])
    table = ' from ' + sql_dict['table'] + ' '
    join = ' '.join(sql_dict['join']) + ' '
    where_list = sql_dict['where']
    where = condition(where_list, columns_type, 'where ')
    group_list = sql_dict['group']
    if len(group_list) > 0:
        group = 'group by ' + ','.join(group_list) + ' '
        having_list = sql_dict['having']
        having = condition(having_list, columns_type, 'having ')
        group += having
    else:
        group = ''
    order_list = sql_dict['order']
    if len(order_list) > 0:
        order = 'order by ' + ','.join(order_list) + ' '
    else:
        order = ''
    limit = '' if len(sql_dict['limit']) == 0 else ('limit ' + sql_dict['limit'] + ' ')
    offset = '' if len(sql_dict['offset']) == 0 else ('offset ' + sql_dict['offset'])
    sql = attribute + table + join + where + group + order + limit + offset
    print(sql)
    return sql



# def concatSQL(sql_dict):
#     print(sql_dict)
#     if len(sql_dict['attribute']) == 0:
#         sql_dict['attribute'].append('*')
#     attribute = 'select ' + ','.join(sql_dict['attribute'])
#     table = ' from ' + ','.join(sql_dict['table']) + ' '
#     join = ' '.join(sql_dict['join']) + ' '
#     where_list = sql_dict['where']
#     if len(where_list) > 0:
#         where = 'where '
#         for i, c in enumerate(where_list):
#             conn = c.split(':')[0]
#             formula = c.split(':')[1]
#             if i == 0:
#                 where = where + formula + ' '
#             else:
#                 where = where + conn + ' ' + formula + ' '
#     else:
#         where = ''
#     group_list = sql_dict['group']
#     if len(group_list) > 0:
#         group = 'group by ' + ','.join(group_list) + ' '
#         having_list = sql_dict['having']
#         if len(having_list) > 0:
#             having = 'having '
#             for i, c in enumerate(having_list):
#                 conn = c.split(':')[0]
#                 formula = c.split(':')[1]
#                 if i == 0:
#                     having = having + formula + ' '
#                 else:
#                     having = having + conn + ' ' + formula + ' '
#         else:
#             having = ''
#         group += having
#     else:
#         group = ''
#     order_list = sql_dict['order']
#     if len(order_list) > 0:
#         order = 'order by ' + ','.join(order_list) + ' '
#     else:
#         order = ''
#     limit = '' if len(sql_dict['limit']) == 0 else ('limit ' + sql_dict['limit'] + ' ')
#     offset = '' if len(sql_dict['offset']) == 0 else ('offset ' + sql_dict['offset'])
#     sql = attribute + table + join + where + group + order + limit + offset
#     print(sql)
#     return sql