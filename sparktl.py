import findspark
global df1
df1 = None

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fc

spark = SparkSession.builder.appName('app').getOrCreate()

host_schema = StructType(
    [StructField('id', IntegerType(), True),
     StructField('name', StringType(), True),
     StructField('listings_count', IntegerType(), True)
     ]
)
listing_schema = StructType(
    [StructField('id', IntegerType(), True),
     StructField('name', StringType(), True),
     StructField('host_id', IntegerType(), True),
     StructField('neighbourhood', StringType(), True),
     StructField('latitude', FloatType(), True),
     StructField('longitude', FloatType(), True),
     StructField('room_type', StringType(), True),
     StructField('price', IntegerType(), True)
     ]
)
review_schema = StructType(
    [StructField('id', IntegerType(), True),
     StructField('listing_id', IntegerType(), True),
     StructField('date', StringType(), True),
     StructField('reviewer_name', StringType(), True),
     StructField('comments', StringType(), True),
     ]
)
schema_dict = {'host': host_schema, 'listing': listing_schema, 'review': review_schema}


def getTableDict(s_list, db, table, join):
    table_dict = {}
    tables = [table]

    for j in join:
        tables.append(j.split('join ')[1].split(' ')[0])
    for t in tables:
        table_dict[t] = spark.read.schema(schema_dict[t]).csv("{}.csv".format(t), header=True)
        if len(join) == 0:
            s_list.append("df = spark.read.csv('{0}.csv', header=True)".format(t))
        else:
            s_list.append("{0} = spark.read.csv('{0}.csv', header=True)".format(t))
    for name, t in table_dict.items():
        if len(join) > 0:
            s = "{0} = {0}".format(name)
            for c in t.columns:
                table_dict[name] = table_dict[name].withColumnRenamed(c, "{}_{}".format(name, c))
                s += ".withColumnRenamed('{}','{}')".format(c, name + '_' + c)
            s_list.append(s)
    return s_list, table_dict


def parse_join(df, s_list, table, joins, table_dict):
    for join in joins:
        join_table = join.split('join ')[1].split(' ')[0]
        right, left = join.split(' on ')[1].split(' = ')
        join_type = 'inner'
        if join.startswith('left'):
            join_type = 'left'
        elif join.startswith('right'):
            join_type = 'right'
        df = df.join(table_dict[join_table],
                     df[left.replace('.', '_')] == table_dict[join_table][right.replace('.', '_')], how=join_type)
        s_list.append(
            "df = {0}.join({1},{0}['{2}']=={1}['{3}'],how='{4}')".format(table, join_table, left.replace('.', '_'),
                                                                         right.replace('.', '_'), join_type))
    type_dict = {}
    for i in df.dtypes:
        type_dict[i[0]] = i[1]
    return s_list, df, type_dict


def getCon(clause, type_dict, reverse=False):
    attr = clause.split(' ')[0].replace('.', '_')
    if attr.find('(') == -1:
        new_attr = attr
    else:
        new_attr = attr.split('(')[1][:-1]
    compare = clause.split(' ')[1]
    if compare == 'between':
        val_1 = clause.split(' and ')[0].split(' ')[-1]
        val_2 = clause.split(' and ')[1]
        if type_dict[new_attr] == 'string':
            val_1 = "'" + val_1 + "'"
            val_2 = "'" + val_2 + "'"
        if reverse:
            s = "~(df['{0}'] {1} {3})&(~df['{0}'] {2} {4})".format(attr, '>=', '<=', val_1, val_2)
        else:
            s = "(df['{0}'] {1} {3})&(df['{0}'] {2} {4})".format(attr, '>=', '<=', val_1, val_2)

    elif compare in ['in', 'not']:
        #         print(compare, clause.split('in')[1])
        val_list = clause.split(' in ')[1].split('(')[1][:-1].split('，')
        if type_dict[new_attr] == 'string':
            val_list = [str(i) for i in val_list]
        if (compare != 'in') ^ reverse:
            s = "~(df['{}'].isin({}))".format(attr, str(val_list))
        else:
            s = "(df['{}'].isin({}))".format(attr, str(val_list))
    else:
        value_index = clause.index(clause.split(' ')[2])
        value = clause[value_index:]
        if type_dict[new_attr] == 'string':
            value = "'" + value + "'"
        if compare == '=':
            compare = '=='
        if reverse:
            s = "~(df['{}'] {} {})".format(attr, compare, value)
        else:
            s = "(df['{}'] {} {})".format(attr, compare, value)
    return s


def parse_condition(df, s_list, where, type_dict):
    logic_con_map = {}
    for w in where:
        logic, reverse = w.split(':')[0], False
        if logic == 'not':
            reverse = True
        logic_con_map[getCon(w.split(':')[1], type_dict, reverse)] = logic
    con = ''
    for k, v in logic_con_map.items():
        if con == '':
            con += k
        else:
            if v == 'or':
                con = con + '|' + k
            else:
                con = con + '&' + k

    if con == '':
        return s_list, df
    else:
        s_list.append("df = df[{}]".format(con))
        #         print(con)
        return s_list, df[eval(con)]


def parse_group(df, s_list, group, projection, order, having):
    if len(group) == 0:
        return s_list, df
    group = [g.replace('.', '_') for g in group]
    s_list.append("df = df.groupby({})".format(str(group)))  # listing.groupBy(['listing_neighbourhood'])
    df = df.groupby(group)
    agg_list, agg_s = [], ""
    attributes = projection[:]
    for h in having:
        if h.find('(') != -1:
            attributes.append(h.split(':')[1].split(' ')[0])
    for o in order:
        if o.find('(') != -1:
            attributes.append(o.split(' ')[0])
    for a in set(attributes):
        a = a.replace('.', '_')
        if a not in s_list and a.find('(') != -1:
            func = a.split('(')[0]
            attr = a.split('(')[1][:-1]
            s = "fc.{}('{}').alias('{}')".format(func, attr, a)
            agg_s = agg_s + s + ','
            agg_list.append(eval(s))

    if len(agg_list) > 0:
        df = df.agg(*agg_list)
        s_list.append("df = df.agg({})".format(agg_s[:-1]))
    else:
        df = df.count()[group]
        s_list.append("df = df.count()[{}]".format(group))
    return s_list, df


def parse_order(df, s_list, order):
    if len(order) == 0:
        return s_list, df
    columns, ascendings = [], []
    for o in order:
        columns.append(o.split(' ')[0].replace('.', '_'))
        if o.split(' ')[1].lower() == 'asc':
            ascendings.append(True)
        else:
            ascendings.append(False)
    s_list.append("df = df.orderBy({},ascending={})".format(str(columns), str(ascendings)))
    df = df.orderBy(columns, ascending=ascendings)
    return s_list, df


def parse_limit_offset(df, s_list, offset, limit):
    if limit == '':
        limit = 0
    else:
        limit = int(limit)
    if offset == '':
        offset = 0
    else:
        offset = int(offset)
    if limit == 0:
        return s_list, df.toPandas()

    df = df.toPandas().iloc[offset:offset + limit]
    s_list.append("df = df.toPandas().iloc[{}:{}]".format(offset, offset + limit))
    return s_list, df


def parse_projection(df, s_list, attributes, group):
    if len(attributes) == 0 and len(group) == 0:
        pass
    else:
        if len(attributes) == 0:
            group = [g.replace('.', '_') for g in group]
            df = df[group]
            s_list.append("df = df[{}]".format(group))
        else:
            attributes = [a.replace('.', '_') for a in attributes]
            df = df[attributes]
            s_list.append("df = df[{}]".format(attributes))
    return s_list, df


def getResult(sql_dict):
    # s_list = ["spark = SparkSession.builder.appName('tl').getOrCreate()"]
    s_list = []
    s_list, table_dict = getTableDict(s_list, sql_dict['db'], sql_dict['table'], sql_dict['join'])

    df = table_dict[sql_dict['table']]
    s_list, df, type_dict = parse_join(df, s_list, sql_dict['table'], sql_dict['join'], table_dict)

    s_list, df = parse_condition(df, s_list, sql_dict['where'], type_dict)
    s_list, df = parse_group(df, s_list, sql_dict['group'], sql_dict['attribute'], sql_dict['order'],
                             sql_dict['having'])
    s_list, df = parse_condition(df, s_list, sql_dict['having'], type_dict)
    s_list, df = parse_order(df, s_list, sql_dict['order'])
    s_list, df = parse_limit_offset(df, s_list, sql_dict['offset'], sql_dict['limit'])
    s_list, df = parse_projection(df, s_list, sql_dict['attribute'], sql_dict['group'])
    return s_list, df


def translate(sql_dict):
    global df1
    s_list, df1 = getResult(sql_dict)
    return s_list


def query(sql_dict):
    global df1
    # print('df:',df)
    if df1 is None:
        _, df1 = getResult(sql_dict)
    # _, df = getResult(sql_dict)
    return df1.to_html()
