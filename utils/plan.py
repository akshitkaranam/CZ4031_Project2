import itertools
import queue
import psycopg2
import json
from utils.config import config
import utils.queries as queries
import sqlparse
import re

nodeListOperations = []
nodeListScans = {}
rawNodeList = []
nodeListJoins = []
depth = 0

PARAMS = {
    'hashjoin': 'ON',
    'mergejoin': 'ON',
    'nestloop': 'ON',
    'indexscan': 'ON',
    'bitmapscan': 'ON',
    'seqscan': 'ON',
}


class Node(object):
    def __repr__(self):
        return f"Node({self.node_type}, {self.relation_name}, {self.schema}, {self.alias}, {self.group_key}, {self.sort_key}, {self.join_type}" \
               f", {self.index_name},{self.hash_condition}, {self.table_filter}, {self.index_condition}, {self.merge_condition}" \
               f", {self.recheck_condition}, {self.join_filter},{self.subplan_name}, {self.actual_rows}, {self.actual_time}" \
               f", {self.description},{self.cost},{self.sort_type},{self.output})"

    def __init__(self, node_type, relation_name, schema, alias, group_key, sort_key, join_type, index_name,
                 hash_condition, table_filter, index_condition, merge_condition, recheck_condition, join_filter,
                 subplan_name, actual_rows, actual_time, description, cost, sort_type, output):
        self.node_type = node_type.upper()
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        self.group_key = group_key
        self.sort_key = sort_key
        self.join_type = join_type
        self.index_name = index_name
        self.hash_condition = hash_condition
        self.table_filter = table_filter
        self.index_condition = index_condition
        self.merge_condition = merge_condition
        self.recheck_condition = recheck_condition
        self.join_filter = join_filter
        self.subplan_name = subplan_name
        self.actual_rows = actual_rows
        self.actual_time = actual_time
        self.description = description
        self.cost = cost
        self.sort_type = sort_type
        self.children = []
        self.output = output


def get_query_plan(query_number, disable_parameters=(), ):
    output_json = {}
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        # print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        query = queries.getQuery(query_number)

        query = "EXPLAIN (ANALYSE, VERBOSE, FORMAT JSON) " + query
        if query is None:
            print("Please select a valid query number!")
            return

        for param in disable_parameters:
            query = "SET LOCAL enable_" + str(param) + "= off;" + query
        query = "set session statement_timeout=5000;" + query
        # print(query)
        cur.execute(query)
        rows = cur.fetchall()

        output_json = json.dumps(rows)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        if "timeout" in str(error):
            return {}
    finally:
        if conn is not None:
            conn.close()

    return output_json


def get_operations(query_number):
    sql_operators_main = ["SELECT", "FROM", "WHERE", 'GROUP BY', 'ORDER', 'JOIN']
    operation_list = []
    query = queries.getQuery(query_number)

    # for nodes in rawNodeList:
    #     print(nodes)

    # format query
    statements = sqlparse.split(query)
    formatted_query = sqlparse.format(statements[0], reindent=True, keyword_case='upper')
    # print()
    # print(formatted_query)
    # print()

    # split query
    split_query = formatted_query.splitlines()

    #  get indexes of formatted query
    index = 0
    for line in split_query:
        # print(index, line)
        index += 1

    # GET required indexes of the lines that contain the Relations to retrieve
    from_indexes_start = [i for i, line in enumerate(split_query) if 'FROM' in line]
    from_indexes_stop = []
    if from_indexes_start:
        for from_index in from_indexes_start:
            count = 0
            for line in split_query[from_index + 1:]:
                if not any(ext in line for ext in sql_operators_main):
                    count += 1
                else:
                    break
            from_indexes_stop.append(from_index + count)

    # print("From Start Indexes: " + str(from_indexes_start))
    # print("From Stop Indexes: " + str(from_indexes_stop))

    # Link the SCAN Nodes to QEP using the "FROM" indexes
    if len(from_indexes_start) != len(from_indexes_stop):
        return Exception("There is an error in the SQL query")

    for i in range(len(from_indexes_start)):
        from_start = from_indexes_start[i]
        from_end = from_indexes_stop[i]
        count = 0
        for line in split_query[from_start:from_end + 1]:
            query_scan = get_scans(from_start + count, line)
            operation_list.append(query_scan)
            count += 1

    # GET required indexes of the lines that are contained in the WHERE Clause
    where_indexes_start = [i for i, line in enumerate(split_query) if 'WHERE' in line]
    where_indexes_end = []
    if where_indexes_start:
        for where_index in where_indexes_start:
            count = 0
            for line in split_query[where_index + 1:]:
                if not any(ext in line for ext in sql_operators_main):

                    count += 1
                else:
                    break
            where_indexes_end.append(where_index + count)

    # print("Where Start Indexes: " + str(where_indexes_start))
    # print("Where Stop Indexes: " + str(where_indexes_end))

    # Getting lines that have "=" in the WHERE clause
    lines_that_have_equalsign = []
    if len(where_indexes_start) != len(where_indexes_end):
        return Exception("There is an error in the SQL query")

    for i in range(len(where_indexes_start)):
        where_start = where_indexes_start[i]
        where_end = where_indexes_end[i]
        count = 0
        for line in split_query[where_start:where_end + 1]:
            if (">" not in line) and ("=" in line) and ("<" not in line):
                lines_that_have_equalsign.append(where_start + count)
            count += 1
    join_indexes = []
    for i in range(len(lines_that_have_equalsign)):
        this_line = split_query[lines_that_have_equalsign[i]]
        # print(this_line)
        split_this_line = this_line.split('=')
        if "\'" not in split_this_line[1] and "\"" not in split_this_line[1] and (not split_this_line[1].isnumeric()):
            join_indexes.append(lines_that_have_equalsign[i])
    # print("Join Indexes: " + str(join_indexes))

    join_conditions_list = []

    for index in join_indexes:
        join_condition = split_query[index].split(" ")
        index_equal_sign = -1
        for i in range(len(join_condition)):
            if join_condition[i] == "=":
                index_equal_sign = i
                break
        if index_equal_sign == -1:
            Exception("Error")

        right = join_condition[index_equal_sign - 1]
        left = join_condition[index_equal_sign + 1]
        right = re.sub(r'[()]', '', right)
        left = re.sub(r'[()]', '', left)
        temp_list = [right, left]
        join_conditions_list.append(temp_list)

    print()

    # # Check if there is A=B, B=C, A=C relation, weird relation
    # for i in range(len(join_conditions_list)):
    #     for j in range(i + 1, len(join_conditions_list)):
    #         join_condition_i = join_conditions_list[i]
    #         join_condition_j = join_conditions_list[j]
    #         combined_list = join_condition_i + join_condition_j
    #         combined_list2 = list(set(combined_list))
    #         if (len(combined_list2) < 4):
    #             common_condition = list(set(combined_list) - set(combined_list2))
    #             join_condition_i_new = list(set(join_condition_i) - set(common_condition))
    #             join_condition_j_new = list(set(join_condition_j) - set(common_condition))
    #             join_condition_i_new.append(join_condition_j_new[0])
    #             join_condition_j_new.append(join_condition_i_new[0])
    #             join_conditions_list.append({i: join_condition_i})
    #             join_conditions_list.append({j: join_condition_j})
    #             continue

    getJoinMapping(join_conditions_list, join_indexes, split_query, operation_list)
    return operation_list


def get_mapping_hashjoin(i, join_conditions_list, join_indexes, split_query, operation_list):
    count_con = 0

    for join_condition in join_conditions_list:

        if (join_condition[0] in rawNodeList[i].hash_condition) and (
                join_condition[1] in rawNodeList[i].hash_condition):
            hash_positions = []
            for j in range(i, 0, -1):
                if rawNodeList[j].node_type == 'HASH' and (
                        join_condition[0] in str(rawNodeList[j].output) or join_condition[1] in str(
                    rawNodeList[j].output)):
                    hash_positions.append(j)
                    break

            list_nodes = []
            for hash_pos in hash_positions:
                list_nodes.append(rawNodeList[hash_pos])
            list_nodes.append(rawNodeList[i])
            query_scan = {"index": join_indexes[count_con], "sql": split_query[join_indexes[count_con]],
                          "operation": "HASH JOIN", "nodes": list_nodes}
            operation_list.append(query_scan)
        count_con += 1


def get_mapping_mergejoin(i, join_conditions_list, join_indexes, split_query, operation_list):
    count_con = 0

    for join_condition in join_conditions_list:
        if (join_condition[0] in rawNodeList[i].merge_condition) and (
                join_condition[1] in rawNodeList[i].merge_condition):

            sort_positions = []
            for j in range(i, 0, -1):
                if rawNodeList[j].node_type == 'SORT':
                    if join_condition[0] in str(rawNodeList[j].sort_key) or join_condition[1] in str(
                            rawNodeList[j].sort_key):
                        sort_positions.append(j)

            list_nodes = []
            for sort_pos in sort_positions:
                list_nodes.append(rawNodeList[sort_pos])
            list_nodes.append(rawNodeList[i])

            query_scan = {"index": join_indexes[count_con], "sql": split_query[join_indexes[count_con]],
                          "operation": "MERGE JOIN", "nodes": list_nodes}
            operation_list.append(query_scan)
            break
        count_con += 1


def get_mapping_nestloop(i, join_conditions_list, join_indexes, split_query, operation_list):
    index_scan_position = -1

    for j in range(i, 0, -1):
        if rawNodeList[j].node_type == 'INDEX SCAN' or rawNodeList[j].node_type == 'BITMAP INDEX SCAN':
            index_scan_position = j
            break

    if rawNodeList[i].join_filter is None:
        if index_scan_position != -1:
            count_con = 0
            for join_condition in join_conditions_list:
                if (join_condition[0] in rawNodeList[index_scan_position].index_condition) and (
                        join_condition[1] in rawNodeList[index_scan_position].index_condition):
                    query_scan = {"index": join_indexes[count_con], "sql": split_query[join_indexes[count_con]],
                                  "operation": "INDEX JOIN",
                                  "nodes": [rawNodeList[i], rawNodeList[index_scan_position]]}
                    operation_list.append(query_scan)
                    break
                count_con += 1

    elif index_scan_position != -1 and (i - index_scan_position < 3):
        count_con = 0
        for join_condition in join_conditions_list:
            if (join_condition[0] in rawNodeList[index_scan_position].index_condition) and (
                    join_condition[1] in rawNodeList[index_scan_position].index_condition):
                query_scan = {"index": join_indexes[count_con], "sql": split_query[join_indexes[count_con]],
                              "operation": "INDEX JOIN", "nodes": [rawNodeList[i], rawNodeList[index_scan_position]]}
                operation_list.append(query_scan)

            if (join_condition[0] in rawNodeList[i].join_filter) and (
                    join_condition[1] in rawNodeList[i].join_filter):
                query_scan = {"index": join_indexes[count_con], "sql": split_query[join_indexes[count_con]],
                              "operation": "NESTED LOOP JOIN", "nodes": [rawNodeList[i]]}
                operation_list.append(query_scan)
            count_con += 1

    else:
        count_con = 0
        for join_condition in join_conditions_list:
            if (join_condition[0] in rawNodeList[i].join_filter) and (
                    join_condition[1] in rawNodeList[i].join_filter):
                query_scan = {"index": join_indexes[count_con], "sql": split_query[join_indexes[count_con]],
                              "operation": "NESTED LOOP JOIN", "nodes": [rawNodeList[i]]}
                operation_list.append(query_scan)
                break
            count_con += 1


def getJoinMapping(join_conditions_list, join_indexes, split_query, operation_list):
    for i in range(len(rawNodeList)):
        if "JOIN" in rawNodeList[i].node_type or "NEST" in rawNodeList[i].node_type:

            if rawNodeList[i].node_type == "HASH JOIN":
                get_mapping_hashjoin(i, join_conditions_list, join_indexes, split_query, operation_list)


            elif rawNodeList[i].node_type == "MERGE JOIN":
                get_mapping_mergejoin(i, join_conditions_list, join_indexes, split_query, operation_list)


            elif rawNodeList[i].node_type == "NESTED LOOP":
                get_mapping_nestloop(i, join_conditions_list, join_indexes, split_query, operation_list)


# get type of scan operation for each index
def get_scans(index, sql):
    for node in nodeListScans:
        if node.node_type == 'BITMAP INDEX SCAN':
            relation_name = node.index_name.split('_')[0]
        else:
            relation_name = node.relation_name

        if relation_name in sql:
            query_scan = {"index": index, "sql": sql, "operation": node.node_type,
                          "relation": relation_name, "nodes": [node]}

            return query_scan


def get_qep_tree(qep_json):
    q_child_plans = queue.Queue()
    q_parent_plans = queue.Queue()
    plan = qep_json[0][0][0]['Plan']

    q_child_plans.put(plan)
    q_parent_plans.put(None)

    while not q_child_plans.empty():
        current_plan = q_child_plans.get()
        parent_node = q_parent_plans.get()

        relation_name = schema = alias = group_key = sort_key = join_type = index_name = hash_condition = table_filter \
            = index_condition = merge_condition = recheck_condition = join_filter = subplan_name = actual_rows = actual_time = description = cost = sort_type = None
        if 'Relation Name' in current_plan:
            relation_name = current_plan['Relation Name']
        if 'Schema' in current_plan:
            schema = current_plan['Schema']
        if 'Alias' in current_plan:
            alias = current_plan['Alias']
        if 'Group Key' in current_plan:
            group_key = current_plan['Group Key']
        if 'Sort Key' in current_plan:
            sort_key = current_plan['Sort Key']
        if 'Join Type' in current_plan:
            join_type = current_plan['Join Type']
        if 'Index Name' in current_plan:
            index_name = current_plan['Index Name']
        if 'Hash Cond' in current_plan:
            hash_condition = current_plan['Hash Cond']
        if 'Filter' in current_plan:
            table_filter = current_plan['Filter']
        if 'Index Cond' in current_plan:
            index_condition = current_plan['Index Cond']
        if 'Merge Cond' in current_plan:
            merge_condition = current_plan['Merge Cond']
        if 'Recheck Cond' in current_plan:
            recheck_condition = current_plan['Recheck Cond']
        if 'Join Filter' in current_plan:
            join_filter = current_plan['Join Filter']
        if 'Actual Rows' in current_plan:
            actual_rows = current_plan['Actual Rows']
        if 'Actual Total Time' in current_plan:
            actual_time = current_plan['Actual Total Time']
        if 'Subplan Name' in current_plan:
            if "returns" in current_plan['Subplan Name']:
                name = current_plan['Subplan Name']
                subplan_name = name[name.index("$"):-1]
            else:
                subplan_name = current_plan['Subplan Name']
        if 'Total Cost' in current_plan:
            cost = current_plan['Total Cost']
        if 'Sort Space Type' in current_plan:
            sort_type = current_plan['Sort Space Type']
        if 'Output' in current_plan:
            output = current_plan['Output']

        current_node = Node(current_plan['Node Type'], relation_name, schema, alias, group_key, sort_key, join_type,
                            index_name, hash_condition, table_filter, index_condition, merge_condition,
                            recheck_condition, join_filter,
                            subplan_name, actual_rows, actual_time, description, cost, sort_type, output)

        if parent_node is not None:
            parent_node.children.append(current_node)
        else:
            root_node = current_node

        if 'Plans' in current_plan:
            for item in current_plan['Plans']:
                # push child plans into queue
                q_child_plans.put(item)
                # push parent for each child into queue
                q_parent_plans.put(current_node)

    return root_node


def traverse_tree(node, depth):
    global nodeListOperations
    global nodeListJoins
    global nodeListScans
    global rawNodeList

    for child in node.children:
        traverse_tree(child, depth + 1)

    if "SCAN" in str(node.node_type):
        nodeListScans.update({node: depth})

    elif "LOOP" in str(node.node_type) \
            or "JOIN" in str(node.node_type):  # \
        # or str(node.node_type) == "HASH" \
        # or (str(node.node_type) == "SORT" and str(node.sort_type) == "Disk"):
        nodeListJoins.append(node)
    else:
        nodeListOperations.append(node)

    rawNodeList.append(node)


def get_qep_nodes_with_depth(query_number, disable=()):
    global nodeListOperations
    global nodeListScans
    global rawNodeList
    global nodeListJoins
    raw_json = get_query_plan(query_number, disable)
    if raw_json == {}:
        return None
    qep_json = json.loads(raw_json)
    nodeListOperations.clear()
    nodeListScans.clear()
    root_node = get_qep_tree(qep_json)
    traverse_tree(root_node, 0)


def get_mapping(query_number, disable=()):
    global nodeListOperations
    global nodeListScans
    global rawNodeList
    global nodeListJoins
    nodeListOperations = []
    nodeListScans = {}
    rawNodeList = []
    nodeListJoins = []
    get_qep_nodes_with_depth(query_number, disable)
    sorted_scan = dict(sorted(nodeListScans.items(), key=lambda item: item[1], reverse=True))
    # print(nodeListScans)
    if rawNodeList:
        operation_list = get_operations(query_number)
        return operation_list
    return None
