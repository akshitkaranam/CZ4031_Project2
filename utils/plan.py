import queue
import psycopg2
import json
from utils.config import config
import utils.queries as queries
import sqlparse

nodeListOperations = []
nodeListScans = {}
rawNodeList = []
nodeListJoins = []
depth = 0


class Node(object):
    def __repr__(self):
        return f"Node({self.node_type}, {self.relation_name}, {self.schema}, {self.alias}, {self.group_key}, {self.sort_key}, {self.join_type}" \
               f", {self.index_name},{self.hash_condition}, {self.table_filter}, {self.index_condition}, {self.merge_condition}" \
               f", {self.recheck_condition}, {self.join_filter},{self.subplan_name}, {self.actual_rows}, {self.actual_time}" \
               f", {self.description},{self.cost},{self.sort_type})"

    def __init__(self, node_type, relation_name, schema, alias, group_key, sort_key, join_type, index_name,
                 hash_condition, table_filter, index_condition, merge_condition, recheck_condition, join_filter,
                 subplan_name, actual_rows, actual_time, description, cost, sort_type):
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


def get_query_plan(query_number, disable_parameters=(), ):
    output_json = {}
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
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
        print(query)
        cur.execute(query)
        rows = cur.fetchall()
        output_json = json.dumps(rows)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
            print()

    return output_json


def get_operations(query_number):
    sql_operators_main = ["SELECT", "FROM", "WHERE", 'GROUP BY', 'ORDER', 'JOIN']
    operation_list = []
    index_scan_values = []
    query = queries.getQuery(query_number)

    # for nodes in rawNodeList:
    #     print(nodes)

    # format query
    statements = sqlparse.split(query)
    formatted_query = sqlparse.format(statements[0], reindent=True, keyword_case='upper')
    print()
    print(formatted_query)
    print()

    # split query
    split_query = formatted_query.splitlines()

    #  get indexes of formatted query
    index = 0
    for line in split_query:
        print(index, line)
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

    print("From Start Indexes: " + str(from_indexes_start))
    print("From Stop Indexes: " + str(from_indexes_stop))

    # Link the SCAN Nodes to QEP using the "FROM" indexes
    if len(from_indexes_start) != len(from_indexes_stop):
        return Exception("There is an error in the SQL query")

    for i in range(len(from_indexes_start)):
        from_start = from_indexes_start[i]
        from_end = from_indexes_stop[i]
        count = 0
        for line in split_query[from_start:from_end + 1]:
            query_scan, index_scan = get_scans(from_start + count, line)
            if index_scan != -1:
                index_scan_values.append(index_scan)
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

    print("Where Start Indexes: " + str(where_indexes_start))
    print("Where Stop Indexes: " + str(where_indexes_end))

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
    print("Join Indexes: " + str(join_indexes))


    # TODO: get type of join operation for each index - needs to be worked on
    print("reached here")

    join_conditions_list = []

    for index in join_indexes:
        join_condition = split_query[index].split(" ")
        if ("WHERE" in join_condition):
            del join_condition[0:1]
        elif ("AND" in join_condition) or ("OR" in join_condition[index]):
            del join_condition[0:3]
        join_conditions_list.append(join_condition)

    for i in range(len(rawNodeList)):
        if "JOIN" in rawNodeList[i].node_type or "NEST" in rawNodeList[i].node_type:
            if rawNodeList[i].node_type == "HASH JOIN":
                for join_condition in join_conditions_list:
                    count_con = 0
                    if (join_condition[0] in rawNodeList[i].hash_condition) and (join_condition[2] in rawNodeList[i].hash_condition):
                        query_scan = {"index": join_indexes[count_con], "sql": split_query[index], "operation": "HASH JOIN"}
                        operation_list.append(query_scan)
                        break
                    count_con += 1


            elif rawNodeList[i].node_type == "MERGE JOIN":
                for join_condition in join_conditions_list:
                    count_con = 0
                    if (join_condition[0] in rawNodeList[i].merge_condition) and (
                            join_condition[2] in rawNodeList[i].merge_condition):
                        query_scan = {"index": join_indexes[count_con], "sql": split_query[index], "operation": "MERGE JOIN"}
                        operation_list.append(query_scan)
                        break
                    count_con += 1


            elif rawNodeList[i].node_type == "NESTED LOOP":
                if rawNodeList[i].merge_condition is None and rawNodeList[i - 1].node_type == "SEQ SCAN":
                    for join_condition in join_conditions_list:
                        count_con = 0
                        if (join_condition[0] in rawNodeList[i-1].index_condtion) and (
                            join_condition[2] in rawNodeList[i-1].index_condtion):
                            query_scan = {"index": join_indexes[count_con], "sql": split_query[index],
                                            "operation": "INDEX JOIN"}
                            operation_list.append(query_scan)
                            break
                        count_con += 1


                elif rawNodeList[i].merge_condition is not None and rawNodeList[i-1].node_type == "SEQ SCAN":
                    for join_condition in join_conditions_list:
                        count_con = 0
                        if (join_condition[0] in rawNodeList[i - 1].index_condtion) and (
                                join_condition[2] in rawNodeList[i - 1].index_condtion):
                            query_scan = {"index": join_indexes[count_con], "sql": split_query[index],
                                          "operation": "INDEX JOIN"}
                            operation_list.append(query_scan)

                        if (join_condition[0] in rawNodeList[i].join_filter) and (
                                join_condition[2] in rawNodeList[i].join_filter):
                            query_scan = {"index": join_indexes[count_con], "sql": split_query[index],
                                          "operation": "NESTED LOOP JOIN"}
                            operation_list.append(query_scan)
                        count_con += 1

                else:
                    print("Work in progress")
                    # for join_condition in join_conditions_list:
                    #     count_con = 0
                    #     if (join_condition[0] in rawNodeList[i].join_filter) and (
                    #             join_condition[2] in rawNodeList[i].join_filter):
                    #         query_scan = {"index": join_indexes[count_con], "sql": split_query[index],
                    #                       "operation": "NESTED LOOP JOIN"}
                    #         operation_list.append(query_scan)
                    #         break
                    #     count_con += 1
    print()
    for i in operation_list:
        print(operation_list[operation_list.index(i)])

    return operation_list


# get type of scan operation for each index
def get_scans(index, sql):
    count = 0
    for node in nodeListScans:
        if node.relation_name in sql:
            if node.node_type == "INDEX SCAN":
                index_scan_value = count
            else:
                index_scan_value = -1
            query_scan = {"index": index, "sql": sql, "operation": node.node_type,
                          "relation": node.relation_name}
            return query_scan, index_scan_value
        count += 1


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

        current_node = Node(current_plan['Node Type'], relation_name, schema, alias, group_key, sort_key, join_type,
                            index_name, hash_condition, table_filter, index_condition, merge_condition,
                            recheck_condition, join_filter,
                            subplan_name, actual_rows, actual_time, description, cost, sort_type)

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
            or "JOIN" in str(node.node_type): #\
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
    qep_json = json.loads(get_query_plan(query_number, disable))
    nodeListOperations.clear()
    nodeListScans.clear()
    root_node = get_qep_tree(qep_json)
    traverse_tree(root_node, 0)


def get_qep_nodes(query_number, disable=()):
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
    get_operations(query_number)
    return sorted_scan.keys(), nodeListJoins
