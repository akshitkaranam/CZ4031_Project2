import math

from algorithms.bitmapscan import bitmapscan
from algorithms.indexscan import indexscan
from utils import queries
from utils.plan import get_mapping
import itertools
import json
import utils.queries
import sqlparse
from algorithms.mergejoin import mergejoin
from algorithms.hashjoin import hashjoin
from algorithms.indexNLjoin import indexNLjoin
from algorithms.nestedloopjoin import nestedloopjoin
from algorithms.seqscan import seqscan


PARAMS = {
    'hashjoin': 'ON',
    'mergejoin': 'ON',
    'nestloop': 'ON',
    'indexscan': 'ON',
    'bitmapscan': 'ON',
    'seqscan': 'ON',
}


def get_all_plans(query_number):
    query = queries.getQuery(query_number)
    statements = sqlparse.split(query)
    formatted_query = sqlparse.format(statements[0], reindent=True, keyword_case='upper')
    split_query = formatted_query.splitlines()
    index = 0
    for line in split_query:
        print(index, line)
        index += 1

    optimal = get_mapping(query_number)


    # For the various joins
    print("Getting Nested Loop")
    disable = tuple(["hashjoin", "mergejoin", "indexscan", "bitmapscan"])
    nestedloop_list = get_mapping(query_number, disable)


    print("Getting Hash Join")
    disable = tuple(["nestloop", "mergejoin", "indexscan", "bitmapscan"])
    hashjoin_list = get_mapping(query_number, disable)

    print("Getting Merge Join")
    disable = tuple(["nestloop", "hashjoin", "indexscan", "bitmapscan"])
    mergejoin_list = get_mapping(query_number, disable)


    print("Getting Index Join")
    disable = tuple(["nestloop", "mergejoin", "hashjoin"])
    indexjoin_list = get_mapping(query_number, disable)



    optimal_dict = {}
    if optimal:
        for line_index in optimal:
            index = line_index["index"]
            operation = line_index['operation']
            nodes = line_index["nodes"]
            total_cost = 0
            index_cond = ""
            for node in nodes:
                if(node.node_type == "INDEX SCAN"):
                    index_cond = node.index_condition

                total_cost += node.cost
            if "SCAN" in operation:
                if "INDEX SCAN" in operation:
                    optimal_dict.update({index: [operation, total_cost, line_index['relation'],index_cond]})
                else:
                    optimal_dict.update({index: [operation, total_cost,line_index['relation']]})
            else:
                optimal_dict.update({index: [operation, total_cost]})

    hashjoin_dict = {}
    if hashjoin_list:
        for line_index in hashjoin_list:
            index = line_index["index"]
            operation = line_index["operation"]
            nodes = line_index["nodes"]
            total_cost = 0
            for node in nodes:
                total_cost += node.cost
            hashjoin_dict.update({index: [operation, total_cost]})

    nestedloop_dict = {}
    if nestedloop_list:
        for line_index in nestedloop_list:
            index = line_index["index"]
            operation = line_index["operation"]
            nodes = line_index["nodes"]
            total_cost = 0
            for node in nodes:
                total_cost += node.cost
            nestedloop_dict.update({index: [operation, total_cost]})

    indexjoin_dict = {}
    if indexjoin_list:
        for line_index in indexjoin_list:
            index = line_index["index"]
            operation = line_index["operation"]
            nodes = line_index["nodes"]
            total_cost = 0
            for node in nodes:
                total_cost += node.cost
            indexjoin_dict.update({index: [operation, total_cost]})

    mergejoin_dict = {}
    if mergejoin_list:
        for line_index in mergejoin_list:
            index = line_index["index"]
            operation = line_index["operation"]
            nodes = line_index["nodes"]
            total_cost = 0
            for node in nodes:
                total_cost += node.cost
            mergejoin_dict.update({index: [operation, total_cost]})


    annotations = {}
    for index in optimal_dict.keys():
        if optimal_dict[index][0] == "MERGE JOIN":
            annotation = getAnnotationsJoins("MERGE JOIN", index, mergejoin, optimal_dict, nestedloop_dict,
                                             indexjoin_dict, hashjoin_dict, mergejoin_dict)
            annotations.update({index: annotation})
        elif optimal_dict[index][0] == "HASH JOIN":
            annotation = getAnnotationsJoins("HASH JOIN", index, hashjoin, optimal_dict, nestedloop_dict,
                                             indexjoin_dict, hashjoin_dict, mergejoin_dict)
            annotations.update({index: annotation})
        elif optimal_dict[index][0] == "NESTED LOOP":
            annotation = getAnnotationsJoins("NESTED LOOP", index, nestedloopjoin, optimal_dict, nestedloop_dict,
                                             indexjoin_dict, hashjoin_dict, mergejoin_dict)
            annotations.update({index: annotation})
        elif optimal_dict[index][0] == "INDEX JOIN":
            annotation = getAnnotationsJoins("INDEX JOIN", index, indexNLjoin, optimal_dict, nestedloop_dict,
                                             indexjoin_dict, hashjoin_dict, mergejoin_dict)
            annotations.update({index: annotation})

        elif optimal_dict[index][0] == "SEQ SCAN":
            annotation = seqscan(optimal_dict[index][2])
            annotations.update({index: annotation})

        elif optimal_dict[index][0] == "BITMAP SCAN":
            annotation = bitmapscan(optimal_dict[index][2])
            annotations.update({index: annotation})

        elif optimal_dict[index][0] == "INDEX SCAN":
            annotation = indexscan(relation_name=optimal_dict[index][2],join_condition=optimal_dict[index][3])
            annotations.update({index: annotation})

    print(annotations)


def getAnnotationsJoins(operation, index, operation_function, optimal_dict, nestedloop_dict, indexjoin_dict,
                        hashjoin_dict,
                        mergejoin_dict):
    nested_cost = -1
    index_nest_cost = -1
    hash_cost = -1
    merge_cost = -1
    optimal_cost = -1

    if not nestedloop_dict.keys():
        nested_cost = math.inf
    if index in nestedloop_dict.keys():
        nested_operation = nestedloop_dict[index][0]
        if nested_operation != operation:
            nested_cost = nestedloop_dict[index][1]

    if not indexjoin_dict.keys():
        index_nest_cost = math.inf
    if index in indexjoin_dict.keys():
        indexjoin_operation = indexjoin_dict[index][0]
        if indexjoin_operation != operation:
            index_nest_cost = indexjoin_dict[index][1]

    if not hashjoin_dict.keys():
        hash_cost = math.inf
    if index in hashjoin_dict.keys():
        hashjoin_operation = hashjoin_dict[index][0]
        if hashjoin_operation != operation:
            hash_cost = hashjoin_dict[index][1]

    if not mergejoin_dict.keys():
        merge_cost = math.inf
    if index in mergejoin_dict.keys():
        merge_operation = mergejoin_dict[index][0]
        if merge_operation != operation:
            merge_cost = mergejoin_dict[index][1]

    if index in optimal_dict.keys():
        optimal_cost = optimal_dict[index][1]

    annotation = operation_function(optimal_cost, hash_cost, merge_cost, nested_cost, index_nest_cost)
    print(annotation)
    return annotation



if __name__ == '__main__':
    mapping = get_all_plans(5)
