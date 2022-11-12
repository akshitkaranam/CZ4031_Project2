from utils import queries
from utils.plan import get_mapping
import itertools
import json
import utils.queries
import sqlparse

PARAMS = {
    'hashjoin': 'ON',
    'mergejoin': 'ON',
    'nestloop': 'ON',
    'indexscan': 'ON',
    'bitmapscan': 'ON',
    'seqscan': 'ON',
}


def get_all_plans(query_number, disable_methods=()):
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
    nestedloop = get_mapping(query_number, disable)

    print("Getting Hash Join")
    disable = tuple(["nestloop", "mergejoin", "indexscan", "bitmapscan"])
    hashjoin = get_mapping(query_number, disable)

    print("Getting Merge Join")
    disable = tuple(["nestloop", "hashjoin", "indexscan", "bitmapscan"])
    mergejoin = get_mapping(query_number, disable)

    print("Getting Index Join")
    disable = tuple(["nestloop", "mergejoin", "hashjoin"])
    indexjoin = get_mapping(query_number, disable)

    # For the various scans
    print("Getting Seq Scan")
    disable = tuple(["indexscan", "bitmapscan"])
    seqscan = get_mapping(query_number, disable)

    print("Getting Index Scan")
    disable = tuple(["seqscan", "bitmapscan"])
    indexscan = get_mapping(query_number, disable)

    print("Getting Bitmap Scan")
    disable = tuple(["indexscan", "seqscan"])
    bitmapscan = get_mapping(query_number, disable)

    optimal_dict = {}
    if optimal:
        for line_index in optimal:
            index = line_index["index"]
            operation = line_index['operation']
            nodes = line_index["nodes"]
            total_cost = 0
            for node in nodes:
                total_cost += node.cost
            print(index, operation, total_cost)
    print()
    print()
    if hashjoin:
        for line_index in hashjoin:
            index = line_index["index"]
            operation = line_index["operation"]
            nodes = line_index["nodes"]
            total_cost = 0
            for node in nodes:
                total_cost += node.cost
            print(index,operation,total_cost)
    print()
    print()
    if mergejoin:
        for line_index in mergejoin:
            index = line_index["index"]
            operation = line_index["operation"]
            nodes = line_index["nodes"]
            total_cost = 0
            for node in nodes:
                total_cost += node.cost
            print(index,operation,total_cost)
    print()
    print()

if __name__ == '__main__':
    mapping = get_all_plans(5)
