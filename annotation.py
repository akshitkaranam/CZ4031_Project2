from utils.plan import get_mapping
import itertools
import json

PARAMS = {
    'hashjoin': 'ON',
    'mergejoin': 'ON',
    'nestloop': 'ON',
    'indexscan': 'ON',
    'bitmapscan': 'ON',
    'seqscan': 'ON',
}


def get_all_plans(query_number, disable_methods=()):
    optimal = get_mapping(query_number)

    # For the various joins
    print("Getting Nested Loop")
    disable = tuple(["hashjoin","mergejoin","indexscan","bitmapscan"])
    nestedloop = get_mapping(query_number,disable)

    print("Getting Hash Join")
    disable = tuple(["nestloop", "mergejoin", "indexscan", "bitmapscan"])
    hashjoin = get_mapping(query_number,disable)

    print("Getting Merge Join")
    disable = tuple(["nestloop", "hashjoin", "indexscan", "bitmapscan"])
    mergejoin = get_mapping(query_number,disable)

    print("Getting Index Join")
    disable = tuple(["nestloop", "mergejoin", "hashjoin"])
    indexjoin = get_mapping(query_number,disable)

    # For the various scans
    print("Getting Seq Scan")
    disable = tuple(["indexscan", "bitmapscan"])
    seqscan = get_mapping(query_number,disable)

    print("Getting Index Scan")
    disable = tuple(["seqscan", "bitmapscan"])
    indexscan = get_mapping(query_number, disable)

    print("Getting Bitmap Scan")
    disable = tuple(["indexscan", "seqscan"])
    bitmapscan = get_mapping(query_number,disable)

    if optimal:
        for line_index in optimal:
            print(line_index)









def generateAQPs(query_number):

    aqps = []
    #count is the number of kinds of join
    count = 6
    permutations = list(itertools.product(["ON", "OFF"], repeat=count))
    max = 10
    cur = 0
    for p in permutations:
        cur += 1
        i = 0
        alt_params = PARAMS.copy()
        for key, value in alt_params.items():
            if value == "ON":
                alt_params.update({key: p[i]})
                i += 1
            if i == count:
                break
        disable = []

        for key, value in alt_params.items():
            if value == "OFF":
                disable.append(key)
        new_disable = tuple(disable)
        temp = json.dumps(get_query_plan(query_number, new_disable))
        aqp_json = json.loads(temp)
        if aqp_json:
            aqps.append(aqp_json)
            print(aqp_json)
            if (cur > max):
                break
    return aqps

if __name__ == '__main__':
    mapping = get_all_plans(5)

