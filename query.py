from utils.plan import get_qep_nodes, generateAQPs

if __name__ == '__main__':

    disable = tuple(["hashjoin"])
    list_scan, list_join = get_qep_nodes(3)

    print()
    for node in list_scan:
        # print(node.__repr__())
        print(node.node_type, node.relation_name)

    print()
    print()
    for node in list_join:
        # print(node.node_type)
        # print(node.children)
        if node.node_type == "HASH JOIN":
            print(node.node_type, node.hash_condition)
        elif node.node_type == "MERGE JOIN":
            print(node.node_type, node.merge_condition)
        elif node.node_type == "NESTED LOOP":
            print(node.node_type, node.join_filter)
        else:
            print(node.node_type)

    print()
    print()
    print("start generating aqps")
    aqps = generateAQPs(3)