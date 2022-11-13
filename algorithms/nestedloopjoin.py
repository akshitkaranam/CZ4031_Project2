import math


def nestedloopjoin(optimalcost, hashcost, mergecost, nestloopcost, indexnestloopcost):
    scale_merge = str("%.2f" % float(mergecost / optimalcost))
    scale_index_nested_loop = str("%.2f" % float(indexnestloopcost / optimalcost))
    scale_hash = str("%.2f" % float(hashcost / optimalcost))
    timeoutarray = []
    notapplicable = []

    annotation = "This join is implemented using NESTED LOOP JOIN, the cost for this operation is:  " + str(
        "%.2f" % optimalcost)

    if mergecost != math.inf:
        if mergecost == -1:
            notapplicable.append(". MERGE JOIN IS NOT APPLICABLE")
        else:
            annotation += ". MERGE JOIN would have costed: " + str("%.2f" % mergecost) + " which costs " + str(scale_merge) \
                      + " times more"
    else:
        timeoutarray.append(". MERGE JOIN TIMES OUT!")

    if indexnestloopcost != math.inf:
        if indexnestloopcost == -1:
            notapplicable.append(". INDEX NESTED LOOP JOIN IS NOT APPLICABLE")
        else:
            annotation += ". INDEX NESTED LOOP JOIN would have costed: " + str("%.2f" % indexnestloopcost) + " which costs " \
                      + str(scale_index_nested_loop) + " times more"
    else:
        timeoutarray.append(". NESTED LOOP JOIN TIMES OUT!")

    if hashcost != math.inf:
        if hashcost == -1:
            notapplicable.append(". HASH JOIN IS NOT APPLICABLE")
        else:
            annotation += ". HASH JOIN would have costed: " + str("%.2f" % hashcost) \
                      + " which costs " + str(scale_hash) + " times more"
    else:
        timeoutarray.append(". HASH JOIN TIMES OUT!")

    for na in notapplicable:
        annotation += na
    for timeout in timeoutarray:
        annotation += timeout

    return annotation
