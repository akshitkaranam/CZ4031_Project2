import math


def hashjoin(optimalcost, hashcost, mergecost, nestloopcost, indexnestloopcost):
    scale_merge = str("%.2f" % float(mergecost / optimalcost))
    scale_nested_loop = str("%.2f" % float(nestloopcost / optimalcost))
    scale_index_nested_loop = str("%.2f" % float(indexnestloopcost / optimalcost))
    timeoutarray = []
    notapplicable = []

    annotation = "This join is implemented using HASH JOIN, the cost for this operation is:  " + str("%.2f" %optimalcost)

    if mergecost != math.inf:
        if mergecost == -1:
            notapplicable.append(". MERGE JOIN IS NOT APPLICABLE")
        else:
            annotation += ". MERGE JOIN would have costed: " + str("%.2f" % mergecost) + " which costs " + str(scale_merge) \
                      + " times more"

    else:
        timeoutarray.append(". MERGE JOIN TIMES OUT!")

    if nestloopcost != math.inf:
        if nestloopcost == -1:
            notapplicable.append(". NESTED LOOP JOIN IS NOT APPLICABLE")
        else:
            annotation += ". NESTED LOOP JOIN would have costed: " + str("%.2f" % nestloopcost) + " which costs " \
                      + str(scale_nested_loop) + " times more"
    else:
        timeoutarray.append(". NESTED LOOP JOIN TIMES OUT!")

    if indexnestloopcost != math.inf:
        if indexnestloopcost == -1:
            notapplicable.append(". INDEX NESTED LOOP JOIN IS NOT APPLICABLE")
        else:
            annotation += ". INDEX NESTED LOOP JOIN would have costed: " + str("%.2f" % indexnestloopcost) \
                      + " which costs " + str(scale_index_nested_loop) + " times more"
    else:
        timeoutarray.append(". INDEX NESTED LOOP JOIN TIMES OUT!")

    for na in notapplicable:
        annotation += na
    for timeout in timeoutarray:
        annotation += timeout

    return annotation
