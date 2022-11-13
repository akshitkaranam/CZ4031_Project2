def seqscan(relation):
    annotation = "This table, " + str(relation).upper() + ", is read using sequential scan. " \
                                                          "This is because no index is created on the table."

    return annotation
