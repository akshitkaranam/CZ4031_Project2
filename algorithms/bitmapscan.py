def bitmapscan(relation):
    annotation = "This table, " + str(relation).upper() + ", is read using bitmap scan. " \
                                                          " This is because there are multiple indexes created " \
                                                          "on the table."

    return annotation