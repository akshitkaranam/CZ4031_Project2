def indexscan(index_name="", join_condition=""):
    annotation = "The table is read using index scan. "

    if index_name == "":
        annotation += "An index on the primary key is created automatically by PostgreSQL. " \
                      " This operation is used in conjunction of the join condition:  " + str(join_condition)

    else:
        annotation +=  "This is because there is an index:" + str(index_name)  + "created on the table."

    return annotation
