def indexscan(relation_name, index_name="", join_condition=""):
    annotation = "This table, " + str(relation_name).upper() + " is read using index scan. "

    if index_name == "":
        annotation += "An index on the primary key is created automatically by PostgreSQL." \
                      "This operation is used in conjunction with the join condition:  " + str(join_condition)

    else:
        annotation += "This is because there is an index:" + str(index_name) + "created on the table."

    return annotation
