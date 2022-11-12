
def mergejoin(hashcost, mergecost, nestloopcost):
    print("This join is implemented using merge join operator as hash join and NL joins \
        increase the estimated cost by at least " + int(hashcost/mergecost) + " times and " \
            + int(nestloopcost/mergecost) + " times, respectively")