
def nestloopjoin(hashcost, mergecost, nestloopcost):
    print("This join is implemented using NL joins operator as hash join and merge join \
        increase the estimated cost by at least " + int(hashcost/nestloopcost) + " times and " \
            + int(mergecost/nestloopcost) + " times, respectively")