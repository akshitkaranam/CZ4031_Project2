def q1():
        return "SELECT l_returnflag,l_linestatus,sum(l_quantity) as sum_qty,sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order FROM lineitem WHERE l_shipdate <= date '1998-12-01' - interval '90' day GROUP BY l_returnflag,l_linestatus ORDER BY l_returnflag,l_linestatus;"


def q10():
    return "SELECT c_custkey,c_name,sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment FROM customer,orders,lineitem,nation WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate >= date '1993-10-01' AND o_orderdate < date '1993-10-01' + interval '3' month AND l_returnflag = 'R' AND c_nationkey = n_nationkey GROUP BY c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment ORDER BY revenue desc LIMIT 20;"


def q12():
    return "SELECT l_shipmode,sum(case when o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,sum(case when o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' then 1 else 0 end) AS low_line_count FROM orders,lineitem WHERE o_orderkey = l_orderkey AND l_shipmode in ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate AND l_receiptdate >= date '1994-01-01' AND l_receiptdate < date '1994-01-01' + interval '1' year GROUP BY l_shipmode ORDER BY l_shipmode;"


def q14():
    return "SELECT 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue FROM lineitem,part WHERE l_partkey = p_partkey AND l_shipdate >= date '1995-09-01' AND l_shipdate < date '1995-09-01' + interval '1' month;"


def q19():
    return "SELECT sum(l_extendedprice* (1 - l_discount)) as revenue FROM lineitem,part WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12' AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') AND l_quantity >= 1 AND l_quantity <= 1 + 10 AND p_size between 1 AND 5 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#23' AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') AND l_quantity >= 10 AND l_quantity <= 10 + 10 AND p_size between 1 AND 10 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') OR (p_partkey = l_partkey AND p_brand = 'Brand#34' AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') AND l_quantity >= 20 AND l_quantity <= 20 + 10 AND p_size between 1 AND 15 AND l_shipmode in ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON');"


def q3():
    return "SELECT l_orderkey,sum(l_extendedprice * (1 - l_discount)) as revenue,o_orderdate,o_shippriority FROM customer,orders,lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < date '1995-03-15' AND l_shipdate > date '1995-03-15' GROUP BY l_orderkey,o_orderdate,o_shippriority ORDER BY revenue desc,o_orderdate LIMIT 20;"


def q5():
    return "SELECT n_name,sum(l_extendedprice * (1 - l_discount)) as revenue FROM customer,orders,lineitem,supplier,nation,region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'ASIA' AND o_orderdate >= date '1994-01-01' AND o_orderdate < date '1994-01-01' + interval '1' year GROUP BY n_name ORDER BY revenue desc;"


def q6():
    return "SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1994-01-01' + interval '1' year AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01 AND l_quantity < 24;"

def q2():
    return "SELECT o_orderpriority, count(*) as order_count FROM orders WHERE o_orderdate >= date '1996-03-01' AND exists (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority LIMIT 1;"

def customeQuery(query):
    return query

def getQuery(query_number,custom=None):
    if query_number == 1:
        return q1()
    elif query_number == 3:
        return q3()
    elif query_number == 5:
        return q5()
    elif query_number == 6:
        return q6()
    elif query_number == 10:
        return q10()
    elif query_number == 12:
        return q12()
    elif query_number == 14:
        return q14()
    elif query_number == 19:
        return q19()
    elif query_number == 2:
        return q2()
    elif query_number == 999:
        customeQuery(custom)
    else:
        return None
