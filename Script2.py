#!/usr/bin/env python

import psycopg2
import os
import time
import getpass
import itertools
import statistics
from os.path import expanduser
from git import Repo
from unidiff import PatchSet

os.system("pip install unidiff --user")
os.system("pip install psycopg2 --user")

NUM_COMMITS = input("Please type the number of the commits you would like to check for performance regression files : ")
NUM_RUNS = 5

all_queries = ["""Select l_returnflag,l_linestatus,sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,sum(l_extendedprice * (1-l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc,count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '90' day group by l_returnflag,l_linestatus order by l_returnflag,l_linestatus;""",
        """Select s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment from part,supplier,partsupp,nation,region where p_partkey=ps_partkey and s_suppkey=ps_suppkey and p_size=15 and p_type like '%BRASS' and s_nationkey=n_nationkey and n_regionkey=r_regionkey and r_name='EUROPE' and ps_supplycost=(Select min(ps_supplycost) from partsupp,supplier,nation,region where p_partkey=ps_partkey and s_suppkey=ps_suppkey and s_nationkey=n_nationkey and n_regionkey=r_regionkey and r_name='EUROPE') order by s_acctbal desc, n_name, s_name, p_partkey limit 100;""",
        """Select l_orderkey,sum(l_extendedprice * (1-l_discount)) as revenue, o_orderdate,o_shippriority from customer,orders,lineitem where c_mktsegment='BUILDING' and c_custkey=o_custkey and l_orderkey=o_orderkey and o_orderdate<date '1995-03-15' and l_shipdate>date '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate limit 10;""",
        """Select o_orderpriority, count(*) as order_count from orders where o_orderdate>= date '1993-07-01' and o_orderdate < date '1993-07-01' + interval '3' month and exists ( Select * from lineitem where l_orderkey=o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority;""",
        """Select n_name, sum(l_extendedprice * (1-l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey=o_custkey and l_orderkey=o_orderkey and l_suppkey=s_suppkey and c_nationkey=n_nationkey and n_regionkey=r_regionkey and r_name='ASIA' and o_orderdate >=date '1994-01-01' and o_orderdate>=date '1994-01-01' and o_orderdate<date '1994-01-01' + interval '1' year group by n_name order by revenue desc;""",
        """Select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate>=date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_discount between 0.06 - 0.01 and 0.06 + 0.01 and l_quantity<24;""",
        """Select supp_nation,cust_nation,l_year,sum(volume) as revenue from ( Select n1.n_name as supp_nation, n2.n_name as cust_nation, extract(year from l_shipdate) as l_year, l_extendedprice * (1-l_discount) as volume from supplier,lineitem,orders,customer,nation n1, nation n2 where s_suppkey=l_suppkey and o_orderkey=l_orderkey and c_custkey=o_custkey and s_nationkey=n1.n_nationkey and c_nationkey=n2.n_nationkey and ((n1.n_name='FRANCE' and n2.n_name='GERMANY') or (n1.n_name='GERMANY' and n2.n_name='FRANCE')) and l_shipdate between date '1995-01-01' and date '1996-12-31') as shipping group by supp_nation,cust_nation,l_year order by supp_nation,cust_nation,l_year;""",
        """Select o_year, sum(case when nation='BRAZIL' then volume else 0 end)/sum(volume) as mkt_share from (Select extract(year from o_orderdate) as o_year,l_extendedprice * (1-l_discount) as volume, n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey=l_partkey and s_suppkey=l_suppkey and l_orderkey=o_orderkey and o_custkey=c_custkey and c_nationkey=n1.n_nationkey and n1.n_regionkey=r_regionkey and r_name='AMERICA' and s_nationkey=n2.n_nationkey and o_orderdate between date '1995-01-01' and date '1996-12-31' and p_type='ECONOMY ANODIZED STEEL') as all_nations group by o_year order by o_year;""",
        """Select nation,o_year,sum(amount) as sum_profit from ( Select n_name as nation, extract(year from o_orderdate) as o_year, l_extendedprice * (1-l_discount)-ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey=l_suppkey and ps_suppkey=l_suppkey and ps_partkey=l_partkey and p_partkey=l_partkey and o_orderkey=l_orderkey and s_nationkey=n_nationkey and p_name like '%green%') as profit group by nation, o_year order by nation,o_year desc;""",
        """Select c_custkey, c_name, sum(l_extendedprice * (1-l_discount)) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey=o_custkey and l_orderkey=o_orderkey and o_orderdate>=date '1993-10-01' and o_orderdate < date '1993-10-01' + interval '3' month and l_returnflag='R' and c_nationkey=n_nationkey group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment order by revenue desc limit 20;""",
        """Select ps_partkey, sum(ps_supplycost * ps_availqty) as value from partsupp, supplier, nation where ps_suppkey=s_suppkey and s_nationkey=n_nationkey and n_name='GERMANY' group by ps_partkey having sum(ps_supplycost * ps_availqty)>(Select sum(ps_supplycost * ps_availqty) * 0.0001000000 from partsupp, supplier, nation where ps_suppkey=s_suppkey and s_nationkey=n_nationkey and n_name='GERMANY') order by value desc;""",
        """Select l_shipmode,sum(case when o_orderpriority='1-URGENT' or o_orderpriority='2-HIGH' then 1 else 0 end) as high_line_count, sum(case when o_orderpriority <> '1=URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders,lineitem where o_orderkey=l_orderkey and l_shipmode in('MAIL','SHIP') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= date '1994-01-01' and l_receiptdate < date '1004-01-01'+ interval '1' year group by l_shipmode order by l_shipmode;""",
        """Select c_count,count(*) as custdist from ( Select c_custkey, count(o_orderkey) from customer left outer join orders on c_custkey=o_custkey and o_comment not like '%special%requests%' group by c_custkey) as c_orders(c_custkey,c_count) group by c_count order by custdist desc, c_count desc;""",
        """Select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1-l_discount) else 0 end) / sum(l_extendedprice * (1-l_discount)) as promo_revenue from lineitem,part where l_partkey=p_partkey and l_shipdate >= date '1995-09-01' and l_shipdate < date '1995-09-01' + interval '1' month;""",
        """Create view revenue0(supplier_no, total_revenue) as select l_suppkey, sum(l_extendedprice * (1-l_discount)) from lineitem where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month group by l_suppkey; Select s_suppkey,s_name, s_address, s_phone, total_revenue from supplier, revenue0 where s_suppkey=supplier_no and total_revenue=(select max(total_revenue) from revenue0) order by s_suppkey; drop view revenue0;""",
        """Select p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey=ps_partkey and p_brand <> 'Brand#45'and p_type not like 'MEDIUM POLISHED%' and p_size in (49,14,23,45,19,3,36,9) and ps_suppkey not in( Select s_suppkey from supplier where s_comment like '%Customer%Complaints%') group by p_brand,p_type,p_size order by supplier_cnt desc, p_brand,p_type, p_size;""",
        """Select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey=l_partkey and p_brand='Brand#23' and p_container='MED BOX' and l_quantity < (Select 0.2 * avg(l_quantity) from lineitem where l_partkey=p_partkey);""",
        """Select c_name,c_custkey,o_orderkey, o_orderdate, o_totalprice, sum(l_quantity) from customer, orders, lineitem where o_orderkey in (Select l_orderkey from lineitem group by l_orderkey having sum(l_quantity)>300) and c_custkey=o_custkey and o_orderkey=l_orderkey group by c_name, c_custkey, o_orderkey,o_orderdate,o_totalprice order by o_totalprice desc, o_orderdate limit 100;""",
        """Select sum(l_extendedprice * (1-l_discount)) as revenrue from lineitem, part where ( p_partkey=l_partkey and p_brand='Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >=1 and l_quantity <= 1+10 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct='DELIVER IN PERSON') or ( p_partkey=l_partkey and p_brand ='Brand#23' and p_container in ('MED BAG','MED BOX', 'MED PKG', 'MED PACK') and l_quantity >=10 and l_quantity <= 10+10 and p_size between 1 and 10 and l_shipmode in ('AIR','AIR REG') and l_shipinstruct='DELIVER IN PERSON') or (p_partkey=l_partkey and p_brand ='Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 20 and l_quantity <=20+10 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct='DELIVER IN PERSON');""",
        """Select s_name, s_address from supplier, nation where s_suppkey in ( Select ps_suppkey from partsupp where ps_partkey in (Select p_partkey from part where p_name like 'forest%' ) and ps_availqty > ( Select 0.5 * sum(l_quantity) from lineitem where l_partkey=ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year)) and s_nationkey=n_nationkey and s_name ='CANADA' order by s_name;""",
        """Select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey=l1.l_suppkey and o_orderkey=l1.l_orderkey and o_orderstatus='F' and l1.l_receiptdate > l1.l_commitdate and exists ( Select * from lineitem l2 where l2.l_orderkey=l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey) and not exists ( Select * from lineitem l3 where l3.l_orderkey =l1.l_orderkey and l3.l_suppkey<> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate) and s_nationkey=n_nationkey and n_name='SAUDI ARABIA' group by s_name order by numwait desc, s_name limit 100;""",
        """Select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( Select substring(c_phone from 1 for 2) as cntrycode,c_acctbal from customer where substring(c_phone from 1 for 2) in ('13','31','23','29','30','18','17') and c_acctbal > ( Select avg(c_acctbal) from customer where c_acctbal >0.00 and substring (c_phone from 1 for 2) in ('13','31','23','29','30','18','17')) and not exists ( Select * from orders where o_custkey=c_custkey)) as custsale group by cntrycode order by cntrycode;"""]

Uservar=getpass.getuser()
home=expanduser("~")

DirvarTpch=raw_input("Please type the name of the directory that you want to save TPCH: ")

pathcustomer=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','customer.tbl')
pathnation=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','nation.tbl')
pathpartsupp=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','partsupp.tbl')
pathregion=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','region.tbl')
pathlineitem=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','lineitem.tbl')
pathorders=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','orders.tbl')
pathpart=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','part.tbl')
pathsupplier=os.path.join(home,DirvarTpch,'pg-tpch','dbgen','supplier.tbl')



while os.path.exists(os.path.join(home,DirvarTpch))==True:
    DirvarTpch=raw_input("The folder already exists, please type another name: ")

os.chdir(home)
os.mkdir(DirvarTpch)
os.chdir(DirvarTpch)
os.system("git clone https://github.com/2ndQuadrant/pg-tpch")
os.chdir("pg-tpch")
os.chdir("dbgen")
os.system("make -s")
Scalefactor=raw_input("Please type the scale factor number you would like for tpch : ")
os.system("./dbgen -s %s" %Scalefactor)
os.system("for i in `ls *.tbl`; do sed 's/|$//' $i > ${i/tbl/csv}; echo $i; done;")
        
Dirvar=raw_input("Please type the directory name in which you have Postgres installed or you want to install it: ")

pathDirvar=os.path.join(home,Dirvar)
pathupdatecommit=os.path.join(home,Dirvar,'postgres')
pathpostgres=os.path.join(home,Dirvar,'postgres')
pathbin=os.path.join(home,Dirvar,'bin')
pathpatchfile=os.path.join(home,Dirvar,'postgres','testfile1.patch')

def run_queries(cur, queries):
    results = []
    for i in xrange(len(queries)):
        run_times = []
        for _ in xrange(NUM_RUNS): 
            start = time.time()
            cur.execute(queries[i])
            end = time.time()
            run_times.append(end - start)
        result_obj = {
            'cold': run_times[0],
            'min': min(run_times[1:]),
            'max': max(run_times[1:]),
            'avg': (sum(run_times[1:]) / (len(run_times) - 1))
        }
        results.append(result_obj)
        print ("The cold run for query %d was: %s seconds" % (i + 1, result_obj['cold']))
        print ("The fastest time for query %d hot runs was : %s seconds" % (i + 1, result_obj['min']))
        print ("The slowest time for query %d hot runs was : %s" % (i + 1, result_obj['max']))
        print ("Average time for query  %d hot runs is : %s seconds" % (i + 1, result_obj['avg']))
        print ("---------------------------------------------------------------")
    return results


if os.path.exists(os.path.join(home,Dirvar))==True:
    print "PostGresql is already installed."
    os.chdir(home)
    os.chdir(Dirvar)
    os.chdir('postgres')
    Pversion=os.popen('psql -V').read()
    print ("The version of Postgresql you have installed is : %s " %Pversion)
    Curcommit=os.popen("git rev-parse HEAD").read()
    print ("Current commit is : %s" %Curcommit)
    print ("Checking if there is a new commit...")
    os.system("git pull -q https://github.com/postgres/postgres")
    os.system("make -s -j 5")
    os.system("make install -s -j 5")
    Curcommit=os.popen("git rev-parse HEAD").read()
    print ("Current commit is : %s" %Curcommit)
    Datadir=raw_input("Please identify the directory where the data for this database will reside: ")
    while os.path.exists(os.path.join(home,Dirvar,Datadir))==True:
        Datadir=raw_input("The directory already exists, please specify the directory where the data for this database will reside: ")
    else:
        os.chdir(pathDirvar)
        os.chdir(pathbin)
	os.system("./initdb -D %s" % Datadir)
	print "Starting PostgreSQL.."
    os.system("./pg_ctl -D %s -l logfile start" % Datadir)
    DBname=raw_input("Please specify the name of your database: ")
    os.system("./createdb %s" % DBname)
    print ("Creating the tables...")

    conn=psycopg2.connect("dbname='%s' user='%s' host='localhost'" % (DBname,Uservar))
 
    cur=conn.cursor()
    
    cur.execute("""Create table nation (n_nationkey integer, n_name text, n_regionkey integer, n_comment text)""")
    cur.execute("""Create index GRulez0 on nation (n_nationkey);""")

    cur.execute("""Create table supplier (s_suppkey integer, s_name text, s_address text, s_nationkey integer, s_phone text, s_acctbal decimal, s_comment text)""")
    cur.execute("""Create index GRulez1 on supplier (s_suppkey);""")

    cur.execute("""Create table region (r_regionkey integer, r_name text, r_comment text)""")
    cur.execute("""Create index GRulez2 on region (r_regionkey);""")

    cur.execute("""Create table customer (c_custkey integer,c_name text, c_address text, c_nationkey integer, c_phone text, c_acctbal decimal, c_mktsegment text, c_comment text)""")
    cur.execute("""Create index Grulez3 on customer (c_custkey);""")

    cur.execute("""Create table orders (o_orderkey integer, o_custkey integer, o_orderstatus text,o_totalprice decimal, o_orderdate date, o_orderpriority text, o_shippriority text, o_comment text, extracol text)""")
    cur.execute("""Create index Grulez4 on orders (o_orderkey);""")

    cur.execute(""" Create table part(p_partkey integer, p_name text, p_mfgr text, p_brand text, p_type text, p_size integer, p_container text, p_retailprice decimal, p_comment text)""")
    cur.execute("""Create index GRulez5 on part (p_partkey);""")

    cur.execute("""Create table partsupp (ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost decimal, ps_comment text)""")
    cur.execute("""Create index GRulez6 on partsupp (ps_partkey);""")

    cur.execute("""Create table lineitem (l_orderkey integer, l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity decimal, l_extendedprice decimal, l_discount decimal, l_tax decimal,l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text)""")
    cur.execute("""Create index GRulez7 on lineitem (l_partkey);""")


    startloading1=time.time()

    cur.execute("""Copy customer from '%s' (format csv, delimiter ('|'));""" % pathcustomer)

    cur.execute("""Copy lineitem from '%s' (format csv, delimiter ('|'));""" % pathlineitem)

    cur.execute("""Copy nation from '%s' (format csv, delimiter ('|'));""" % pathnation)

    cur.execute("""Copy orders from '%s' (format csv, delimiter ('|'));""" % pathorders)

    cur.execute("""Copy part  from '%s' (format csv, delimiter ('|'));""" % pathpart)

    cur.execute("""Copy partsupp from '%s' (format csv, delimiter ('|'));""" % pathpartsupp)

    cur.execute("""Copy region from '%s' (format csv, delimiter ('|'));""" % pathregion)

    cur.execute("""Copy supplier from '%s' (format csv, delimiter ('|'));""" % pathsupplier)
    endloading1=time.time()
    difloading1=endloading1-startloading1

    print "Done loading the tables"
    print ("Time loading the tables : %s seconds" %difloading1)

        
    os.chdir(pathpostgres)
    currentcom=os.popen("git rev-parse HEAD").read()
    print ("The current commit is : %s " %currentcom)
    totalcom=os.popen("git rev-list --count HEAD").read()
    print ("Number of total commits : %s" %totalcom)
    print ("Rolling back %d commits..." %NUM_COMMITS)
    os.system("git reset --hard HEAD~%d" %NUM_COMMITS)
    os.system("make -s -j 5")
    os.system("make install -s -j 5")        



        
    for commit in xrange(1, NUM_COMMITS + 1):
        os.chdir(pathpostgres)
        currentcom=os.popen("git rev-parse HEAD").read()
        print ("The current commit is : %s " %currentcom)
        print ("Running the queries...")
        old_results = run_queries(cur, all_queries)

        print ("Dropping the tables..")
        cur.execute("""Drop table nation;""")
        cur.execute("""Drop table supplier;""")
        cur.execute("""Drop table region;""")
        cur.execute("""Drop table customer;""")
        cur.execute("""Drop table orders;""")
        cur.execute("""Drop table part;""")
        cur.execute("""Drop table partsupp;""")
        cur.execute("""Drop table lineitem;""")

        print ("to the next commit ... !")
        os.system("git checkout HEAD@{1}")
        currentcom2=os.popen("git rev-parse HEAD").read()
        print ("Current commit is : %s " %currentcom2)

        print "Recompliling the new commit of PostGresql..."
        os.system("make -s -j 5")
        os.system("make install -s -j 5")
            
            
        print ("Reloading the tables...")
        cur.execute("""Create table nation (n_nationkey integer, n_name text, n_regionkey integer, n_comment text)""")
        cur.execute("""Create index GRulez0 on nation (n_nationkey);""")

        cur.execute("""Create table supplier (s_suppkey integer, s_name text, s_address text, s_nationkey integer, s_phone text, s_acctbal decimal, s_comment text)""")
        cur.execute("""Create index GRulez1 on supplier (s_suppkey);""")

        cur.execute("""Create table region (r_regionkey integer, r_name text, r_comment text)""")
        cur.execute("""Create index GRulez2 on region (r_regionkey);""")

        cur.execute("""Create table customer (c_custkey integer,c_name text, c_address text, c_nationkey integer, c_phone text, c_acctbal decimal, c_mktsegment text, c_comment text)""")
        cur.execute("""Create index Grulez3 on customer (c_custkey);""")

        cur.execute("""Create table orders (o_orderkey integer, o_custkey integer, o_orderstatus text,o_totalprice decimal, o_orderdate date, o_orderpriority text, o_shippriority text, o_comment text, extracol text)""")
        cur.execute("""Create index Grulez4 on orders (o_orderkey);""")

        cur.execute(""" Create table part(p_partkey integer, p_name text, p_mfgr text, p_brand text, p_type text, p_size integer, p_container text, p_retailprice decimal, p_comment text)""")
        cur.execute("""Create index GRulez5 on part (p_partkey);""")

        cur.execute("""Create table partsupp (ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost decimal, ps_comment text)""")
        cur.execute("""Create index GRulez6 on partsupp (ps_partkey);""")

        cur.execute("""Create table lineitem (l_orderkey integer, l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity decimal, l_extendedprice decimal, l_discount decimal, l_tax decimal,l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text)""")
        cur.execute("""Create index GRulez7 on lineitem (l_partkey);""")


        startloading2=time.time()

        cur.execute("""Copy customer from '%s' (format csv, delimiter ('|'));""" % pathcustomer)
        cur.execute("""Copy lineitem from '%s' (format csv, delimiter ('|'));""" % pathlineitem)
        cur.execute("""Copy nation from '%s' (format csv, delimiter ('|'));""" % pathnation)
        cur.execute("""Copy orders from '%s' (format csv, delimiter ('|'));""" % pathorders)
        cur.execute("""Copy part  from '%s' (format csv, delimiter ('|'));""" % pathpart)
        cur.execute("""Copy partsupp from '%s' (format csv, delimiter ('|'));""" % pathpartsupp)
        cur.execute("""Copy region from '%s' (format csv, delimiter ('|'));""" % pathregion)
        cur.execute("""Copy supplier from '%s' (format csv, delimiter ('|'));""" % pathsupplier)
        endloading2=time.time()
        difloading2=endloading2-startloading2
        print "Done loading the tables"
        print ("Time loading the tables : %s seconds" %difloading1)

        new_results = run_queries(cur, all_queries)
        difftables=difloading2-difloading1
        print ("The difference in loading the tables between those 2 commits is : %f seconds" %difftables)

        listavgold=[]
        listavgnew=[]
        combidifflist=[]
        for i in xrange(len(all_queries)):
            old_result = old_results[i]
            new_result = new_results[i]
            print "Differences for query %d are: cold run: %s, min hot run: %s, max hot run: %s, avg hot run: %s" % (
                i + 1,
                old_result['cold'] - new_result['cold'],
                old_result['min'] - new_result['min'],
                old_result['max'] - new_result['max'],
                old_result['avg'] - new_result['avg'])
	    listavgold.append(old_result['avg'])
            listavgnew.append(new_result['avg']) 
	meanold=statistics.mean(listavgold)
	meannew=statistics.mean(listavgnew)
	totmean=meannew-meanold
	mergedlist=listavgold+listavgnew
	dev=statistics.pstdev(mergedlist)
	
	if totmean>dev:
            print ">>>>>Performance regression detected!<<<<"
            os.chdir(home)

            os.chdir(Dirvar)
            os.chdir("postgres")

            currentcom=os.popen("git rev-parse HEAD").read().replace("\n", "")
            print("Current commit is : %s " %currentcom)

            os.system("git reset --soft HEAD@{1}")
            currentcom2=os.popen("git rev-parse HEAD").read().replace("\n", "")
            print("the previous commit is : %s " % currentcom2)

            os.popen("git diff %s %s >> testfile1.patch" %(currentcom, currentcom2)).read()
            
            del_diffs=[]
            listavgold1=[]
            
            with open(pathpatchfile,'r') as f1:
                patch=PatchSet(f1, 'utf-8')
                print ("Number of files in this commit : %d " %len(patch))
                print "--------------------------"
                for diffz in patch:
                    while (len(patch))!=0:
                        last_diff=patch[-1]
                        del patch[-1]
                        f2=open("output.patch","w")
                        f2.write(unicode(diffz).encode('utf8'))
                        f2.close()
                        print ("The diff that was just deleted in the output file was :\n %s" % last_diff)
                        
                        os.system("git apply --stat output.patch") 
                        if os.system("echo $?")==0:
                            print ">it compiles<"
                            os.system("make -s -j 5")
                            os.system("make install -s -j 5")
                            patch_new=run_queries(cur,all_queries)
                            for i in xrange(len(all_queries)):
                                old_res=new_results[i]
                                new_res=patch_new[i]
                                listavgold1.append(old_res['avg'])
                                       
                                print ("Differences for query %d are: cold run: %s, min hot run: %s, max hot run: %s, avg hot run: %s" % (i + 1, old_res['cold'] - new_res['cold'], old_res['min'] - new_res['min'], old_res['max'] - new_res['max'], old_res['avg'] - new_res['avg']))
                                filesumavgold=sum(listavgold1)
                                filesumavgnew=sum(listavgnew)
                                

                            if filesumavgnew < filesumavgold:
                                print "Going through each diff .."
                               
                                del_diffs.append(unicode(last_diff).encode('utf8'))
                                combidifflist.append(new_res['avg'])
                                print combidifflist
                                for i in xrange(len(combidifflist)):
                                    suspicious_file=min(combidifflist)                                        
                                    index_of_combi=combidifflist.index(suspicious_file)
                                    the_susp_file=del_diffs[index_of_combi]        
                                    
                                        
                            
                                print ("The most suspicious file is : %s " %the_susp_file)
                                with open("SuspiciousFiles.txt", "w") as f3:
                                    f3.write(unicode(currentcom))
                                    f3.write(unicode(the_susp_file).encode('utf8'))
                                    f3.close()

                            else:
                                print "No performance regression was detected in this file"
                            
                            
                                   
                                
                        else:
                            print "it does not compile"
            


            f1.close()

            
        else :
            print "No performance regression was detected between those two commits"

        os.system("rm -f testfile1.patch")
        os.system("rm -f output.patch")

#------------------------------------------------
else:
    print "Installing PostgreSQL.."
    os.chdir(home)
    os.mkdir(Dirvar)
    os.chdir(Dirvar)
    os.system("git clone https://github.com/postgres/postgres")
    os.chdir("postgres")
    PREFIX=os.path.join(home,Dirvar)
    os.system("./configure --prefix=%s" % PREFIX)
    os.system("make -j -s")
    os.system("make install -j -s")
    os.chdir(home)
    os.chdir(Dirvar)
    os.chdir("bin")

    Datadir=raw_input("Please identify the directory where the data for this database will reside: ")
    while os.path.exists(os.path.join(home,Dirvar,Datadir))==True:
        Datadir=raw_input("The directory already exists, please specify the directory where the data for this database will reside: ")
    else:
        os.system("./initdb -D %s" % Datadir)
        print "Starting PostgreSQL.."

    os.system("./pg_ctl -D %s -l logfile start" % Datadir)
    DBname=raw_input("Please specify the name of your database: ")
    os.system("./createdb %s" % DBname)
    print ("Creating the tables...")

    conn=psycopg2.connect("dbname='%s' user='%s' host='localhost'" % (DBname,Uservar))
 
    cur=conn.cursor()
    
    cur.execute("""Create table nation (n_nationkey integer, n_name text, n_regionkey integer, n_comment text)""")
    cur.execute("""Create index GRulez0 on nation (n_nationkey);""")

    cur.execute("""Create table supplier (s_suppkey integer, s_name text, s_address text, s_nationkey integer, s_phone text, s_acctbal decimal, s_comment text)""")
    cur.execute("""Create index GRulez1 on supplier (s_suppkey);""")

    cur.execute("""Create table region (r_regionkey integer, r_name text, r_comment text)""")
    cur.execute("""Create index GRulez2 on region (r_regionkey);""")

    cur.execute("""Create table customer (c_custkey integer,c_name text, c_address text, c_nationkey integer, c_phone text, c_acctbal decimal, c_mktsegment text, c_comment text)""")
    cur.execute("""Create index Grulez3 on customer (c_custkey);""")

    cur.execute("""Create table orders (o_orderkey integer, o_custkey integer, o_orderstatus text,o_totalprice decimal, o_orderdate date, o_orderpriority text, o_shippriority text, o_comment text, extracol text)""")
    cur.execute("""Create index Grulez4 on orders (o_orderkey);""")

    cur.execute(""" Create table part(p_partkey integer, p_name text, p_mfgr text, p_brand text, p_type text, p_size integer, p_container text, p_retailprice decimal, p_comment text)""")
    cur.execute("""Create index GRulez5 on part (p_partkey);""")

    cur.execute("""Create table partsupp (ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost decimal, ps_comment text)""")
    cur.execute("""Create index GRulez6 on partsupp (ps_partkey);""")

    cur.execute("""Create table lineitem (l_orderkey integer, l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity decimal, l_extendedprice decimal, l_discount decimal, l_tax decimal,l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text)""")
    cur.execute("""Create index GRulez7 on lineitem (l_partkey);""")


    startloading1=time.time()

    cur.execute("""Copy customer from '%s' (format csv, delimiter ('|'));""" % pathcustomer)

    cur.execute("""Copy lineitem from '%s' (format csv, delimiter ('|'));""" % pathlineitem)

    cur.execute("""Copy nation from '%s' (format csv, delimiter ('|'));""" % pathnation)

    cur.execute("""Copy orders from '%s' (format csv, delimiter ('|'));""" % pathorders)

    cur.execute("""Copy part  from '%s' (format csv, delimiter ('|'));""" % pathpart)

    cur.execute("""Copy partsupp from '%s' (format csv, delimiter ('|'));""" % pathpartsupp)

    cur.execute("""Copy region from '%s' (format csv, delimiter ('|'));""" % pathregion)

    cur.execute("""Copy supplier from '%s' (format csv, delimiter ('|'));""" % pathsupplier)
    endloading1=time.time()
    difloading1=endloading1-startloading1

    print "Done loading the tables"
    print ("Time loading the tables : %s seconds" %difloading1)

        
    os.chdir(pathpostgres)
    currentcom=os.popen("git rev-parse HEAD").read()
    print ("The current commit is : %s " %currentcom)
    totalcom=os.popen("git rev-list --count HEAD").read()
    print ("Number of total commits : %s" %totalcom)
    print ("Rolling back %d commits..." %NUM_COMMITS)
    os.system("git reset --hard HEAD~%d" %NUM_COMMITS)
    os.system("make -s -j 5")
    os.system("make install -s -j 5")        

    for commit in xrange(1, NUM_COMMITS + 1):
        os.chdir(pathpostgres)
        currentcom=os.popen("git rev-parse HEAD").read()
        print ("The current commit is : %s " %currentcom)
        print ("Running the queries...")
        old_results = run_queries(cur, all_queries)

        print ("Dropping the tables..")
        cur.execute("""Drop table nation;""")
        cur.execute("""Drop table supplier;""")
        cur.execute("""Drop table region;""")
        cur.execute("""Drop table customer;""")
        cur.execute("""Drop table orders;""")
        cur.execute("""Drop table part;""")
        cur.execute("""Drop table partsupp;""")
        cur.execute("""Drop table lineitem;""")

        print ("to the next commit ... !")
        os.system("git checkout HEAD@{1}")
        currentcom2=os.popen("git rev-parse HEAD").read()
        print ("Current commit is : %s " %currentcom2)

        print "Recompliling the new commit of PostGresql..."
        os.system("make -s -j 5")
        os.system("make install -s -j 5")
            
            
        print ("Reloading the tables...")
        cur.execute("""Create table nation (n_nationkey integer, n_name text, n_regionkey integer, n_comment text)""")
        cur.execute("""Create index GRulez0 on nation (n_nationkey);""")

        cur.execute("""Create table supplier (s_suppkey integer, s_name text, s_address text, s_nationkey integer, s_phone text, s_acctbal decimal, s_comment text)""")
        cur.execute("""Create index GRulez1 on supplier (s_suppkey);""")

        cur.execute("""Create table region (r_regionkey integer, r_name text, r_comment text)""")
        cur.execute("""Create index GRulez2 on region (r_regionkey);""")

        cur.execute("""Create table customer (c_custkey integer,c_name text, c_address text, c_nationkey integer, c_phone text, c_acctbal decimal, c_mktsegment text, c_comment text)""")
        cur.execute("""Create index Grulez3 on customer (c_custkey);""")

        cur.execute("""Create table orders (o_orderkey integer, o_custkey integer, o_orderstatus text,o_totalprice decimal, o_orderdate date, o_orderpriority text, o_shippriority text, o_comment text, extracol text)""")
        cur.execute("""Create index Grulez4 on orders (o_orderkey);""")

        cur.execute(""" Create table part(p_partkey integer, p_name text, p_mfgr text, p_brand text, p_type text, p_size integer, p_container text, p_retailprice decimal, p_comment text)""")
        cur.execute("""Create index GRulez5 on part (p_partkey);""")

        cur.execute("""Create table partsupp (ps_partkey integer, ps_suppkey integer, ps_availqty integer, ps_supplycost decimal, ps_comment text)""")
        cur.execute("""Create index GRulez6 on partsupp (ps_partkey);""")

        cur.execute("""Create table lineitem (l_orderkey integer, l_partkey integer, l_suppkey integer, l_linenumber integer, l_quantity decimal, l_extendedprice decimal, l_discount decimal, l_tax decimal,l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text)""")
        cur.execute("""Create index GRulez7 on lineitem (l_partkey);""")

        startloading2=time.time()

        cur.execute("""Copy customer from '%s' (format csv, delimiter ('|'));""" % pathcustomer)
        cur.execute("""Copy lineitem from '%s' (format csv, delimiter ('|'));""" % pathlineitem)
        cur.execute("""Copy nation from '%s' (format csv, delimiter ('|'));""" % pathnation)
        cur.execute("""Copy orders from '%s' (format csv, delimiter ('|'));""" % pathorders)
        cur.execute("""Copy part  from '%s' (format csv, delimiter ('|'));""" % pathpart)
        cur.execute("""Copy partsupp from '%s' (format csv, delimiter ('|'));""" % pathpartsupp)
        cur.execute("""Copy region from '%s' (format csv, delimiter ('|'));""" % pathregion)
        cur.execute("""Copy supplier from '%s' (format csv, delimiter ('|'));""" % pathsupplier)
        endloading2=time.time()
        difloading2=endloading2-startloading2
        print "Done loading the tables"
        print ("Time loading the tables : %s seconds" %difloading1)

        new_results = run_queries(cur, all_queries)
        difftables=difloading2-difloading1
        print ("The difference in loading the tables between those 2 commits is : %f seconds" %difftables)

        listavgold=[]
        listavgnew=[]
        combidifflist=[]
        for i in xrange(len(all_queries)):
            old_result = old_results[i]
            new_result = new_results[i]
            print "Differences for query %d are: cold run: %s, min hot run: %s, max hot run: %s, avg hot run: %s" % (
                i + 1,
                old_result['cold'] - new_result['cold'],
                old_result['min'] - new_result['min'],
                old_result['max'] - new_result['max'],
                old_result['avg'] - new_result['avg'])
            listavgold.append(old_result['avg'])
            listavgnew.append(new_result['avg'])        
        meanold=statistics.mean(listavgold)
	 meannew=statistics.mean(listavgnew)
	 totmean=meannew-meanold
	 mergedlist=listavgold+listavgnew
	 dev=statistics.pstdev(mergedlist)

        if totmean>dev:
            print ">>>>>Performance regression detected!<<<<"
            os.chdir(home)

            os.chdir(Dirvar)
            os.chdir("postgres")

            currentcom=os.popen("git rev-parse HEAD").read().replace("\n", "")
            print("Current commit is : %s " %currentcom)

            os.system("git reset --soft HEAD@{1}")
            currentcom2=os.popen("git rev-parse HEAD").read().replace("\n", "")
            print("the previous commit is : %s " % currentcom2)

            os.popen("git diff %s %s >> testfile1.patch" %(currentcom, currentcom2)).read()
            
            del_diffs=[]
            listavgold1=[]
            
            with open(pathpatchfile,'r') as f1:
                patch=PatchSet(f1, 'utf-8')
                print ("Number of files in this commit : %d " %len(patch))
                print "--------------------------"
                for diffz in patch:
                    while (len(patch))!=0:
                        last_diff=patch[-1]
                        del patch[-1]
                        f2=open("output.patch","w")
                        f2.write(unicode(diffz).encode('utf8'))
                        f2.close()
                        print ("The diff that was just deleted in the output file was :\n %s" % last_diff)
                        
                        os.system("git apply --stat output.patch") 
                        if os.system("echo $?")==0:
                            print ">it compiles<"
                            os.system("make -s -j 5")
                            os.system("make install -s -j 5")
                            patch_new=run_queries(cur,all_queries)
                            for i in xrange(len(all_queries)):
                                old_res=new_results[i]
                                new_res=patch_new[i]
                                listavgold1.append(old_res['avg'])
                                       
                                print ("Differences for query %d are: cold run: %s, min hot run: %s, max hot run: %s, avg hot run: %s" % (i + 1, old_res['cold'] - new_res['cold'], old_res['min'] - new_res['min'], old_res['max'] - new_res['max'], old_res['avg'] - new_res['avg']))
                                filesumavgold=sum(listavgold1)
                                filesumavgnew=sum(listavgnew)
                                

                            if filesumavgnew < filesumavgold:
                                print "Going through each diff .."
                               
                                del_diffs.append(unicode(last_diff).encode('utf8'))
                                combidifflist.append(new_res['avg'])
                                print combidifflist
                                for i in xrange(len(combidifflist)):
                                    suspicious_file=min(combidifflist)                                        
                                    index_of_combi=combidifflist.index(suspicious_file)
                                    the_susp_file=del_diffs[index_of_combi]        
                                    
                                        
                            
                                print ("The most suspicious file is : %s " %the_susp_file)
                                with open("SuspiciousFiles.txt", "w") as f3:
                                    f3.write(unicode(currentcom))
                                    f3.write(unicode(the_susp_file).encode('utf8'))
                                    f3.close()

                            else:
                                print "No performance regression was detected in this file"
                            
                            
                                   
                                
                        else:
                            print "it does not compile"
            


            f1.close()

            
        else :
            print "No performance regression was detected between those two commits"

        os.system("rm -f testfile1.patch")
        os.system("rm -f output.patch")


