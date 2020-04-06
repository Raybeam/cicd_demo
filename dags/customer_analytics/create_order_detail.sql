create table {{ var.value.database }}.public.order_detail as
select
   o.o_orderkey as order_key,
   o.o_custkey as customer_key,
   o.o_orderstatus,
   o.o_totalprice,
   o.o_orderdate,
   o.o_orderpriority,
   o.o_clerk,
   o.o_shippriority,
   l.l_linenumber,
   l.l_quantity,
   l.l_extendedprice,
   l.l_discount,
   l.l_tax,
   l.l_returnflag,
   l.l_linestatus,
   l.l_shipdate,
   l.l_commitdate,
   l.l_receiptdate,
   l.l_shipinstruct,
   l.l_shipmode,
   p.p_partkey as part_key,
   p.p_name,
   p.p_mfgr,
   p.p_brand,
   p.p_type,
   p.p_size,
   p.p_container,
   p.p_retailprice,
   l.l_suppkey as supplier_key,
   s.s_name,
   s.s_address,
   s.s_phone,
   s.s_acctbal,
   s.s_nationkey as nation_key,
   n.n_name,
   n.n_regionkey as region_key,
   r.r_name
from snowflake_sample_data.tpch_sf100.lineitem l
   left join {{ var.value.sample_database }}.tpch_sf100.orders o on l.l_orderkey=o.o_orderkey
   left join {{ var.value.sample_database }}.tpch_sf100.part p on l.l_partkey=p.p_partkey
   left join {{ var.value.sample_database }}.tpch_sf100.supplier s on l.l_suppkey=s.s_suppkey
   left join {{ var.value.sample_database }}.tpch_sf100.nation n on s.s_nationkey=n.n_nationkey
   left join {{ var.value.sample_database }}.tpch_sf100.region r on n.n_regionkey=r.r_regionkey;