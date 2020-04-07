create or replace table {{ var.value.database }}.public.customer as
with
    today
    as
    (
        select max(o_orderdate) as dt
        from {{ var.value.database }}.public.order_detail
    ),
    lifetime
    as
    (
        select
            o_custkey,
            min(o_orderdate) as first_purchase_date,
            max(o_orderdate) as last_purchase_date,
            sum(o_totalprice) as ltv,
            avg(o_totalprice) as aov,
            count(o_orderdate) as order_count_lt
        from {{ var.value.sample_database }}.tpch_sf100.orders
        group by 1
    ),
    last12
    as
    (
        select
            o_custkey,
            sum(o_totalprice) as customer_value_12mo,
            avg(o_totalprice) as aov_12mo,
            count(o_orderdate) as order_count_12mo
        from {{ var.value.sample_database }}.tpch_sf100.orders
            join today 
        where o_orderdate >= dateadd(year, -1, today.dt)
        group by 1
    )
select
    c.c_name,
    c.c_address,
    c.c_nationkey as nation_key,
    n.n_name,
    n.n_regionkey as region_key,
    r.r_name,
    c.c_phone,
    c.c_acctbal,
    c.c_mktsegment,
    l.ltv as lifetime_value,
    l.first_purchase_date,
    l.last_purchase_date,
    l.order_count_lt,
    l.aov as average_order_value_lt,
    last12.customer_value_12mo,
    last12.aov_12mo as average_order_value_12mo,
    last12.order_count_12mo
from {{ var.value.sample_database }}.tpch_sf100.customer c
    left join lifetime l on c.c_custkey=l.o_custkey
    left join last12 on c.c_custkey=last12.o_custkey
    left join {{ var.value.sample_database }}.tpch_sf100.nation n on c.c_nationkey=n.n_nationkey
    left join {{ var.value.sample_database }}.tpch_sf100.region r on n.n_regionkey=r.r_regionkey;