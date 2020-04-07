create or replace table {{ var.value.database }}.public.meta_order_detail as
with
    date_range
    as
    (
        select
            min(o_orderdate) as start_dt,
            max(o_orderdate) as end_dt
        from {{ var.value.database }}.public.order_detail
    ),
    all_dates
    as
    (
        select
            d_date
        from {{ var.value.database }}.public.calendar
            join date_range 
        where d_date >= start_dt and d_date <= end_dt
    ),
    orders_by_date
    as
    (
        select
            o_orderdate as dt,
            count(*) as line_items,
            count(distinct(order_key)) as orders
        from {{ var.value.database }}.public.order_detail
        group by 1
    )
select
    dt,
    line_items,
    orders
from orders_by_date o join all_dates a
    on o.dt=a.d_date;