create or replace table {{ var.value.database }}.public.calendar as
select *
from {{ var.value.sample_database }}.tpcds_sf10tcl.date_dim;