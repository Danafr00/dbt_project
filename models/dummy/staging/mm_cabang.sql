{{ config(materialized='table') }}

select *
from {{ ref('ms_cabang') }}