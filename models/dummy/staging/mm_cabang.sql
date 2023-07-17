{{ config(materialized='table', database="side_project") }}

select *
from {{ ref('ms_cabang') }}