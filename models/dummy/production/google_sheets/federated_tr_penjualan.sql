{{ config(database="side_project") }}

select * from {{ source('google_sheets', 'tr_penjualan') }}