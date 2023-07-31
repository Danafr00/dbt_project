{{ config(materialized='table') }}

WITH list_of_date AS(  
  SELECT
  *
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', '2023-08-14', INTERVAL 1 DAY)) AS date_list
),

prediction AS (
    SELECT company_name,
        prediction,
        high,
        low,
        forecast_date,
        "Prediction" AS data_type
    FROM `latihan-345909.stocks.prediction_table`
),

latest_date AS (
    SELECT MAX(jakarta_data_date) AS max_date
    FROM `latihan-345909.stocks.stocks_table`
),

stocks_data AS(
  SELECT *,
    "Actual" AS data_type
  FROM `latihan-345909.stocks.stocks_table`
  WHERE DATE(jakarta_data_date) = (SELECT max_date FROM latest_date)
),

combined_table AS (
SELECT
    COALESCE(date, forecast_date) AS date,
    close,
    volume,
    stochrsi,
    macd,
    macds
    mfi,
    a.company_name,
    b.prediction,
    b.low lower_bound,
    b.high upper_bound,
FROM `latihan-345909.stocks.stocks_table` a
    FULL OUTER JOIN prediction b 
    ON a.date = b.forecast_date
        AND a.company_name = b.company_name
)

SELECT date_list,
  b.*,
  coalesce(
    last_value(close ignore nulls) over (order by date_list),
    first_value(close ignore nulls) over (order by date_list RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ) AS close_adjustment,
  coalesce(
    last_value(volume ignore nulls) over (order by date_list),
    first_value(volume ignore nulls) over (order by date_list RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ) AS volume_adjustment,
    coalesce(
    last_value(stochrsi ignore nulls) over (order by date_list),
    first_value(stochrsi ignore nulls) over (order by date_list RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ) AS stochrsi_adjustment,
  coalesce(
    last_value(macd ignore nulls) over (order by date_list),
    first_value(macd ignore nulls) over (order by date_list RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ) AS macd_adjustment,
  coalesce(
    last_value(mfi ignore nulls) over (order by date_list),
    first_value(mfi ignore nulls) over (order by date_list RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ) AS mfi_adjustment,
   coalesce(
    last_value(company_name ignore nulls) over (order by date_list),
    first_value(company_name ignore nulls) over (order by date_list RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    ) AS company_name_adjustment    
FROM list_of_date a 
LEFT JOIN combined_table b
  ON a.date_list = b.date