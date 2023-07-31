{{ config(materialized='table') }}

WITH prediction AS (
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
)

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
    b.low louwer_bound,
    b.high upper_bound,
FROM `latihan-345909.stocks.stocks_table` a
    FULL OUTER JOIN prediction b 
    ON a.date = b.forecast_date
        AND a.company_name = b.company_name
