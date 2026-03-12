SELECT
    ticker,
    asset_class,
    variable_name,
    date AS trading_date,
    value AS close_price,
    primary_exchange_name
FROM {{ source('stock_price_timeseries', 'stock_price_timeseries') }}
-- We filter for 'Post-Market Close' to get a single daily price record per ticker
WHERE variable_name = 'Post-Market Close'