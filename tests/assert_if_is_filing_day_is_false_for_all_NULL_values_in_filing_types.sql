SELECT
    trading_date
FROM {{ ref('fct_stock_market_pulse') }}
WHERE filing_types is not NULL AND is_filing_day = false