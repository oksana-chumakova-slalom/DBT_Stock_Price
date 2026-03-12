SELECT
    c.company_name,
    c.ticker,
    c.cik,
    p.trading_date,
    p.close_price
FROM {{ ref('stg_stock_prices') }} p
INNER JOIN {{ ref('stg_company_index') }} c ON p.ticker = c.ticker
                                            AND p.primary_exchange_name = c.primary_exchange_name