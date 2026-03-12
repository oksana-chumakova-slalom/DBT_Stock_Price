WITH market_history AS (
    SELECT 
        ticker,
        company_name,
        cik,
        trading_date,
        close_price,
        -- Calculate the previous day's price to find the return
        LAG(close_price) OVER (PARTITION BY ticker ORDER BY trading_date) AS prev_close
    FROM {{ ref('int_market_activity_unified') }}
),
inflation AS (
    -- Monthly PCE/CPI data
    SELECT * FROM {{ ref('stg_inflation') }}
),
filings AS (
    -- Now unique by CIK and Date
    SELECT * FROM {{ ref('stg_sec_filings') }}
)

SELECT
    m.company_name,
    m.ticker,
    m.trading_date,
    m.close_price,
    -- Nominal Return %
    ((m.close_price - m.prev_close) / m.prev_close) * 100 AS nominal_return_pct,
    -- Join Inflation (aligning daily stock date to monthly inflation date)
    i.inflation_index_value,
    -- New Logic: Bring in the list of filings and create the Boolean flag
    f.filing_types,
    CASE WHEN f.filing_types IS NOT NULL THEN TRUE ELSE FALSE END AS is_filing_day
FROM market_history m
LEFT JOIN inflation i 
    ON DATE_TRUNC('month', m.trading_date) = DATE_TRUNC('month', i.cpi_date)
LEFT JOIN filings f 
    ON m.cik = f.cik 
    AND m.trading_date = f.filed_date
WHERE m.prev_close IS NOT NULL
