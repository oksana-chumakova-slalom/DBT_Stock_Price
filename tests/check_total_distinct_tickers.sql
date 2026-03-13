with int_tickers as (
    select count(distinct ticker) as total_dist_ticker 
    from {{ ref('int_market_activity_unified' )}}
),
fact_tickers as (
    select count(distinct ticker) as total_dist_ticker 
    from {{ ref('fct_stock_market_pulse') }}
)

select 
    int_tickers.total_dist_ticker as expected,
    fact_tickers.total_dist_ticker as actual
from int_tickers
join fact_tickers 
    -- This query returns a row (and fails) if the totals do not match
    on int_tickers.total_dist_ticker != fact_tickers.total_dist_ticker