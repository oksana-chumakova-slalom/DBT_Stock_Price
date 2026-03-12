SELECT
    company_id,
    company_name,
    cik,
    ein,
    primary_ticker AS ticker,
    primary_exchange_name
FROM {{ source('public_data_free', 'company_index') }}
-- Filter for 'Corporate' and ensure there is a ticker to join on
WHERE entity_level = 'Corporate'
  AND primary_ticker IS NOT NULL
  AND cik IS NOT NULL