SELECT
    date AS cpi_date, -- We'll keep the name 'cpi' for consistency in your project
    value AS inflation_index_value
FROM {{ source('public_data_free', 'financial_economic_indicators_timeseries') }}
WHERE variable = 'BEA_NIPA_PCE_2.8.4_DPCCRG_M_SA'