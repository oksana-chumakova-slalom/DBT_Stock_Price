SELECT
cik,
filed_date,
LISTAGG(form_type, ', ') WITHIN GROUP (ORDER BY form_type) AS filing_types,
COUNT(*) AS total_filings_that_day
FROM {{ source('public_data_free', 'sec_report_index') }}
GROUP BY cik, filed_date