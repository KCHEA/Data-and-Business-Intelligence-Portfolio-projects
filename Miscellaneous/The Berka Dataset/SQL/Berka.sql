/*
  For data cleaning we will proceed through 4 steps:
  1. Check for duplicates
  2. Check for null values or empty strings
  3. Check for appropriate value ranges 
  4. validate business rules (e.g. if ID is only 3 numbers we will filter value by that rule)

  After this pre-processing, a temporary table will be created that will by data wrangling (JOIN,CREATE,AGGREGATE).
  Only then will these data be used for analysis in python.
*/

/*
  Date is in the format YYMMDD and will need to be change to the format DDMMYY
  Frequency meaning:
  "POPLATEK MESICNE" - Monthly Issuance
  "POPLATEK TYDNE" - Weekly Issuance
  "POPLATEK PO OBRATU" - Issuance After Transaction
*/

-- Cleaning account table --
SELECT * FROM berka.account;

CREATE OR REPLACE TABLE berka.cleaned_account AS
SELECT
  account_id,
  district_id,
  frequency,
  SAFE.PARSE_DATE('%y%m%d',LPAD(CAST(`date` AS STRING),6,'0')) AS `date`
FROM berka.account;

SELECT DISTINCT frequency FROM berka.account;

UPDATE berka.cleaned_account
SET frequency = 
CASE frequency
  WHEN 'POPLATEK MESICNE' THEN 'Monthly Issuance'
  WHEN 'POPLATEK TYDNE' THEN 'Weekly Issuance'
  WHEN 'POPLATEK PO OBRATU' THEN 'Issuance After Transaction'
  ELSE frequency
END
WHERE frequency IN (
    'POPLATEK MESICNE',
    'POPLATEK TYDNE',
    'POPLATEK PO OBRATU'
);

SELECT * FROM berka.cleaned_account;

-- There is no duplicate record in account table
SELECT
  account_id,
  COUNT(*) AS num_row
FROM berka.cleaned_account
GROUP BY account_id
HAVING num_row > 1;

-- There are no NULL values in account table
SELECT *
FROM berka.cleaned_account
WHERE account_id IS NULL OR
      district_id IS NULL OR
      frequency IS NULL OR
      `date` IS NULL;


-- Cleaning card table --

SELECT * FROM berka.card;

-- There are no duplicate record in card table
SELECT 
  card_id,
  COUNT(*) AS num_row
FROM berka.card
GROUP BY card_id
HAVING num_row > 1;

SELECT DISTINCT type FROM berka.card;
SELECT DISTINCT issued FROM berka.card;


-- There is only one distinct value for hh:mm:ss which is 00:00:00, we will not use it
WITH time_added AS (
SELECT 
  card_id,
  disp_id,
  type,
  SAFE.PARSE_TIMESTAMP('%y%m%d %H:%M:%S', CAST(issued AS STRING)) AS `date`
FROM berka.card
)
SELECT DISTINCT EXTRACT(TIME from `date`) AS time_distinct_value
FROM time_added;

CREATE OR REPLACE TABLE berka.cleaned_card AS 
SELECT 
  card_id,
  disp_id,
  type,
  SAFE.PARSE_DATE('%y%m%d', LPAD(CAST(issued AS STRING),6,'0')) AS `date`
FROM berka.card;

SELECT * FROM berka.cleaned_card;

-- There are no NULL values in card table
SELECT *
FROM berka.cleaned_card
WHERE card_id IS NULL OR
      disp_id IS NULL OR
      type IS NULL OR
      `date` IS NULL;

-- Cleaning client table

SELECT * FROM berka.client;

-- There are no duplicate records in client table
SELECT 
  client_id,
  COUNT(*) AS num_row
FROM berka.client
GROUP BY client_id
HAVING num_row > 1;

/*
The value is in the form: YYMMDD (for men)
The value is in the form: YYMM+50DD (for women)
Where YYMMDD is the date of birth

T
*/
  
/*
The dd or day part of the 'birth_number' column ranges from 1 - 31 
This mean  month is actually the one determining gender
The value is in the form: YY50+MMDD (For Women)
*/
WITH day_cte AS (
  SELECT
    SUBSTR(CAST(birth_number AS STRING),5,2) AS day
  FROM berka.client
)
SELECT 
  MIN(SAFE_CAST(day AS INT)) AS min_birth_day,
  MAX(SAFE_CAST(day AS INT)) AS max_birth_day
FROM day_cte;


-- CTE for formatting date and gender
CREATE OR REPLACE TABLE berka.cleaned_client AS
WITH split_cte AS (
  SELECT
    client_id,
    SUBSTR(CAST(birth_number AS STRING),1,2) AS year,
    SUBSTR(CAST(birth_number AS STRING),3,2) AS month,
    SUBSTR(CAST(birth_number AS STRING),5,2) AS day,
    district_id
  FROM berka.client
),
add_gender AS (
  SELECT
    client_id,
    CONCAT('19',year) AS year,
    CASE
      WHEN CAST(month AS INT) > 12 THEN 
        CASE 
          WHEN CAST(MONTH AS INT) - 50 < 10 THEN CONCAT('0',CAST(CAST(month AS INT) - 50 AS STRING))
          ELSE CAST(CAST(month AS INT) - 50 AS STRING)
        END
      ELSE month
    END AS month,
    day,
    CASE
      WHEN CAST(month AS INT) > 12 THEN 'female'
      ELSE 'male'
    END AS gender,
    district_id
  FROM split_cte
),
combined_cte AS (
  SELECT
    client_id,
    CONCAT(year,month,day) AS birth_number,
    gender,
    district_id
  FROM add_gender
),
final AS (
  SELECT 
    client_id,
    district_id,
    SAFE.PARSE_DATE('%Y%m%d',birth_number) AS birthday,
    gender
  FROM combined_cte
)
SELECT *
FROM final
ORDER BY client_id;

-- Cleaning disp table --

SELECT * FROM berka.disp;

SELECT DISTINCT type FROM berka.disp;

-- There are no duplicate records in disp table
SELECT 
  disp_id,
  COUNT(*) AS num_row
FROM berka.disp
GROUP BY disp_id
HAVING num_row > 1;

-- There are no NULL values in disp table
SELECT *
FROM berka.disp
WHERE disp_id IS NULL OR
      client_id IS NULL OR
      account_id IS NULL OR
      type IS NULL;

-- Cleaning district table --

SELECT * FROM berka.district;

SELECT DISTINCT A2 FROM berka.district;
SELECT DISTINCT A3 FROM berka.district;

-- There is a '?' value in the 'A12' column, we will turn it into NULL
SELECT DISTINCT A12 FROM berka.district;

-- There is a '?' value in the 'A15' column, we will turn it into NULL
SELECT DISTINCT A15 FROM berka.district;

CREATE OR REPLACE TABLE berka.cleaned_district AS 
SELECT
  A1 AS district_id,
  A2 AS district_name,
  A3 AS region,
  A4 AS number_of_inhabitants,
  A5,
  A6,
  A7,
  A8,
  A9 AS number_of_cities,
  A10 AS ratio_of_urban_inhabitants,
  A11 AS average_salary,
  CASE 
   WHEN A12 = '?' THEN NULL
   ELSE A12
  END AS A12,
  A13,
  A14,
  CASE 
    WHEN A15 = '?' THEN NULL
    ELSE A15  
  END AS A15,
  A16
FROM berka.district
ORDER BY district_id;

SELECT * FROM berka.cleaned_district;

-- There are no duplicate records in district table
SELECT 
  district_id,
  COUNT(*) AS num_row 
FROM berka.cleaned_district
GROUP BY district_id
HAVING num_row > 1;


-- Cleaning loan table

SELECT * FROM berka.loan;

-- There are no duplicate records in loan table
SELECT 
  loan_id,
  COUNT(*) AS num_row
FROM berka.loan
GROUP BY loan_id
HAVING num_row > 1;

SELECT DISTINCT status FROM berka.loan;

CREATE OR REPLACE TABLE berka.cleaned_loan AS
SELECT
  loan_id,
  account_id,
  SAFE.PARSE_DATE('%Y%m%d',CONCAT('19',CAST(`date` AS STRING))) AS `date`,
  amount,
  duration,
  payments,
  CASE 
    WHEN status = 'A' THEN 'Contract finished, no problems'
    WHEN status = 'B' THEN 'Contract finished, loan not paid'
    WHEN status = 'C' THEN 'Running contract, OK thus-far'
    WHEN status = 'D' THEN 'Running contract, client in debt'
  END AS status
FROM berka.loan
ORDER BY loan_id;

SELECT * FROM berka.cleaned_loan;

-- There are no NULL values in loan table
SELECT * 
FROM berka.cleaned_loan
WHERE loan_id IS NULL OR
      account_id IS NULL OR
      `date` IS NULL OR
      amount IS NULL OR
      duration IS NULL OR
      payments IS NULL OR
      status IS NULL;

-- Cleaning order table --

SELECT * FROM berka.order;

SELECT * FROM berka.order WHERE k_symbol = ' ';

-- There are no duplicate records in order table
SELECT 
  order_id,
  COUNT(*) AS num_row
FROM berka.order
GROUP BY order_id
HAVING num_row > 1;


SELECT DISTINCT bank_to FROM berka.order;

-- There is an empty string ' ' value in 'k_symbol' column, we will turn it into NULL
SELECT DISTINCT k_symbol from berka.order;


CREATE OR REPLACE TABLE berka.cleaned_order AS
SELECT
  order_id,
  account_id,
  bank_to,
  account_to,
  amount,
  CASE
    WHEN k_symbol = ' ' THEN NULL
    WHEN k_symbol = 'LEASING' THEN 'Leasing Payment'
    WHEN k_symbol = 'POJISTNE' THEN 'Insurance Payment'
    WHEN k_symbol = 'SIPO' THEN 'Household Payment'
    WHEN k_symbol = 'UVER' THEN 'Loan Payment'
  END AS k_symbol
FROM berka.order;


-- Cleaning trans table --

SELECT * FROM berka.trans;

-- There is no duplicate records in trans table 
SELECT 
  trans_id,
  COUNT(*) AS num_row
FROM berka.trans
GROUP BY trans_id
HAVING num_row > 1;


SELECT DISTINCT type FROM berka.trans;

-- There are empty string values for 'operation' column, we will turn it into NULL
SELECT DISTINCT operation FROM berka.trans;

-- There are two empty string values: '' and ' ' for 'k_symbol' column, we will turn it into NULL
SELECT DISTINCT k_symbol FROM berka.trans;

-- There are empty string values for 'bank' column, we will turn it into NULL
SELECT DISTINCT bank FROM berka.trans;


CREATE OR REPLACE TABLE berka.cleaned_trans AS
SELECT
  trans_id,
  account_id,
  SAFE.PARSE_DATE('%Y%m%d',CONCAT('19',CAST(`date` AS STRING))) AS `date`,
  CASE
    WHEN type = 'PRIJEM' THEN 'Credit'
    WHEN type = 'VYDAJ' THEN 'Debit'
    WHEN type = 'VYBER' THEN 'Cash withdrawal'
  END AS type,
  CASE
    WHEN operation = 'PREVOD NA UCET' THEN 'Remittance to Another Bank'
    WHEN operation = 'VKLAD' THEN 'Credit in Cash'
    WHEN operation = 'VYBER' THEN 'Cash withdrawal'
    WHEN operation = 'VYBER KARTOU' THEN 'Credit Card withdrawal'
    WHEN operation = 'PREVOD Z UCTU' THEN 'Collection from Another Bank'
    ELSE NULL
  END AS operation,
  amount,
  balance,
  CASE
    WHEN k_symbol = 'UROK' THEN 'Interest Credited'
    WHEN k_symbol = 'UVER' THEN 'Loan Payment'
    WHEN k_symbol = 'SLUZBY' THEN 'Payment of Statement'
    WHEN k_symbol = 'SANKC. UROK' THEN 'Sanction Interest if Negative Balance'
    WHEN k_symbol = 'SIPO' THEN 'Household Payment'
    WHEN k_symbol = 'POJISTNE' THEN 'Insurance Payment'
    WHEN k_symbol = 'DUCHOD' THEN 'Old-age Pension Payment'
    ELSE NULL
  END AS k_symbol,
  CASE
    WHEN bank = '' THEN NULL
    ELSE bank
  END AS bank,
  account
FROM berka.trans
ORDER BY trans_id;


SELECT * FROM berka.cleaned_trans;

















