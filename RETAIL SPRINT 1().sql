----warehouse--
CREATE WAREHOUSE IF NOT EXISTS TOP_wH
WITH 
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Main warehouse for MedicoRe Data Pipeline';

USE WAREHOUSE TOP_WH;

-- Create Database
CREATE DATABASE IF NOT EXISTS RETAIL_DB;

-- Use it
USE DATABASE RETAIL_DB;

---SCHEMAS
select * from raw.customers_raw
-- Core Layers
CREATE SCHEMA IF NOT EXISTS STAGE;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS VALIDATED;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS STREAMS; 
CREATE SCHEMA IF NOT EXISTS TASKS;

----stageing--------
use schema stage;

CREATE OR REPLACE FILE FORMAT RETAIL_DB.STAGE.CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null', '')
EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE RETAIL_DB.STAGE.LANDING_STAGE
FILE_FORMAT = RETAIL_DB.STAGE.CSV_FORMAT
COMMENT = 'Landing zone for all incoming CSV files';

-----------creating raw tables
---using raw
USE SCHEMA RAW;

CREATE OR REPLACE TABLE customers_raw (
    customer_id STRING,
    name STRING,
    city STRING,
    signup_date STRING
);

CREATE OR REPLACE TABLE products_raw (
    product_id STRING,
    product_name STRING,
    category STRING,
    price STRING
);



CREATE OR REPLACE TABLE orders_raw (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    total_amount STRING
);

CREATE OR REPLACE TABLE order_items_raw (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity STRING
);

CREATE OR REPLACE TABLE user_activity_raw (
    activity_id STRING,
    customer_id STRING,
    activity_type STRING,
    activity_time STRING
);

---------streams------------
use schema streams;

CREATE OR REPLACE STREAM customers_raw_stream
ON TABLE RETAIL_DB.RAW.customers_raw;

CREATE OR REPLACE STREAM products_raw_stream
ON TABLE RETAIL_DB.RAW.products_raw;

CREATE OR REPLACE STREAM orders_raw_stream
ON TABLE RETAIL_DB.RAW.orders_raw;

CREATE OR REPLACE STREAM order_items_raw_stream
ON TABLE RETAIL_DB.RAW.order_items_raw;

CREATE OR REPLACE STREAM user_activity_raw_stream
ON TABLE RETAIL_DB.RAW.user_activity_raw;

-------loading stage to raw------------
COPY INTO RETAIL_DB.RAW.customers_raw FROM @RETAIL_DB.STAGE.LANDING_STAGE/customers_500.csv FILE_FORMAT = RETAIL_DB.STAGE.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RETAIL_DB.RAW.products_raw FROM @RETAIL_DB.STAGE.LANDING_STAGE/products_500.csv FILE_FORMAT = RETAIL_DB.STAGE.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RETAIL_DB.RAW.orders_raw FROM @RETAIL_DB.STAGE.LANDING_STAGE/orders_500.csv FILE_FORMAT = RETAIL_DB.STAGE.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RETAIL_DB.RAW.order_items_raw FROM @RETAIL_DB.STAGE.LANDING_STAGE/order_items_500.csv FILE_FORMAT = RETAIL_DB.STAGE.CSV_FORMAT ON_ERROR = 'CONTINUE';
COPY INTO RETAIL_DB.RAW.user_activity_raw FROM @RETAIL_DB.STAGE.LANDING_STAGE/user_activity_500.csv FILE_FORMAT = RETAIL_DB.STAGE.CSV_FORMAT ON_ERROR = 'CONTINUE';


select * from streams.customers_raw_stream;


-------------------------------------------------------------------------------------------------------------------- Data Cleaning ----------------------------------------------------------------------------------------------------------------------------------------------------VALID
CREATE OR REPLACE TABLE customers_Valid(
    customer_id STRING,
    name STRING,
    city STRING,
    signup_date DATE,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE products_Valid (
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2),

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



CREATE OR REPLACE TABLE orders_Valid (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    total_amount NUMBER(10,2),

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE order_items_Valid (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity NUMBER,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE TABLE user_activity_Valid(
    activity_id STRING,
    customer_id STRING,
    activity_type STRING,
    activity_time TIMESTAMP,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

------------------ INVALID SCHEMA ----------
CREATE SCHEMA IF NOT EXISTS INVALID;
CREATE OR REPLACE TABLE customers_INValid(
    customer_id STRING,
    name STRING,
    city STRING,
    signup_date DATE,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message STRING
);

CREATE OR REPLACE TABLE products_INValid (
    product_id STRING,
    product_name STRING,
    category STRING,
    price STRING,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message STRING
);



CREATE OR REPLACE TABLE orders_INValid (
    order_id STRING,
    customer_id STRING,
    order_date STRING,
    total_amount STRING,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message STRING
);

CREATE OR REPLACE TABLE order_items_INValid (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity STRING,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message STRING
);

CREATE OR REPLACE TABLE user_activity_INValid(
    activity_id STRING,
    customer_id STRING,
    activity_type STRING,
    activity_time STRING,

    -- Metadata
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message STRING
);


------------------------- TASKS ---------------------------------------------------------
---------------------------------------------------------------------------------------

CREATE OR REPLACE TASK RETAIL_DB.TASKS.CUSTOMERS_VALIDATION_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.CUSTOMERS_RAW_STREAM')
AS
BEGIN

CREATE OR REPLACE TEMP TABLE TMP_CUSTOMERS AS
SELECT
    customer_id,
    name,
    city,
    signup_date,
    METADATA$ACTION,
    CASE
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'CUSTOMER_ID NULL'
        WHEN name IS NULL OR TRIM(name) = '' THEN 'NAME NULL'
        WHEN TRY_TO_DATE(signup_date) IS NULL THEN 'INVALID DATE'
        ELSE 'VALID'
    END AS RECORD_STATUS
FROM RETAIL_DB.STREAMS.CUSTOMERS_RAW_STREAM;

MERGE INTO RETAIL_DB.VALIDATED.CUSTOMERS_VALID TGT
USING (
    SELECT
        TRIM(customer_id) AS customer_id,
        INITCAP(TRIM(name)) AS name,
        INITCAP(TRIM(city)) AS city,
        TRY_TO_DATE(signup_date) AS signup_date,
        METADATA$ACTION
    FROM TMP_CUSTOMERS
    WHERE RECORD_STATUS = 'VALID'
) SRC
ON TGT.customer_id = SRC.customer_id

WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE

WHEN MATCHED THEN UPDATE SET
    name = SRC.name,
    city = SRC.city,
    signup_date = SRC.signup_date

WHEN NOT MATCHED THEN INSERT (
    customer_id, name, city, signup_date
)
VALUES (
    SRC.customer_id, SRC.name, SRC.city, SRC.signup_date
);

INSERT INTO RETAIL_DB.INVALID.CUSTOMERS_INVALID
(
    customer_id, name, city, signup_date, load_timestamp, error_message
)
SELECT
    customer_id,
    name,
    city,
    TRY_TO_DATE(signup_date),
    CURRENT_TIMESTAMP(),
    RECORD_STATUS
FROM TMP_CUSTOMERS
WHERE RECORD_STATUS != 'VALID';

END;



ALTER TASK RETAIL_DB.TASKS.CUSTOMERS_VALIDATION_TASK RESUME;

EXECUTE TASK RETAIL_DB.TASKS.CUSTOMERS_VALIDATION_TASK;

SELECT * FROM VALIDATED.CUSTOMERS_VALID;
SELECT * FROM INVALID.CUSTOMERS_INVALID;

select * from streams.customers_raw_stream;


------------------------- PRODUCTS TASK ---------------------------------------------------------

CREATE OR REPLACE TASK RETAIL_DB.TASKS.PRODUCTS_VALIDATION_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.PRODUCTS_RAW_STREAM')
AS
BEGIN

CREATE OR REPLACE TEMP TABLE TMP_PRODUCTS AS
SELECT
    product_id, product_name, category, price, METADATA$ACTION,
    CASE
        WHEN product_id IS NULL OR TRIM(product_id) = '' THEN 'PRODUCT_ID NULL'
        WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 'PRODUCT_NAME NULL'
        WHEN category IS NULL OR TRIM(category) = '' THEN 'CATEGORY NULL'
        WHEN TRY_TO_NUMBER(price, 10, 2) IS NULL THEN 'INVALID PRICE'
        WHEN TRY_TO_NUMBER(price, 10, 2) < 0 THEN 'NEGATIVE PRICE'
        ELSE 'VALID'
    END AS RECORD_STATUS
FROM RETAIL_DB.STREAMS.PRODUCTS_RAW_STREAM;

MERGE INTO RETAIL_DB.VALIDATED.PRODUCTS_VALID TGT
USING (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        INITCAP(TRIM(category)) AS category,
        TRY_TO_NUMBER(price, 10, 2) AS price,
        METADATA$ACTION
    FROM TMP_PRODUCTS
    WHERE RECORD_STATUS = 'VALID'
) SRC
ON TGT.product_id = SRC.product_id
WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    product_name = SRC.product_name,
    category = SRC.category,
    price = SRC.price
WHEN NOT MATCHED THEN INSERT (product_id, product_name, category, price)
VALUES (SRC.product_id, SRC.product_name, SRC.category, SRC.price);

INSERT INTO RETAIL_DB.INVALID.PRODUCTS_INVALID
(product_id, product_name, category, price, load_timestamp, error_message)
SELECT product_id, product_name, category, price, CURRENT_TIMESTAMP(), RECORD_STATUS
FROM TMP_PRODUCTS
WHERE RECORD_STATUS != 'VALID';

END;

ALTER TASK RETAIL_DB.TASKS.PRODUCTS_VALIDATION_TASK RESUME;
EXECUTE TASK RETAIL_DB.TASKS.PRODUCTS_VALIDATION_TASK;


------------------------- ORDERS TASK ---------------------------------------------------------

CREATE OR REPLACE TASK RETAIL_DB.TASKS.ORDERS_VALIDATION_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.ORDERS_RAW_STREAM')
AS
BEGIN

CREATE OR REPLACE TEMP TABLE TMP_ORDERS AS
SELECT
    order_id, customer_id, order_date, total_amount, METADATA$ACTION,
    CASE
        WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'ORDER_ID NULL'
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'CUSTOMER_ID NULL'
        WHEN TRY_TO_DATE(order_date) IS NULL THEN 'INVALID DATE'
        WHEN TRY_TO_NUMBER(total_amount, 10, 2) IS NULL THEN 'INVALID AMOUNT'
        WHEN TRY_TO_NUMBER(total_amount, 10, 2) < 0 THEN 'NEGATIVE AMOUNT'
        ELSE 'VALID'
    END AS RECORD_STATUS
FROM RETAIL_DB.STREAMS.ORDERS_RAW_STREAM;

MERGE INTO RETAIL_DB.VALIDATED.ORDERS_VALID TGT
USING (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        TRY_TO_DATE(order_date) AS order_date,
        TRY_TO_NUMBER(total_amount, 10, 2) AS total_amount,
        METADATA$ACTION
    FROM TMP_ORDERS
    WHERE RECORD_STATUS = 'VALID'
) SRC
ON TGT.order_id = SRC.order_id
WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    customer_id = SRC.customer_id,
    order_date = SRC.order_date,
    total_amount = SRC.total_amount
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_date, total_amount)
VALUES (SRC.order_id, SRC.customer_id, SRC.order_date, SRC.total_amount);

INSERT INTO RETAIL_DB.INVALID.ORDERS_INVALID
(order_id, customer_id, order_date, total_amount, load_timestamp, error_message)
SELECT order_id, customer_id, order_date, total_amount, CURRENT_TIMESTAMP(), RECORD_STATUS
FROM TMP_ORDERS
WHERE RECORD_STATUS != 'VALID';

END;

ALTER TASK RETAIL_DB.TASKS.ORDERS_VALIDATION_TASK RESUME;
EXECUTE TASK RETAIL_DB.TASKS.ORDERS_VALIDATION_TASK;


------------------------- ORDER ITEMS TASK ---------------------------------------------------------

CREATE OR REPLACE TASK RETAIL_DB.TASKS.ORDER_ITEMS_VALIDATION_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.ORDER_ITEMS_RAW_STREAM')
AS
BEGIN

CREATE OR REPLACE TEMP TABLE TMP_ORDER_ITEMS AS
SELECT
    order_item_id, order_id, product_id, quantity, METADATA$ACTION,
    CASE
        WHEN order_item_id IS NULL OR TRIM(order_item_id) = '' THEN 'ORDER_ITEM_ID NULL'
        WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'ORDER_ID NULL'
        WHEN product_id IS NULL OR TRIM(product_id) = '' THEN 'PRODUCT_ID NULL'
        WHEN TRY_TO_NUMBER(quantity) IS NULL THEN 'INVALID QUANTITY'
        WHEN TRY_TO_NUMBER(quantity) <= 0 THEN 'NON-POSITIVE QUANTITY'
        ELSE 'VALID'
    END AS RECORD_STATUS
FROM RETAIL_DB.STREAMS.ORDER_ITEMS_RAW_STREAM;

MERGE INTO RETAIL_DB.VALIDATED.ORDER_ITEMS_VALID TGT
USING (
    SELECT
        TRIM(order_item_id) AS order_item_id,
        TRIM(order_id) AS order_id,
        TRIM(product_id) AS product_id,
        TRY_TO_NUMBER(quantity) AS quantity,
        METADATA$ACTION
    FROM TMP_ORDER_ITEMS
    WHERE RECORD_STATUS = 'VALID'
) SRC
ON TGT.order_item_id = SRC.order_item_id
WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    order_id = SRC.order_id,
    product_id = SRC.product_id,
    quantity = SRC.quantity
WHEN NOT MATCHED THEN INSERT (order_item_id, order_id, product_id, quantity)
VALUES (SRC.order_item_id, SRC.order_id, SRC.product_id, SRC.quantity);

INSERT INTO RETAIL_DB.INVALID.ORDER_ITEMS_INVALID
(order_item_id, order_id, product_id, quantity, load_timestamp, error_message)
SELECT order_item_id, order_id, product_id, quantity, CURRENT_TIMESTAMP(), RECORD_STATUS
FROM TMP_ORDER_ITEMS
WHERE RECORD_STATUS != 'VALID';

END;

ALTER TASK RETAIL_DB.TASKS.ORDER_ITEMS_VALIDATION_TASK RESUME;
EXECUTE TASK RETAIL_DB.TASKS.ORDER_ITEMS_VALIDATION_TASK;


------------------------- USER ACTIVITY TASK ---------------------------------------------------------

CREATE OR REPLACE TASK RETAIL_DB.TASKS.USER_ACTIVITY_VALIDATION_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.USER_ACTIVITY_RAW_STREAM')
AS
BEGIN

CREATE OR REPLACE TEMP TABLE TMP_USER_ACTIVITY AS
SELECT
    activity_id, customer_id, activity_type, activity_time, METADATA$ACTION,
    CASE
        WHEN activity_id IS NULL OR TRIM(activity_id) = '' THEN 'ACTIVITY_ID NULL'
        WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN 'CUSTOMER_ID NULL'
        WHEN activity_type IS NULL OR TRIM(activity_type) = '' THEN 'ACTIVITY_TYPE NULL'
        WHEN TRY_TO_TIMESTAMP(activity_time) IS NULL THEN 'INVALID TIMESTAMP'
        ELSE 'VALID'
    END AS RECORD_STATUS
FROM RETAIL_DB.STREAMS.USER_ACTIVITY_RAW_STREAM;

MERGE INTO RETAIL_DB.VALIDATED.USER_ACTIVITY_VALID TGT
USING (
    SELECT
        TRIM(activity_id) AS activity_id,
        TRIM(customer_id) AS customer_id,
        LOWER(TRIM(activity_type)) AS activity_type,
        TRY_TO_TIMESTAMP(activity_time) AS activity_time,
        METADATA$ACTION
    FROM TMP_USER_ACTIVITY
    WHERE RECORD_STATUS = 'VALID'
) SRC
ON TGT.activity_id = SRC.activity_id
WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    customer_id = SRC.customer_id,
    activity_type = SRC.activity_type,
    activity_time = SRC.activity_time
WHEN NOT MATCHED THEN INSERT (activity_id, customer_id, activity_type, activity_time)
VALUES (SRC.activity_id, SRC.customer_id, SRC.activity_type, SRC.activity_time);

INSERT INTO RETAIL_DB.INVALID.USER_ACTIVITY_INVALID
(activity_id, customer_id, activity_type, activity_time, load_timestamp, error_message)
SELECT activity_id, customer_id, activity_type, activity_time, CURRENT_TIMESTAMP(), RECORD_STATUS
FROM TMP_USER_ACTIVITY
WHERE RECORD_STATUS != 'VALID';

END;

ALTER TASK RETAIL_DB.TASKS.USER_ACTIVITY_VALIDATION_TASK RESUME;
EXECUTE TASK RETAIL_DB.TASKS.USER_ACTIVITY_VALIDATION_TASK;


--------------------------------------CURATED LAYER---------------------------
-- Streams on validated tables (stored in STREAMS schema)

CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.CUSTOMERS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.CUSTOMERS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.PRODUCTS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.PRODUCTS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.ORDERS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.ORDERS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.ORDER_ITEMS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.ORDER_ITEMS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.USER_ACTIVITY_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.USER_ACTIVITY_VALID;

-- Dimension Tables (SCD Type 2 for Customers)
CREATE OR REPLACE TABLE RETAIL_DB.CURATED.DIM_CUSTOMERS (
    customer_key NUMBER AUTOINCREMENT,
    customer_id STRING,
    name STRING,
    city STRING,
    signup_date DATE,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    CONSTRAINT pk_dim_customers PRIMARY KEY (customer_key)
);

CREATE OR REPLACE TABLE RETAIL_DB.CURATED.DIM_PRODUCTS (
    product_key NUMBER AUTOINCREMENT,
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_dim_products PRIMARY KEY (product_key)
);

CREATE OR REPLACE TABLE RETAIL_DB.CURATED.DIM_DATE (
    date_key NUMBER AUTOINCREMENT,
    full_date DATE,
    year NUMBER,
    month NUMBER,
    day NUMBER,
    quarter NUMBER,
    day_of_week STRING,
    is_weekend BOOLEAN,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);

CREATE OR REPLACE TABLE RETAIL_DB.CURATED.DIM_USER_ACTIVITY (
    activity_key NUMBER AUTOINCREMENT,
    activity_id STRING,
    customer_key NUMBER,
    activity_type STRING,
    activity_time TIMESTAMP,
    CONSTRAINT pk_dim_user_activity PRIMARY KEY (activity_key)
);

-- Fact Table
CREATE OR REPLACE TABLE RETAIL_DB.CURATED.FACT_SALES (
    fact_order_key STRING,
    ORDER_ID STRING,
    CUSTOMER_KEY NUMBER,
    PRODUCT_KEY NUMBER,
    order_date DATE,
    order_item_id STRING,
    quantity NUMBER,
    total_amount NUMBER(10,2),
    record_type STRING,
    activity_id STRING,
    activity_time TIMESTAMP,
    activity_type STRING,
    gross_amount NUMBER(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_fact_sales PRIMARY KEY (fact_order_key)
);

------------------------- CURATED TASKS ---------------------------------------------------------

CREATE OR REPLACE TASK RETAIL_DB.TASKS.CURATED_DIM_CUSTOMERS_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.CUSTOMERS_VALID_STREAM')
AS
BEGIN

    CREATE OR REPLACE TEMP TABLE TMP_CUST_CHANGES AS
    SELECT 
        SRC.customer_id, SRC.name, SRC.city, SRC.signup_date,
        SRC.METADATA$ACTION, SRC.METADATA$ISUPDATE,
        CASE
            WHEN SRC.METADATA$ACTION = 'INSERT' AND SRC.METADATA$ISUPDATE = TRUE THEN 'UPDATE'
            WHEN SRC.METADATA$ACTION = 'INSERT' AND SRC.METADATA$ISUPDATE = FALSE THEN 'INSERT'
            WHEN SRC.METADATA$ACTION = 'DELETE' AND SRC.METADATA$ISUPDATE = TRUE THEN 'SKIP'
            WHEN SRC.METADATA$ACTION = 'DELETE' AND SRC.METADATA$ISUPDATE = FALSE THEN 'DELETE'
        END AS change_type
    FROM RETAIL_DB.STREAMS.CUSTOMERS_VALID_STREAM SRC;

    UPDATE RETAIL_DB.CURATED.DIM_CUSTOMERS TGT
    SET effective_to = CURRENT_TIMESTAMP(), is_current = FALSE
    WHERE TGT.is_current = TRUE
      AND TGT.customer_id IN (
          SELECT customer_id FROM TMP_CUST_CHANGES WHERE change_type IN ('UPDATE', 'DELETE')
      );

    INSERT INTO RETAIL_DB.CURATED.DIM_CUSTOMERS (customer_id, name, city, signup_date, effective_from, effective_to, is_current)
    SELECT customer_id, name, city, signup_date, CURRENT_TIMESTAMP(), '9999-12-31'::TIMESTAMP, TRUE
    FROM TMP_CUST_CHANGES
    WHERE change_type IN ('INSERT', 'UPDATE');

END;

CREATE OR REPLACE TASK RETAIL_DB.TASKS.CURATED_DIM_PRODUCTS_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.PRODUCTS_VALID_STREAM')
AS
BEGIN
MERGE INTO RETAIL_DB.CURATED.DIM_PRODUCTS TGT
USING (
    SELECT product_id, product_name, category, price, METADATA$ACTION
    FROM RETAIL_DB.STREAMS.PRODUCTS_VALID_STREAM
) SRC
ON TGT.product_id = SRC.product_id
WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET
    product_name = SRC.product_name, category = SRC.category, price = SRC.price, updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (product_id, product_name, category, price)
VALUES (SRC.product_id, SRC.product_name, SRC.category, SRC.price);
END;

CREATE OR REPLACE TASK RETAIL_DB.TASKS.CURATED_FACT_SALES_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.ORDERS_VALID_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.ORDER_ITEMS_VALID_STREAM')
AS
BEGIN
    MERGE INTO RETAIL_DB.CURATED.FACT_SALES TGT
    USING (
        SELECT 'ORD-' || o.order_id || '-' || oi.order_item_id AS fact_order_key,
            'ORDER' AS record_type,
            o.order_id AS ORDER_ID,
            oi.order_item_id,
            dc.customer_key AS CUSTOMER_KEY,
            dp.product_key AS PRODUCT_KEY,
            o.order_date,
            NULL AS activity_id,
            NULL AS activity_time,
            NULL AS activity_type,
            oi.quantity,
            o.total_amount,
            oi.quantity * dp.price AS gross_amount,
            oi.METADATA$ACTION
        FROM RETAIL_DB.STREAMS.ORDER_ITEMS_VALID_STREAM oi
        JOIN RETAIL_DB.VALIDATED.ORDERS_VALID o ON oi.order_id = o.order_id
        LEFT JOIN RETAIL_DB.CURATED.DIM_CUSTOMERS dc ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
        LEFT JOIN RETAIL_DB.CURATED.DIM_PRODUCTS dp ON oi.product_id = dp.product_id
    ) SRC
    ON TGT.fact_order_key = SRC.fact_order_key
    WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE
    WHEN MATCHED THEN UPDATE SET
        ORDER_ID = SRC.ORDER_ID, CUSTOMER_KEY = SRC.CUSTOMER_KEY, PRODUCT_KEY = SRC.PRODUCT_KEY,
        order_date = SRC.order_date, quantity = SRC.quantity, total_amount = SRC.total_amount,
        gross_amount = SRC.gross_amount, updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (fact_order_key, record_type, ORDER_ID, order_item_id, CUSTOMER_KEY, PRODUCT_KEY, order_date, quantity, total_amount, gross_amount)
    VALUES (SRC.fact_order_key, SRC.record_type, SRC.ORDER_ID, SRC.order_item_id, SRC.CUSTOMER_KEY, SRC.PRODUCT_KEY, SRC.order_date, SRC.quantity, SRC.total_amount, SRC.gross_amount);

    LET dummy1 RESULTSET := (SELECT 1 FROM RETAIL_DB.STREAMS.ORDERS_VALID_STREAM LIMIT 0);
END;

CREATE OR REPLACE TASK RETAIL_DB.TASKS.CURATED_DIM_DATE_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.ORDERS_VALID_STREAM')
AS
BEGIN
    MERGE INTO RETAIL_DB.CURATED.DIM_DATE TGT
    USING (
        SELECT DISTINCT order_date AS full_date,
            YEAR(order_date) AS year,
            MONTH(order_date) AS month,
            DAY(order_date) AS day,
            QUARTER(order_date) AS quarter,
            DAYNAME(order_date) AS day_of_week,
            CASE WHEN DAYOFWEEK(order_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
        FROM RETAIL_DB.STREAMS.ORDERS_VALID_STREAM
        WHERE order_date IS NOT NULL AND METADATA$ACTION = 'INSERT'
    ) SRC
    ON TGT.full_date = SRC.full_date
    WHEN NOT MATCHED THEN INSERT (full_date, year, month, day, quarter, day_of_week, is_weekend)
    VALUES (SRC.full_date, SRC.year, SRC.month, SRC.day, SRC.quarter, SRC.day_of_week, SRC.is_weekend);
END;

CREATE OR REPLACE TASK RETAIL_DB.TASKS.CURATED_DIM_USER_ACTIVITY_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.USER_ACTIVITY_VALID_STREAM')
AS
BEGIN
    MERGE INTO RETAIL_DB.CURATED.DIM_USER_ACTIVITY TGT
    USING (
        SELECT ua.activity_id,
            dc.customer_key,
            ua.activity_type,
            ua.activity_time,
            ua.METADATA$ACTION
        FROM RETAIL_DB.STREAMS.USER_ACTIVITY_VALID_STREAM ua
        LEFT JOIN RETAIL_DB.CURATED.DIM_CUSTOMERS dc ON ua.customer_id = dc.customer_id AND dc.is_current = TRUE
    ) SRC
    ON TGT.activity_id = SRC.activity_id
    WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN DELETE
    WHEN MATCHED THEN UPDATE SET
        customer_key = SRC.customer_key, activity_type = SRC.activity_type,
        activity_time = SRC.activity_time
    WHEN NOT MATCHED THEN INSERT (activity_id, customer_key, activity_type, activity_time)
    VALUES (SRC.activity_id, SRC.customer_key, SRC.activity_type, SRC.activity_time);
END;

ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_CUSTOMERS_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_PRODUCTS_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_FACT_SALES_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_DATE_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_USER_ACTIVITY_TASK RESUME;

EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_CUSTOMERS_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_PRODUCTS_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_FACT_SALES_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_DATE_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_USER_ACTIVITY_TASK;

------------analytics layer task------------
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.FACT_SALES_ANALYTICS_STREAM ON TABLE RETAIL_DB.CURATED.FACT_SALES;

ALTER TASK RETAIL_DB.TASKS.ANALYTICS_REFRESH_TASK RESUME;
EXECUTE TASK RETAIL_DB.TASKS.ANALYTICS_REFRESH_TASK;

-- =============================================
-- ANALYTICS LAYER (5 KPIs)
-- =============================================

-- KPI 1: SALES PERFORMANCE SUMMARY
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.SALES_PERFORMANCE_SUMMARY AS
SELECT
    d.year,
    d.month,
    d.quarter,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders,
    COUNT(DISTINCT f.CUSTOMER_KEY) AS unique_customers,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(SUM(f.gross_amount), 2) AS total_gross_amount,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    ROUND(SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.CUSTOMER_KEY), 0), 2) AS revenue_per_customer
FROM RETAIL_DB.CURATED.FACT_SALES f
JOIN RETAIL_DB.CURATED.DIM_DATE d ON f.order_date = d.full_date
WHERE f.record_type = 'ORDER'
GROUP BY d.year, d.month, d.quarter
ORDER BY d.year, d.month;
SELECT * FROM ANALYTICS.CUSTOMER_LIFETIME_VALUE;
-- KPI 2: CUSTOMER LIFETIME VALUE
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.CUSTOMER_LIFETIME_VALUE AS
SELECT
    c.customer_key,
    c.customer_id,
    c.name,
    c.city,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS days_since_signup,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders,
    SUM(f.quantity) AS total_items_purchased,
    ROUND(SUM(f.total_amount), 2) AS total_spend,
    ROUND(SUM(f.gross_amount), 2) AS total_gross_amount,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    MIN(f.order_date) AS first_order_date,
    MAX(f.order_date) AS last_order_date,
    DATEDIFF('day', MIN(f.order_date), MAX(f.order_date)) AS customer_lifetime_days,
    ROUND(SUM(f.total_amount) / NULLIF(DATEDIFF('month', MIN(f.order_date), MAX(f.order_date)) + 1, 0), 2) AS avg_monthly_spend,
    CASE
        WHEN SUM(f.total_amount) >= 150000 AND COUNT(DISTINCT f.ORDER_ID) >= 3 THEN 'PLATINUM'
        WHEN SUM(f.total_amount) >= 80000 AND COUNT(DISTINCT f.ORDER_ID) >= 2 THEN 'GOLD'
        WHEN SUM(f.total_amount) >= 30000 THEN 'SILVER'
        ELSE 'BRONZE'
    END AS customer_tier
FROM RETAIL_DB.CURATED.DIM_CUSTOMERS c
LEFT JOIN RETAIL_DB.CURATED.FACT_SALES f ON c.customer_key = f.CUSTOMER_KEY AND f.record_type = 'ORDER'
WHERE c.is_current = TRUE
GROUP BY c.customer_key, c.customer_id, c.name, c.city, c.signup_date
ORDER BY total_spend DESC;
SELECT * FROM ANALYTICS.CUSTOMER_ACTIVITY_INSIGHTS;
-- KPI 3: PRODUCT PERFORMANCE ANALYTICS
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.PRODUCT_PERFORMANCE_ANALYTICS AS
SELECT
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.price AS unit_price,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders,
    COUNT(DISTINCT f.CUSTOMER_KEY) AS unique_buyers,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.gross_amount), 2) AS total_gross_revenue,
    ROUND(SUM(f.total_amount), 2) AS total_order_revenue,
    ROUND(AVG(f.quantity), 2) AS avg_quantity_per_order,
    RANK() OVER (ORDER BY SUM(f.gross_amount) DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.gross_amount) DESC) AS category_revenue_rank,
    ROUND(SUM(f.gross_amount) / NULLIF(SUM(SUM(f.gross_amount)) OVER (), 0) * 100, 2) AS revenue_contribution_pct
FROM RETAIL_DB.CURATED.DIM_PRODUCTS p
LEFT JOIN RETAIL_DB.CURATED.FACT_SALES f ON p.product_key = f.PRODUCT_KEY AND f.record_type = 'ORDER'
GROUP BY p.product_key, p.product_id, p.product_name, p.category, p.price
ORDER BY total_gross_revenue DESC;
SELECT * FROM ANALYTICS.PRODUCT_PERFORMANCE_ANALYTICS;
-- KPI 4: CUSTOMER ACTIVITY INSIGHTS
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.CUSTOMER_ACTIVITY_INSIGHTS AS
SELECT
    c.customer_key,
    c.customer_id,
    c.name,
    c.city,
    COUNT(*) AS total_activities,
    SUM(CASE WHEN ua.activity_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN ua.activity_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ua.activity_type = 'login' THEN 1 ELSE 0 END) AS login_count,
    SUM(CASE WHEN ua.activity_type = 'logout' THEN 1 ELSE 0 END) AS logout_count,
    MIN(ua.activity_time) AS first_activity_time,
    MAX(ua.activity_time) AS last_activity_time,
    DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) AS days_since_last_activity,
    ROUND(SUM(CASE WHEN ua.activity_type = 'purchase' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2) AS purchase_conversion_rate,
    CASE
        WHEN DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) <= 30 THEN 'HIGHLY ACTIVE'
        WHEN DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) <= 90 THEN 'ACTIVE'
        WHEN DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) <= 180 THEN 'AT RISK'
        ELSE 'INACTIVE'
    END AS engagement_status
FROM RETAIL_DB.CURATED.DIM_CUSTOMERS c
JOIN RETAIL_DB.CURATED.DIM_USER_ACTIVITY ua ON c.customer_key = ua.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_key, c.customer_id, c.name, c.city
ORDER BY total_activities DESC;

-- KPI 5: MONTHLY REVENUE TRENDS
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.MONTHLY_REVENUE_TRENDS AS
WITH monthly_data AS (
    SELECT
        d.year,
        d.month,
        d.quarter,
        COUNT(DISTINCT f.ORDER_ID) AS total_orders,
        COUNT(DISTINCT f.CUSTOMER_KEY) AS unique_customers,
        SUM(f.quantity) AS total_units_sold,
        ROUND(SUM(f.total_amount), 2) AS total_revenue,
        ROUND(SUM(f.gross_amount), 2) AS total_gross_revenue
    FROM RETAIL_DB.CURATED.FACT_SALES f
    JOIN RETAIL_DB.CURATED.DIM_DATE d ON f.order_date = d.full_date
    WHERE f.record_type = 'ORDER'
    GROUP BY d.year, d.month, d.quarter
)
SELECT
    year,
    month,
    quarter,
    total_orders,
    unique_customers,
    total_units_sold,
    total_revenue,
    total_gross_revenue,
    LAG(total_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY year, month)) /
        NULLIF(LAG(total_revenue) OVER (ORDER BY year, month), 0) * 100, 2
    ) AS revenue_growth_pct,
    ROUND(SUM(total_revenue) OVER (ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) / 
        LEAST(ROW_NUMBER() OVER (ORDER BY year, month), 3), 2
    ) AS rolling_3m_avg_revenue,
    SUM(total_revenue) OVER (PARTITION BY year ORDER BY month) AS ytd_revenue
FROM monthly_data
ORDER BY year, month;


------------------------- ANALYTICS LAYER STREAM & TASK ---------------------------------------------------------

CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.FACT_SALES_ANALYTICS_STREAM ON TABLE RETAIL_DB.CURATED.FACT_SALES;

CREATE OR REPLACE TASK RETAIL_DB.TASKS.ANALYTICS_REFRESH_TASK
WAREHOUSE = TOP_WH
SCHEDULE = '2 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('RETAIL_DB.STREAMS.FACT_SALES_ANALYTICS_STREAM')
AS
BEGIN

-- KPI 1: SALES PERFORMANCE SUMMARY
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.SALES_PERFORMANCE_SUMMARY AS
SELECT
    d.year,
    d.month,
    d.quarter,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders,
    COUNT(DISTINCT f.CUSTOMER_KEY) AS unique_customers,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(SUM(f.gross_amount), 2) AS total_gross_amount,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    ROUND(SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.CUSTOMER_KEY), 0), 2) AS revenue_per_customer
FROM RETAIL_DB.CURATED.FACT_SALES f
JOIN RETAIL_DB.CURATED.DIM_DATE d ON f.order_date = d.full_date
WHERE f.record_type = 'ORDER'
GROUP BY d.year, d.month, d.quarter
ORDER BY d.year, d.month;

-- KPI 2: CUSTOMER LIFETIME VALUE
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.CUSTOMER_LIFETIME_VALUE AS
SELECT
    c.customer_key,
    c.customer_id,
    c.name,
    c.city,
    c.signup_date,
    DATEDIFF('day', c.signup_date, CURRENT_DATE()) AS days_since_signup,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders,
    SUM(f.quantity) AS total_items_purchased,
    ROUND(SUM(f.total_amount), 2) AS total_spend,
    ROUND(SUM(f.gross_amount), 2) AS total_gross_amount,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
    MIN(f.order_date) AS first_order_date,
    MAX(f.order_date) AS last_order_date,
    DATEDIFF('day', MIN(f.order_date), MAX(f.order_date)) AS customer_lifetime_days,
    ROUND(SUM(f.total_amount) / NULLIF(DATEDIFF('month', MIN(f.order_date), MAX(f.order_date)) + 1, 0), 2) AS avg_monthly_spend,
    CASE
        WHEN SUM(f.total_amount) >= 150000 AND COUNT(DISTINCT f.ORDER_ID) >= 3 THEN 'PLATINUM'
        WHEN SUM(f.total_amount) >= 80000 AND COUNT(DISTINCT f.ORDER_ID) >= 2 THEN 'GOLD'
        WHEN SUM(f.total_amount) >= 30000 THEN 'SILVER'
        ELSE 'BRONZE'
    END AS customer_tier
FROM RETAIL_DB.CURATED.DIM_CUSTOMERS c
LEFT JOIN RETAIL_DB.CURATED.FACT_SALES f ON c.customer_key = f.CUSTOMER_KEY AND f.record_type = 'ORDER'
WHERE c.is_current = TRUE
GROUP BY c.customer_key, c.customer_id, c.name, c.city, c.signup_date
ORDER BY total_spend DESC;

-- KPI 3: PRODUCT PERFORMANCE ANALYTICS
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.PRODUCT_PERFORMANCE_ANALYTICS AS
SELECT
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.price AS unit_price,
    COUNT(DISTINCT f.ORDER_ID) AS total_orders,
    COUNT(DISTINCT f.CUSTOMER_KEY) AS unique_buyers,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.gross_amount), 2) AS total_gross_revenue,
    ROUND(SUM(f.total_amount), 2) AS total_order_revenue,
    ROUND(AVG(f.quantity), 2) AS avg_quantity_per_order,
    RANK() OVER (ORDER BY SUM(f.gross_amount) DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.gross_amount) DESC) AS category_revenue_rank,
    ROUND(SUM(f.gross_amount) / NULLIF(SUM(SUM(f.gross_amount)) OVER (), 0) * 100, 2) AS revenue_contribution_pct
FROM RETAIL_DB.CURATED.DIM_PRODUCTS p
LEFT JOIN RETAIL_DB.CURATED.FACT_SALES f ON p.product_key = f.PRODUCT_KEY AND f.record_type = 'ORDER'
GROUP BY p.product_key, p.product_id, p.product_name, p.category, p.price
ORDER BY total_gross_revenue DESC;

-- KPI 4: CUSTOMER ACTIVITY INSIGHTS
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.CUSTOMER_ACTIVITY_INSIGHTS AS
SELECT
    c.customer_key,
    c.customer_id,
    c.name,
    c.city,
    COUNT(*) AS total_activities,
    SUM(CASE WHEN ua.activity_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
    SUM(CASE WHEN ua.activity_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN ua.activity_type = 'login' THEN 1 ELSE 0 END) AS login_count,
    SUM(CASE WHEN ua.activity_type = 'logout' THEN 1 ELSE 0 END) AS logout_count,
    MIN(ua.activity_time) AS first_activity_time,
    MAX(ua.activity_time) AS last_activity_time,
    DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) AS days_since_last_activity,
    ROUND(SUM(CASE WHEN ua.activity_type = 'purchase' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0) * 100, 2) AS purchase_conversion_rate,
    CASE
        WHEN DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) <= 30 THEN 'HIGHLY ACTIVE'
        WHEN DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) <= 90 THEN 'ACTIVE'
        WHEN DATEDIFF('day', MAX(ua.activity_time), CURRENT_TIMESTAMP()) <= 180 THEN 'AT RISK'
        ELSE 'INACTIVE'
    END AS engagement_status
FROM RETAIL_DB.CURATED.DIM_CUSTOMERS c
JOIN RETAIL_DB.CURATED.DIM_USER_ACTIVITY ua ON c.customer_key = ua.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_key, c.customer_id, c.name, c.city
ORDER BY total_activities DESC;

-- KPI 5: MONTHLY REVENUE TRENDS
CREATE OR REPLACE TABLE RETAIL_DB.ANALYTICS.MONTHLY_REVENUE_TRENDS AS
WITH monthly_data AS (
    SELECT
        d.year,
        d.month,
        d.quarter,
        COUNT(DISTINCT f.ORDER_ID) AS total_orders,
        COUNT(DISTINCT f.CUSTOMER_KEY) AS unique_customers,
        SUM(f.quantity) AS total_units_sold,
        ROUND(SUM(f.total_amount), 2) AS total_revenue,
        ROUND(SUM(f.gross_amount), 2) AS total_gross_revenue
    FROM RETAIL_DB.CURATED.FACT_SALES f
    JOIN RETAIL_DB.CURATED.DIM_DATE d ON f.order_date = d.full_date
    WHERE f.record_type = 'ORDER'
    GROUP BY d.year, d.month, d.quarter
)
SELECT
    year,
    month,
    quarter,
    total_orders,
    unique_customers,
    total_units_sold,
    total_revenue,
    total_gross_revenue,
    LAG(total_revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (ORDER BY year, month)) /
        NULLIF(LAG(total_revenue) OVER (ORDER BY year, month), 0) * 100, 2
    ) AS revenue_growth_pct,
    ROUND(SUM(total_revenue) OVER (ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) / 
        LEAST(ROW_NUMBER() OVER (ORDER BY year, month), 3), 2
    ) AS rolling_3m_avg_revenue,
    SUM(total_revenue) OVER (PARTITION BY year ORDER BY month) AS ytd_revenue
FROM monthly_data
ORDER BY year, month;

LET dummy RESULTSET := (SELECT * FROM RETAIL_DB.STREAMS.FACT_SALES_ANALYTICS_STREAM LIMIT 0);

END;


----------------------------------------------------------TESTING---------------------------
SELECT * FROM STREAMS.CUSTOMERS_RAW_STREAM
--------------validation task--------------
-- Resume all validation tasks
ALTER TASK RETAIL_DB.TASKS.CUSTOMERS_VALIDATION_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.PRODUCTS_VALIDATION_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.ORDERS_VALIDATION_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.ORDER_ITEMS_VALIDATION_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.USER_ACTIVITY_VALIDATION_TASK RESUME;

-- Execute immediately
EXECUTE TASK RETAIL_DB.TASKS.CUSTOMERS_VALIDATION_TASK;
EXECUTE TASK RETAIL_DB.TASKS.PRODUCTS_VALIDATION_TASK;
EXECUTE TASK RETAIL_DB.TASKS.ORDERS_VALIDATION_TASK;
EXECUTE TASK RETAIL_DB.TASKS.ORDER_ITEMS_VALIDATION_TASK;
EXECUTE TASK RETAIL_DB.TASKS.USER_ACTIVITY_VALIDATION_TASK;

------------curated layer task------------
------------curated layer task------------

-- Resume all curated tasks
ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_CUSTOMERS_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_PRODUCTS_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_FACT_SALES_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_DATE_TASK RESUME;
ALTER TASK RETAIL_DB.TASKS.CURATED_DIM_USER_ACTIVITY_TASK RESUME;

-- Execute immediately
EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_CUSTOMERS_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_PRODUCTS_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_FACT_SALES_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_DATE_TASK;
EXECUTE TASK RETAIL_DB.TASKS.CURATED_DIM_USER_ACTIVITY_TASK;


-----------analytical layer----------------------------
ALTER TASK RETAIL_DB.TASKS.ANALYTICS_REFRESH_TASK RESUME;
EXECUTE TASK RETAIL_DB.TASKS.ANALYTICS_REFRESH_TASK;



-------------------deleting all the data----------------------------------
-- RAW
TRUNCATE TABLE RETAIL_DB.RAW.CUSTOMERS_RAW;
TRUNCATE TABLE RETAIL_DB.RAW.PRODUCTS_RAW;
TRUNCATE TABLE RETAIL_DB.RAW.ORDERS_RAW;
TRUNCATE TABLE RETAIL_DB.RAW.ORDER_ITEMS_RAW;
TRUNCATE TABLE RETAIL_DB.RAW.USER_ACTIVITY_RAW;

-- VALIDATED
TRUNCATE TABLE RETAIL_DB.VALIDATED.CUSTOMERS_VALID;
TRUNCATE TABLE RETAIL_DB.VALIDATED.PRODUCTS_VALID;
TRUNCATE TABLE RETAIL_DB.VALIDATED.ORDERS_VALID;
TRUNCATE TABLE RETAIL_DB.VALIDATED.ORDER_ITEMS_VALID;
TRUNCATE TABLE RETAIL_DB.VALIDATED.USER_ACTIVITY_VALID;

-- INVALID
TRUNCATE TABLE RETAIL_DB.INVALID.CUSTOMERS_INVALID;
TRUNCATE TABLE RETAIL_DB.INVALID.PRODUCTS_INVALID;
TRUNCATE TABLE RETAIL_DB.INVALID.ORDERS_INVALID;
TRUNCATE TABLE RETAIL_DB.INVALID.ORDER_ITEMS_INVALID;
TRUNCATE TABLE RETAIL_DB.INVALID.USER_ACTIVITY_INVALID;

-- CURATED
TRUNCATE TABLE RETAIL_DB.CURATED.DIM_CUSTOMERS;
TRUNCATE TABLE RETAIL_DB.CURATED.DIM_PRODUCTS;
TRUNCATE TABLE RETAIL_DB.CURATED.DIM_DATE;
TRUNCATE TABLE RETAIL_DB.CURATED.DIM_USER_ACTIVITY;
TRUNCATE TABLE RETAIL_DB.CURATED.FACT_SALES;

-- ANALYTICS
TRUNCATE TABLE RETAIL_DB.ANALYTICS.SALES_PERFORMANCE_SUMMARY;
TRUNCATE TABLE RETAIL_DB.ANALYTICS.CUSTOMER_LIFETIME_VALUE;
TRUNCATE TABLE RETAIL_DB.ANALYTICS.PRODUCT_PERFORMANCE_ANALYTICS;
TRUNCATE TABLE RETAIL_DB.ANALYTICS.CUSTOMER_ACTIVITY_INSIGHTS;
TRUNCATE TABLE RETAIL_DB.ANALYTICS.MONTHLY_REVENUE_TRENDS;


------------------------turncating streams-----------------
-- RAW streams
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.CUSTOMERS_RAW_STREAM ON TABLE RETAIL_DB.RAW.CUSTOMERS_RAW;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.PRODUCTS_RAW_STREAM ON TABLE RETAIL_DB.RAW.PRODUCTS_RAW;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.ORDERS_RAW_STREAM ON TABLE RETAIL_DB.RAW.ORDERS_RAW;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.ORDER_ITEMS_RAW_STREAM ON TABLE RETAIL_DB.RAW.ORDER_ITEMS_RAW;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.USER_ACTIVITY_RAW_STREAM ON TABLE RETAIL_DB.RAW.USER_ACTIVITY_RAW;

-- VALIDATED streams
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.CUSTOMERS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.CUSTOMERS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.PRODUCTS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.PRODUCTS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.ORDERS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.ORDERS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.ORDER_ITEMS_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.ORDER_ITEMS_VALID;
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.USER_ACTIVITY_VALID_STREAM ON TABLE RETAIL_DB.VALIDATED.USER_ACTIVITY_VALID;

-- ANALYTICS stream
CREATE OR REPLACE STREAM RETAIL_DB.STREAMS.FACT_SALES_ANALYTICS_STREAM ON TABLE RETAIL_DB.CURATED.FACT_SALES;


---remove stage
REMOVE @RETAIL_DB.STAGE.LANDING_STAGE;
SELECT * FROM RETAIL_DB.ANALYTICS.SALES_PERFORMANCE_SUMMARY;
SELECT * FROM RETAIL_DB.ANALYTICS.CUSTOMER_LIFETIME_VALUE;
SELECT * FROM RETAIL_DB.ANALYTICS.PRODUCT_PERFORMANCE_ANALYTICS;
SELECT * FROM RETAIL_DB.ANALYTICS.CUSTOMER_ACTIVITY_INSIGHTS;
SELECT * FROM RETAIL_DB.ANALYTICS.MONTHLY_REVENUE_TRENDS;
SELECT * FROM RETAIL_DB.CURATED.DIM_PRODUCTS;
SELECT * FROM RETAIL_DB.CURATED.DIM_CUSTOMERS;
SELECT * FROM RETAIL_DB.CURATED.DIM_DATE;
SELECT * FROM RETAIL_DB.CURATED.DIM_USER_ACTIVITY;
SELECT * FROM RETAIL_DB.CURATED.FACT_SALES;





















