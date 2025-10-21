--ALTER TABLE fact_sales DROP CONSTRAINT FK_fact_customer;  -- use actual FK name
--TRUNCATE TABLE dim_customer;
---- Re-add FK afterwards
--ALTER TABLE fact_sales
--ADD CONSTRAINT FK_fact_customer FOREIGN KEY (customer_key)
--REFERENCES dim_customer(customer_key) ON DELETE CASCADE ON UPDATE CASCADE;





---SCD TYPE 1 dim_product
MERGE dim_product AS target
USING staging_product AS src
ON target.product_id = src.product_id
WHEN MATCHED THEN
    UPDATE SET 
        product_name = src.product_name,
        category = src.category,
        brand = src.brand
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, brand)
    VALUES (src.product_id, src.product_name, src.category, src.brand);

select * from dim_product;

--scd type 2 dim_customers
--below code works but will not work when fact table includes foreigh key condstraint, mergwe within insert doesnt work

--insert into dim_customer (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
--select id, name, city, status, getdate(), null, 1
--from (
--MERGE dim_customer AS target
--USING staging_customer AS src
--ON target.customer_id = src.customer_id
--   AND target.is_current = 1
--WHEN MATCHED AND (target.city <> src.city OR target.loyalty_status <> src.loyalty_status)
--   THEN UPDATE SET end_date = GETDATE(), is_current = 0

--WHEN NOT MATCHED BY TARGET
--   THEN INSERT (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
--        VALUES (src.customer_id, src.name, src.city, src.loyalty_status, GETDATE(), NULL, 1)
--output
--    $action as action,
--    src.customer_id as id, 
--    src.name as name, 
--    src.city as city, 
--    src.loyalty_status as status
--) as c
--where action = 'update';

--error - The target table 'dim_customer' of the INSERT statement cannot be on either side of a (primary key, foreign key) relationship 
--when the FROM clause contains a nested INSERT, UPDATE, DELETE, or MERGE statement. Found reference constraint 'FK_fact_customer'.


--scd 2 dim_customers
--select * into #tmp_tb
--from (
--MERGE dim_customer AS target
--USING staging_customer AS src
--ON target.customer_id = src.customer_id
--   AND target.is_current = 1
--WHEN MATCHED AND (target.city <> src.city OR target.loyalty_status <> src.loyalty_status)
--   THEN UPDATE SET end_date = GETDATE(), is_current = 0

--WHEN NOT MATCHED BY TARGET
--   THEN INSERT (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
--        VALUES (src.customer_id, src.name, src.city, src.loyalty_status, GETDATE(), NULL, 1)
--output
--    $action as action,
--    src.customer_id as id, 
--    src.name as name, 
--    src.city as city, 
--    src.loyalty_status as status
--) as c
--where action = 'update';

--insert * into dim_tables from #tmp_tb

--same error

-- Step 1: Close old versions
UPDATE target
SET end_date = GETDATE(), is_current = 0
FROM dim_customer target
JOIN staging_customer src
  ON target.customer_id = src.customer_id
WHERE target.is_current = 1
  AND (target.city <> src.city OR target.loyalty_status <> src.loyalty_status);

INSERT INTO dim_customer (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
SELECT src.customer_id, src.name, src.city, src.loyalty_status, GETDATE(), NULL, 1
FROM staging_customer src
LEFT JOIN dim_customer target
  ON src.customer_id = target.customer_id AND target.is_current = 1
WHERE target.customer_id IS NULL
   OR (target.city <> src.city OR target.loyalty_status <> src.loyalty_status);


select * from dim_customer
select * from staging_customer


select * from dim_store
select * from staging_store
truncate table dim_store

UPDATE target
SET end_date = GETDATE(), is_current = 0
FROM dim_store target
JOIN staging_store src
  ON target.store_id = src.store_id
WHERE target.is_current = 1
  AND (target.store_name <> src.store_name OR target.region <> src.region);

INSERT INTO dim_store (store_id, store_name, region, start_date, end_date, is_current)
SELECT src.store_id, src.store_name, src.region, GETDATE(), NULL, 1
FROM staging_store src
LEFT JOIN dim_store target
  ON src.store_id = target.store_id AND target.is_current = 1
WHERE target.store_id IS NULL
   OR (target.store_name <> src.store_name OR target.region <> src.region);