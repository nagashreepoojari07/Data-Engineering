CREATE TABLE staging_customer (
    customer_id VARCHAR(10) NOT NULL,
    name VARCHAR(100),
    city VARCHAR(50),
    loyalty_status VARCHAR(20)
);

CREATE TABLE staging_product (
    product_id VARCHAR(10) NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50)
);

CREATE TABLE staging_store (
    store_id VARCHAR(10) NOT NULL,
    store_name VARCHAR(100),
    region VARCHAR(50)
);

-- John Doe changed city and loyalty status again
INSERT INTO staging_customer (customer_id, name, city, loyalty_status)
VALUES
('CUST001','John Doe','Boston','Platinum'),
('CUST004','Bob Marley','Miami','Silver');  -- New customer

-- Product name change (Type 1)
INSERT INTO staging_product (product_id, product_name, category, brand)
VALUES
('PROD001','Laptop X Ultra','Electronics','BrandA'), -- updated name
('PROD006','Gaming Mouse','Accessories','BrandF');   -- new product

-- Store region change and new store
INSERT INTO staging_store (store_id, store_name, region)
VALUES
('STORE01','Main Street Store','South'),  -- region changed
('STORE04','Airport Store','East');       -- new store
