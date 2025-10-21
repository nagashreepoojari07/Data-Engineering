--create database RetailDW;

CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(10) NOT NULL,
    name VARCHAR(100),
    city VARCHAR(50),
    loyalty_status VARCHAR(20),
    start_date DATE NOT NULL,
    end_date DATE NULL,
    is_current BIT NOT NULL
);

-- Old record
INSERT INTO dim_customer (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
VALUES ('CUST001','John Doe','New York','Silver','2022-01-01','2023-03-15',0);

-- Current record after change
INSERT INTO dim_customer (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
VALUES ('CUST001','John Doe','Chicago','Gold','2023-03-16',NULL,1);
select * from dim_customer

-- Customer 1 changes city
INSERT INTO dim_customer (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
VALUES 
('CUST001','John Doe','New York','Silver','2022-01-01','2023-03-15',0),
('CUST001','John Doe','Chicago','Gold','2023-03-16',NULL,1);

-- Customer 2 stays same
INSERT INTO dim_customer (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
VALUES 
('CUST002','Jane Smith','Los Angeles','Gold','2022-05-10',NULL,1);

-- Customer 3 changed loyalty status
INSERT INTO dim_customer (customer_id, name, city, loyalty_status, start_date, end_date, is_current)
VALUES
('CUST003','Alice Brown','Houston','Silver','2022-03-01','2023-06-01',0),
('CUST003','Alice Brown','Houston','Platinum','2023-06-02',NULL,1);

select * from dim_customer

CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(10) NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50)
);


INSERT INTO dim_product (product_id, product_name, category, brand)
VALUES ('PROD001','Laptop X','Electronics','BrandA');
INSERT INTO dim_product (product_id, product_name, category, brand)
VALUES
('PROD001','Laptop X','Electronics','BrandA'),
('PROD002','Smartphone Y','Electronics','BrandB'),
('PROD003','Headphones Z','Accessories','BrandC'),
('PROD004','Office Chair','Furniture','BrandD'),
('PROD005','Coffee Maker','Appliances','BrandE');

select * from dim_product   

CREATE TABLE dim_store (
    store_key INT IDENTITY(1,1) PRIMARY KEY,
    store_id VARCHAR(10) NOT NULL,
    store_name VARCHAR(100),
    region VARCHAR(50),
    start_date DATE NOT NULL,
    end_date DATE NULL,
    is_current BIT NOT NULL
);

INSERT INTO dim_store (store_id, store_name, region, start_date, end_date, is_current)
VALUES ('STORE01','Main Street Store','East','2022-01-01','2023-06-01',0);

INSERT INTO dim_store (store_id, store_name, region, start_date, end_date, is_current)
VALUES ('STORE01','Main Street Store','North','2023-06-02',NULL,1);

-- Store 1 moved region
INSERT INTO dim_store (store_id, store_name, region, start_date, end_date, is_current)
VALUES
('STORE01','Main Street Store','East','2022-01-01','2023-06-01',0),
('STORE01','Main Street Store','North','2023-06-02',NULL,1);

-- Store 2 never moved
INSERT INTO dim_store (store_id, store_name, region, start_date, end_date, is_current)
VALUES
('STORE02','City Mall Store','West','2022-02-15',NULL,1);

-- Store 3 changed region
INSERT INTO dim_store (store_id, store_name, region, start_date, end_date, is_current)
VALUES
('STORE03','Town Center','South','2022-03-10','2023-01-20',0),
('STORE03','Town Center','Central','2023-01-21',NULL,1);



select * from dim_store

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,  -- YYYYMMDD format
    full_date DATE NOT NULL,
    day INT,
    month INT,
    quarter INT,
    year INT
);

INSERT INTO dim_date (date_key, full_date, day, month, quarter, year)
VALUES (20231001,'2023-10-01',1,10,4,2023);

INSERT INTO dim_date (date_key, full_date, day, month, quarter, year)
VALUES

(20231002,'2023-10-02',2,10,4,2023),
(20231003,'2023-10-03',3,10,4,2023),
(20231004,'2023-10-04',4,10,4,2023),
(20231005,'2023-10-05',5,10,4,2023),
(20231006,'2023-10-06',6,10,4,2023),
(20231007,'2023-10-07',7,10,4,2023);


select * from dim_date

CREATE TABLE fact_sales (
    sales_key INT IDENTITY(1,1) PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    store_key INT NOT NULL,
    quantity_sold INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(5,2),
    revenue AS (quantity_sold * unit_price - discount) PERSISTED,
    
    CONSTRAINT FK_fact_customer FOREIGN KEY (customer_key) 
        REFERENCES dim_customer(customer_key) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,
    
    CONSTRAINT FK_fact_product FOREIGN KEY (product_key) 
        REFERENCES dim_product(product_key) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,
    
    CONSTRAINT FK_fact_store FOREIGN KEY (store_key) 
        REFERENCES dim_store(store_key) 
        ON DELETE CASCADE 
        ON UPDATE CASCADE,
    
    CONSTRAINT FK_fact_date FOREIGN KEY (date_key) 
        REFERENCES dim_date(date_key)
);

drop table fact_sales;
INSERT INTO fact_sales (date_key, customer_key, product_key, store_key, quantity_sold, unit_price, discount)
VALUES (20231001,2,1,2,3,500.00,50.00);

INSERT INTO fact_sales (date_key, customer_key, product_key, store_key, quantity_sold, unit_price, discount)
VALUES
-- Sales for John Doe
(20231001,2,1,2,2,1200.00,100.00),
(20231002,2,2,2,1,800.00,0.00),
(20231003,1,3,1,5,50.00,10.00),

-- Sales for Jane Smith
(20231001,3,1,2,1,1200.00,50.00),
(20231004,3,4,3,2,150.00,0.00),
(20231005,3,5,3,1,80.00,5.00),

-- Sales for Alice Brown
(20231002,5,2,3,3,800.00,0.00),
(20231003,5,3,1,2,50.00,0.00),
(20231006,5,4,1,1,150.00,20.00);

select * from fact_sales