Create database Olist_Store_Analysis;
use Olist_Store_Analysis;

CREATE TABLE olist_customers_dataset (
    customer_id VARCHAR(50),
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(50),
    customer_state VARCHAR(5)
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_customers_dataset.csv'
INTO TABLE olist_customers_dataset
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'  
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_geolocation_dataset (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(10, 6),
    geolocation_lng DECIMAL(10, 6),
    geolocation_city VARCHAR(50),
    geolocation_state VARCHAR(5)
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_geolocation_dataset.csv'
INTO TABLE olist_geolocation_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_order_items_dataset (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2)
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_order_items_dataset.csv'
INTO TABLE olist_order_items_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_order_payments_dataset (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10, 2)
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE olist_order_reviews_dataset (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_order_reviews_dataset.csv'
INTO TABLE olist_order_reviews_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'  -- Fixes Windows-style line endings
IGNORE 1 ROWS;

CREATE TABLE olist_orders_dataset (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_orders_dataset.csv'
INTO TABLE olist_orders_dataset
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@order_id, @customer_id, @order_status, @order_purchase_timestamp, @order_estimated_delivery_date)
SET 
order_id = @order_id, 
customer_id = @customer_id, 
order_status = @order_status, 
order_purchase_timestamp = STR_TO_DATE(@order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
order_estimated_delivery_date = STR_TO_DATE(@order_estimated_delivery_date, '%Y-%m-%d %H:%i:%s');

CREATE TABLE olist_products_dataset (
    product_id VARCHAR(50),
    product_category_name VARCHAR(50),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10, 2),
    product_length_cm DECIMAL(10, 2),
    product_height_cm DECIMAL(10, 2),
    product_width_cm DECIMAL(10, 2)
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_products_dataset.csv'
INTO TABLE olist_products_dataset
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, product_category_name, product_name_length, product_description_length, 
product_photos_qty, @product_weight_g, @product_length_cm, @product_height_cm, @product_width_cm)
SET 
product_weight_g = NULLIF(TRIM(REPLACE(@product_weight_g, '\r', '')), ''),
product_length_cm = NULLIF(TRIM(REPLACE(@product_length_cm, '\r', '')), ''),
product_height_cm = NULLIF(TRIM(REPLACE(@product_height_cm, '\r', '')), ''),
product_width_cm = NULLIF(TRIM(REPLACE(@product_width_cm, '\r', '')), '');

CREATE TABLE olist_sellers_dataset (
    seller_id VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city VARCHAR(50),
    seller_state VARCHAR(5)
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\olist_sellers_dataset.csv'
INTO TABLE olist_sellers_dataset
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'  
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(50),
    product_category_name_english VARCHAR(50)
);

LOAD DATA LOCAL INFILE 'D:\\ExcelR Solutions\\Projects\\Ecommerce Project\\ecommerce\\product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ','  
ENCLOSED BY '"'  
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

	#----Joins----#
    
    SELECT 
    geolocation_zip_code_prefix,  
    AVG(geolocation_lat) AS avg_latitude,
    AVG(geolocation_lng) AS avg_longitude
FROM 
    olist_geolocation_dataset
GROUP BY 
    geolocation_zip_code_prefix;
    
    SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state
FROM 
    olist_orders_dataset o
LEFT JOIN 
    olist_customers_dataset c ON o.customer_id = c.customer_id;
    
    SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    geo_c.avg_latitude AS customer_latitude,
    geo_c.avg_longitude AS customer_longitude
FROM 
    olist_orders_dataset o
LEFT JOIN 
    olist_customers_dataset c ON o.customer_id = c.customer_id
LEFT JOIN (
    SELECT 
        geolocation_zip_code_prefix,  
        AVG(geolocation_lat) AS avg_latitude,
        AVG(geolocation_lng) AS avg_longitude
    FROM 
        olist_geolocation_dataset
    GROUP BY 
        geolocation_zip_code_prefix
) geo_c ON c.customer_zip_code_prefix = geo_c.geolocation_zip_code_prefix;

SELECT 
    i.order_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    p.product_category_name,
    p.product_name_length,
    p.product_description_length,
    s.seller_city,
    s.seller_state
FROM 
    olist_order_items_dataset i
LEFT JOIN 
    olist_products_dataset p ON i.product_id = p.product_id
LEFT JOIN 
    olist_sellers_dataset s ON i.seller_id = s.seller_id;
    
    SELECT
    o.order_id,
    pay.payment_type,
    pay.payment_installments,
    pay.payment_value,
    r.review_score,
    r.review_creation_date,
    r.review_answer_timestamp
FROM 
    olist_orders_dataset o
LEFT JOIN 
    olist_order_payments_dataset pay ON o.order_id = pay.order_id
LEFT JOIN 
    olist_order_reviews_dataset r ON o.order_id = r.order_id;
    
    ###--------------KPI'S------------------###
    
    ###---1. Weekday Vs Weekend Payment Statistics-------##
    
   SELECT
    CASE 
        WHEN DAYOFWEEK(order_purchase_timestamp) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(pay.payment_value) AS total_payment,
    AVG(pay.payment_value) AS avg_payment
FROM olist_orders_dataset o
LEFT JOIN olist_order_payments_dataset pay ON o.order_id = pay.order_id
GROUP BY day_type;

###--------2: Orders with Review Score 5 and Credit Card Payments------###

SELECT 
    COUNT(DISTINCT o.order_id) AS total_orders
FROM olist_orders_dataset o
LEFT JOIN olist_order_reviews_dataset r ON o.order_id = r.order_id
LEFT JOIN olist_order_payments_dataset pay ON o.order_id = pay.order_id
WHERE r.review_score = 5 AND pay.payment_type = 'credit_card';

###-------3: Avg Days Taken for Delivery – Pet Shop Products-----------###

SELECT COUNT(*) AS count_pet_shop_products
FROM olist_products_dataset
WHERE product_category_name = 'pet_shop';

SELECT COUNT(*) AS pet_shop_orders
FROM olist_order_items_dataset i
JOIN olist_products_dataset p ON i.product_id = p.product_id
WHERE p.product_category_name = 'pet_shop';

SELECT 
    COUNT(*) AS total_pet_shop_orders
FROM olist_orders_dataset o
JOIN olist_order_items_dataset i ON o.order_id = i.order_id
JOIN olist_products_dataset p ON i.product_id = p.product_id
WHERE p.product_category_name = 'pet_shop';


SELECT COUNT(*) AS total_delivered_orders
FROM olist_orders_dataset
WHERE order_status = 'delivered';

###-------4: Avg Price & Payment Value – São Paulo Customers----------###

WITH orderItemsAvg AS (
	Select round(AVG(item.price)) AS avg_order_item_price
	from olist_order_items_dataset item
	join olist_orders_dataset ord ON item.order_id = ord.order_id
	join olist_customers_dataset cust ON ord.customer_id = cust.customer_id
	where cust.customer_city = "Sao Paulo"
)
select
	(select avg_order_item_price from orderItemsAvg) AS avg_order_item_price,
	round(AVG(pmt.payment_value)) AS avg_payment_value
from olist_order_payments_dataset pmt
Join olist_orders_dataset ord ON pmt.order_id = ord.order_id
join olist_customers_dataset cust ON ord.customer_id = cust.customer_id
where cust.customer_city = "Sao Paulo";


###--------5: Shipping Days vs Review Score Relationship----------###

SELECT 
    r.review_score,
   COUNT(*) AS total_reviews
FROM olist_order_reviews_dataset r
GROUP BY r.review_score
ORDER BY r.review_score;

##----Top 5 Selling Products (by number of times sold)----##

SELECT 
    p.product_category_name,
    COUNT(i.order_id) AS total_sold
FROM olist_order_items_dataset i
JOIN olist_products_dataset p ON i.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY total_sold DESC
LIMIT 5;

##-----Top 5 Sellers (by number of order items)----##

SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(i.order_id) AS total_orders
FROM olist_order_items_dataset i
JOIN olist_sellers_dataset s ON i.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY total_orders DESC
LIMIT 5;