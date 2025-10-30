use ECOMM;
 
select * from bundle_items;
select * from customers;
select * from order_items;
select * from order_sessions;
select * from orders;
select * from payments;
select * from product_bundles;
select * from product_price_history;
select * from products;
select * from returns;
select * from sessions;
select * from shipments;




use OLTP;

select * from bundle_items;
select * from customers;
select * from order_items;
select * from order_sessions;
select * from orders;
select * from payments;
select * from product_bundles;
select * from product_price_history;
select * from products;
select * from returns;
select * from sessions;
select * from shipments;





use Team3;

create table dim_customers (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    phone_new VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    signup_date DATE
);

create table dim_products (
    product_id BIGINT PRIMARY KEY,
    product_name VARCHAR(255),
    category_new VARCHAR(100),
    list_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    is_active BOOLEAN,
    created_at DATE
);

create table order_fact(        
order_id BIGINT,            
customer_id BIGINT,          
order_date DATE, 
status VARCHAR(20),
order_placed_at DATETIME ,
order_item_id BIGINT,       
product_id BIGINT ,         
quantity INT,
unit_price DECIMAL(10,2) , 
line_total DECIMAL(12,2),
payment_id BIGINT,          
payment_method VARCHAR(50),
amount DECIMAL(12,2),
payment_date DATE
);

show tables;

truncate dim_customers;
truncate dim_products;
truncate order_fact;

select * from dim_customers;
select * from dim_products;
select * from order_fact;



-- Insights
-- 1. no. of orders per customer
SELECT 
	dc.customer_id, 
    dc.first_name,
    dc.last_name,
	COUNT(odf.order_id) as total_orders
FROM dim_customers dc
JOIN order_fact odf ON dc.customer_id = odf.customer_id
GROUP BY dc.customer_id, dc.first_name, dc.last_name
ORDER BY total_orders DESC;

-- 2. most selling products (quantity not price)
SELECT
	dp.product_id,
    dp.product_name,
    SUM(odf.quantity) AS total_sold
FROM dim_products dp
JOIN order_fact odf ON dp.product_id = odf.product_id
GROUP BY dp.product_id, dp.product_name
ORDER BY total_sold DESC;


-- 3. orders per month
SELECT
	YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    COUNT(order_id) AS total_orders
FROM order_fact
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

-- 4. most selling categories
SELECT 
	dp.category_new,
    SUM(odf.quantity) AS total_sold
FROM dim_products dp
JOIN order_fact odf ON dp.product_id = odf.product_id
GROUP BY dp.category_new
ORDER BY total_sold DESC;

-- 5. revenue per category
SELECT 
	dp.category_new,
    SUM(odf.line_total) AS total_revenue
FROM dim_products dp
JOIN order_fact odf ON dp.product_id = odf.product_id
GROUP BY dp.category_new
ORDER BY total_revenue DESC;

-- 6. total revenue per each month
SELECT
	YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(line_total) AS total_revenue
FROM order_fact
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;

-- 7. difference between revenue of current month with previous month
SELECT
	YEAR(order_date) AS year,
    MONTH(order_date) AS month,
	SUM(line_total) AS current_month_revenue,
    SUM(line_total) - LAG(SUM(line_total), 1, 0) OVER (ORDER BY year(order_date), month(order_date)) AS diff_from_prev
FROM order_fact
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month