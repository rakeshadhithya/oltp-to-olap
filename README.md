# OLTP to OLAP Data Pipeline Project

## Overview

This project designs and implements a star-schema OLAP (Online Analytical Processing) model to transform data from an OLTP (Online Transactional Processing) system. It includes an ETL (Extract, Transform, Load) pipeline that ingests, transforms, and loads OLTP data into OLAP tables, enabling efficient analytical reporting and insights.

## Features

- **Star-Schema Design**: A robust OLAP model with fact and dimension tables optimized for analytical queries.
- **ETL Pipeline**: Extracts data from an OLTP source, applies transformations (e.g., data cleansing, aggregation), and loads it into the OLAP database.
- **Analytical Insights**: Supports reporting and querying for business intelligence and decision-making.

### ETL Pipeline Details

The ETL pipeline (`src/etl_pipeline.py`) performs the following steps:

- **Extract**: Connects to the OLTP MySQL database and retrieves data from the following tables:

  - `customers`: Customer details (e.g., ID, name, email, phone, city).
  - `products`: Product details (e.g., ID, name, category, prices).
  - `orders`: Order details (e.g., ID, customer ID, order date, status).
  - `order_items`: Order item details (e.g., item ID, order ID, product ID, quantity).
  - `returns`: Return details (e.g., return ID, order ID, reason code).
  - `payments`: Payment details (e.g., payment ID, order ID, amount).

- **Transform**:

  - Cleans phone numbers in the `customers` table to a standardized 10-digit format.
  - Maps raw product categories to standardized categories using a predefined mapping.
  - Merges `orders`, `order_items`, `returns`, and `payments` into a fact table for the OLAP schema.

- **Load**:
  - Loads transformed data into the OLAP database (`Team3`) with the following structure:
    - `dim_customers`: Customer dimension table (e.g., customer ID, name, email, phone, city).
    - `dim_products`: Product dimension table (e.g., product ID, name, category, prices).
    - `order_fact`: Fact table combining order, item, return, and payment details (e.g., order ID, customer ID, product ID, order value, refund amount).
  - Uses SQLAlchemy to write data to the OLAP database.

### Example SQL Query

To generate a sales report by region and product category:

```sql
SELECT
    dc.city,
    dp.category_new,
    SUM(of.order_value) as total_sales,
    SUM(of.refund_amount) as total_refunded
FROM
    order_fact of
    JOIN dim_customers dc ON of.customer_id = dc.customer_id
    JOIN dim_products dp ON of.product_id = dp.product_id
GROUP BY
    dc.city,
    dp.category_new;
```
