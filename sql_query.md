# PostgreSQL SQL Queries Used in ETL Project

This document lists the key SQL queries executed during the setup, development, and debugging of the ETL pipeline in PostgreSQL.

## 1. Database and User Setup

These queries are executed as the `postgres` superuser (e.g., `sudo -u postgres psql`).

* **Create the database:**
    ```sql
    CREATE DATABASE ecommerce_dw;
    ```

* **Create the ETL user:**
    ```sql
    CREATE USER etl_user WITH PASSWORD 'your_secure_password';
    ```

* **Grant all privileges on the database to the ETL user:**
    ```sql
    GRANT ALL PRIVILEGES ON DATABASE ecommerce_dw TO etl_user;
    ```

* **Change ownership of the `public` schema (Crucial for permissions):**
    * _Note: Connect to the specific database first, e.g., `sudo -u postgres psql -d ecommerce_dw`_
    ```sql
    ALTER SCHEMA public OWNER TO etl_user;
    ```

* **Grant `CREATE` and `USAGE` on the `public` schema to the ETL user:**
    ```sql
    GRANT CREATE ON SCHEMA public TO etl_user;
    GRANT USAGE ON SCHEMA public TO etl_user;
    ```

* **Alter default privileges for future tables created by `etl_user` in `public` schema:**
    ```sql
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO etl_user;
    ```

## 2. Table Management

These queries are executed by the `etl_user` (either directly in `psql` after connecting as `etl_user`, or via the Python script).

* **Create the `sales_fact` table:**
    ```sql
    CREATE TABLE IF NOT EXISTS sales_fact (
        order_id TEXT PRIMARY KEY,
        order_date DATE,
        sales_channel TEXT,
        customer_id TEXT,
        style TEXT,
        sku TEXT,
        size TEXT,
        quantity INTEGER,
        unit_price NUMERIC,
        amount NUMERIC,
        total_amount NUMERIC,
        total_price NUMERIC,
        current_stock NUMERIC
    );
    ```
    _Note: The `PRIMARY KEY (order_id)` constraint was added for data integrity in the `notes.md` file. In the actual `etl_pipeline.py` script, `order_id` may not be guaranteed unique from combined sources unless explicitly handled for all cases._

* **Truncate the `sales_fact` table (for clean re-loads in ETL):**
    ```sql
    TRUNCATE TABLE sales_fact;
    ```

## 3. Data Inspection and Debugging

These queries were crucial for understanding the data state and diagnosing issues within the ETL pipeline.

* **Get top sales by sales channel (main analytical query):**
    ```sql
    SELECT
        sales_channel,
        SUM(total_price) AS total_sales_amount,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_quantity_sold
    FROM
        sales_fact
    GROUP BY
        sales_channel
    ORDER BY
        total_sales_amount DESC;
    ```
### RESULT:
```sql
 sales_channel | total_sales_amount | total_orders | total_quantity_sold 
---------------+--------------------+--------------+---------------------
 Amazon.in     |        78592678.30 |       120254 |              116482
 Unknown       |        15768162.19 |            1 |               24017
 Non-Amazon    |               0.00 |          124 |                 167
(3 rows)
```

* **Inspect `Non-Amazon` sales details (for debugging $0.00 issue):**
    ```sql
    SELECT
        order_id,
        sales_channel,
        quantity,
        unit_price,
        amount,
        total_price
    FROM
        sales_fact
    WHERE
        sales_channel = 'Non-Amazon'
    LIMIT 10;
    ```
### RESULT:
```sql
      order_id       | sales_channel | quantity | unit_price | amount | total_price 
---------------------+---------------+----------+------------+--------+-------------
 S02-9542567-5952341 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-4520403-8992257 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-1862183-4415901 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-0835825-7267219 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-7568319-2142153 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-9236057-6330320 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-3986693-8942048 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-4244793-3107843 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-7889185-5931306 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
 S02-0764737-6642956 | Non-Amazon    |        1 |       0.00 |   0.00 |        0.00
(10 rows)
```
--- 
# Some Defining Business Questions
## **Section 1: Overall Sales Performance**

These queries will give you a high-level overview of your sales.

**Business Question 1.1: What is the total revenue and total quantity sold across all channels?**

```sql
SELECT
    SUM(total_price) AS total_revenue,
    SUM(quantity) AS total_quantity_sold
FROM
    sales_fact;
```
### RESULT:
```sql
total_revenue | total_quantity_sold 
--------------+---------------------
94360840.49   |              140666
(1 row)
```

**Business Question 1.2: How do sales trend over time, specifically showing total revenue and quantity sold per month?**

```sql
SELECT
    TO_CHAR(order_date, 'YYYY-MM') AS sales_month,
    SUM(total_price) AS monthly_revenue,
    SUM(quantity) AS monthly_quantity_sold,
    COUNT(DISTINCT order_id) AS monthly_total_orders
FROM
    sales_fact
GROUP BY
    sales_month
ORDER BY
    sales_month;
```
### RESULT:
```sql
sales_month | monthly_revenue | monthly_quantity_sold | monthly_total_orders 
------------+-----------------+-----------------------+----------------------
2021-06     |       988233.00 |                  1433 |                    1
2021-07     |       843551.00 |                  1328 |                    1
2021-08     |      1150916.00 |                  1889 |                    1
2021-09     |      1294808.00 |                  1805 |                    1
2021-10     |      2865134.00 |                  4622 |                    1
2021-11     |      1378104.00 |                  2022 |                    1
2021-12     |       803444.00 |                  1062 |                    1
2022-01     |       913688.00 |                  1272 |                    1
2022-02     |      2412848.00 |                  4140 |                    1
2022-03     |      1808545.85 |                  2994 |                  159
2022-04     |     30024799.07 |                 45514 |                45859
2022-05     |     26450960.19 |                 38309 |                39222
2022-06     |     23425809.38 |                 34276 |                35141
(13 rows)
```

**Business Question 1.3: What are the top 5 sales channels by total revenue?**

```sql
SELECT
    sales_channel,
    SUM(total_price) AS total_channel_revenue,
    COUNT(DISTINCT order_id) AS total_orders_on_channel,
    SUM(quantity) AS total_quantity_on_channel
FROM
    sales_fact
GROUP BY
    sales_channel
ORDER BY
    total_channel_revenue DESC
LIMIT 5;
```
### RESULT:
```sql
sales_channel | total_channel_revenue | total_orders_on_channel | total_quantity_on_channel 
--------------+-----------------------+-------------------------+---------------------------
Amazon.in     |           78592678.30 |                  120254 |                    116482
Unknown       |           15768162.19 |                       1 |                     24017
Non-Amazon    |                  0.00 |                     124 |                       167
(3 rows)
```

*(Note: As previously discussed, 'Non-Amazon' sales will show $0.00 here due to source data limitations, and 'Unknown' orders will reflect the synthetic IDs generated, making their count more accurate now.)*

---

## **Section 2: Product Performance**

These queries focus on identifying your best-performing products.

**Business Question 2.1: What are the top 10 products (by SKU) based on total revenue?**

```sql
SELECT
    sku,
    SUM(total_price) AS total_product_revenue,
    SUM(quantity) AS total_quantity_sold,
    COUNT(DISTINCT order_id) AS total_orders_for_product
FROM
    sales_fact
WHERE
    sku IS NOT NULL AND sku != 'Unknown' -- Exclude unknown SKUs for meaningful analysis
GROUP BY
    sku
ORDER BY
    total_product_revenue DESC
LIMIT 10;
```

### RESULT:
```sql
      sku       | total_product_revenue | total_quantity_sold | total_orders_for_product 
----------------+-----------------------+---------------------+--------------------------
J0230-SKD-M     |             549334.20 |                 483 |                      507
JNE3797-KR-L    |             524581.77 |                 661 |                      773
J0230-SKD-S     |             492902.14 |                 430 |                      453
JNE3797-KR-M    |             455265.16 |                 562 |                      658
JNE3797-KR-S    |             407302.57 |                 503 |                      587
JNE3797-KR-XL   |             334007.24 |                 417 |                      475
J0230-SKD-L     |             327050.95 |                 287 |                      298
SET268-KR-NP-XL |             313307.96 |                 405 |                      387
SET268-KR-NP-L  |             309334.00 |                 403 |                      363
JNE3797-KR-XS   |             303616.70 |                 386 |                      431
(10 rows)
```

**Business Question 2.2: What are the top 10 products (by SKU) based on total quantity sold?**

```sql
SELECT
    sku,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_price) AS total_product_revenue,
    COUNT(DISTINCT order_id) AS total_orders_for_product
FROM
    sales_fact
WHERE
    sku IS NOT NULL AND sku != 'Unknown'
GROUP BY
    sku
ORDER BY
    total_quantity_sold DESC
LIMIT 10;
```
### RESULT:
```sql
      sku       | total_quantity_sold | total_product_revenue | total_orders_for_product 
----------------+---------------------+-----------------------+--------------------------
JNE3797-KR-L    |                 661 |             524581.77 |                      773
JNE3797-KR-M    |                 562 |             455265.16 |                      658
JNE3405-KR-L    |                 518 |             206852.29 |                      536
JNE3797-KR-S    |                 503 |             407302.57 |                      587
J0230-SKD-M     |                 483 |             549334.20 |                      507
J0230-SKD-S     |                 430 |             492902.14 |                      453
JNE3797-KR-XL   |                 417 |             334007.24 |                      475
JNE3405-KR-S    |                 416 |             175945.87 |                      444
SET268-KR-NP-XL |                 405 |             313307.96 |                      387
SET268-KR-NP-L  |                 403 |             309334.00 |                      363
(10 rows)
```
**Business Question 2.3: How do sales perform across different product categories?**

```sql
SELECT
    product_category,
    SUM(total_price) AS total_category_revenue,
    SUM(quantity) AS total_category_quantity_sold,
    COUNT(DISTINCT order_id) AS total_orders_in_category
FROM
    sales_fact
WHERE
    product_category IS NOT NULL AND product_category != 'Unknown' -- Exclude unknown categories
GROUP BY
    product_category
ORDER BY
    total_category_revenue DESC;
```
### RESULT:
```sql
  product_category   | total_category_revenue | total_category_quantity_sold | total_orders_in_category 
---------------------+------------------------+------------------------------+--------------------------
KURTA SET            |            29865897.51 |                        33933 |                    30910
KURTA                |            27782604.27 |                        55304 |                    48089
SET                  |            12460162.68 |                        13556 |                    13223
DRESS                |             8674597.03 |                        10806 |                    10133
TOP                  |             4825223.71 |                         9176 |                     7073
BLOUSE               |             1903071.37 |                         2574 |                      816
NIGHT WEAR           |             1273022.84 |                         2280 |                     2183
TUNIC                |             1148094.57 |                         2092 |                     1436
LEHENGA CHOLI        |              613116.57 |                          474 |                      473
CROP TOP WITH PLAZZO |              391701.10 |                          466 |                      498
PALAZZO              |              278564.04 |                          691 |                      172
PANT                 |              244173.82 |                          720 |                      100
SAREE                |              146934.76 |                          182 |                      134
CROP TOP             |               81271.08 |                          217 |                      195
AN : LEGGINGS        |               43404.12 |                          143 |                      115
KURTI                |               16771.43 |                           28 |                       31
JUMPSUIT             |               14223.00 |                           16 |                       16
CARDIGAN             |               10047.90 |                           19 |                       21
BOTTOM               |                 349.00 |                            1 |                        1
(19 rows)
```

---

## **Section 3: Customer Behavior (Corrected Queries)**

These queries provide insights into your customer base, now correctly using the `customer_name` column.

**Business Question 3.1: How many unique customers do we have?**

```sql
SELECT
    COUNT(DISTINCT customer_name) AS total_unique_customers
FROM
    sales_fact
WHERE
    customer_name IS NOT NULL AND customer_name != 'Unknown'; -- Exclude unknown customers
```
### RESULT:
```sql
 total_unique_customers 
------------------------
                    161
(1 row)
```

**Business Question 3.2: Who are the top 10 customers by total spending?**

```sql
SELECT
    customer_name,
    SUM(total_price) AS total_spending,
    COUNT(DISTINCT order_id) AS total_orders_placed
FROM
    sales_fact
WHERE
    customer_name IS NOT NULL AND customer_name != 'Unknown'
GROUP BY
    customer_name
ORDER BY
    total_spending DESC
LIMIT 10;
```
### RESULT:
```sql
           customer_name            | total_spending | total_orders_placed 
------------------------------------+----------------+---------------------
 MULBERRIES BOUTIQUE                |     2094070.50 |                   1
 AMANI CONCEPT TRADING LLC (KAPDA)  |      930451.00 |                   1
 VAHARSHA BOUTIQUE                  |      527214.00 |                   1
 GALAXY GROUP OF COMPANIES PVT. LTD |      445058.00 |                   1
 RIVAAN LLC                         |      443042.00 |                   1
 SURE FASHIONS LLC                  |      403253.00 |                   1
 BHANU SALEINE NAUNITHAM            |      356998.00 |                   1
 COTTON CLOSET LTD                  |      345265.00 |                   1
 VISHA DEVAN                        |      321028.00 |                   1
 NIRUSAH TAILORING                  |      316470.00 |                   1
(10 rows)
```

---

## **Section 4: Operational Metrics**

These queries can help identify operational patterns.

**Business Question 4.1: How do sales vary by day of the week?**

```sql
SELECT
    EXTRACT(DOW FROM order_date) AS day_of_week_num, -- 0=Sunday, 1=Monday, ..., 6=Saturday
    TO_CHAR(order_date, 'Day') AS day_of_week_name,
    SUM(total_price) AS daily_revenue,
    SUM(quantity) AS daily_quantity_sold,
    COUNT(DISTINCT order_id) AS daily_total_orders
FROM
    sales_fact
GROUP BY
    day_of_week_num, day_of_week_name
ORDER BY
    day_of_week_num;
```
### RESULT:
```sql
  day_of_week_num | day_of_week_name | daily_revenue | daily_quantity_sold | daily_total_orders 
 -----------------+------------------+---------------+---------------------+--------------------
                0 | Sunday           |   12109264.30 |               17976 |              18207
                1 | Monday           |   13910863.28 |               20224 |              17368
                2 | Tuesday          |   14526506.57 |               21523 |              17588
                3 | Wednesday        |   13424600.35 |               20191 |              17371
                4 | Thursday         |   13121278.27 |               19378 |              16006
                5 | Friday           |   13367047.20 |               19993 |              16581
                6 | Saturday         |   13901280.52 |               21381 |              17264
  (7 rows)
```

**Business Question 4.2: How do sales vary by hour of the day?**

```sql
SELECT
    EXTRACT(HOUR FROM order_date) AS order_hour,
    SUM(total_price) AS hourly_revenue,
    SUM(quantity) AS hourly_quantity_sold,
    COUNT(DISTINCT order_id) AS hourly_total_orders
FROM
    sales_fact
GROUP BY
    EXTRACT(HOUR FROM order_date)  -- Add the extracted hour to GROUP BY
ORDER BY
    order_hour;
```
### RESULT:
```sql
 order_hour | hourly_revenue | hourly_quantity_sold | hourly_total_orders 
------------+----------------+----------------------+---------------------
          0 |    94360840.49 |               140666 |              120379
(1 row)
```
---