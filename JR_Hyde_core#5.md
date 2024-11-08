# Sam's Sub Final Project Deliverable 3 #
# Group 1 #

### Transform (dbt) ###
- Right click on sandwich and create a new file. Name this file `_src_sandwich.yml
- this file will have all the sources from the sanwich_staging airbyte tables.

```
version: 2

sources:
  - name: sandwich_staging
    database: group1project
    schema: sandwich_staging
    tables:
      - name: customers
      - name: employees
      - name: orders 
      - name: products
      - name: sandwiches
      - name: stores
      - name: supplier
      - name: orderlines
      - name: inventory    
```

#### dim_customer ####
- Create a new file inside of the sandwich model called `dim_customer.sql`

```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
customer_id as customer_key,
customer_id,
customer_phone as phone,
customer_birthdate as birthdate,
customer_last_name as last_name,
customer_first_name as first_name

FROM {{ source('sandwich_staging', 'customers') }}
```

#### dim_date ####
- Create a new file inside of the sandwich model called `dim_date.sql`
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}

with cte_date as (
{{ dbt_date.get_date_dimension("1900-01-01", "2050-12-31") }}
)

SELECT
date_day as date_key,
date_day,
day_of_week,
month_of_year as month,
quarter_of_year as quarter,
year_number as year
from cte_date
```

#### dim_employee ####
- Create a new file inside of the sandwich model called `dim_employee.sql`
  
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
employee_id as employee_key,
employee_id,
employee_birthdate as birthdate,
employee_last_name as last_name,
employee_first_name as first_name

FROM {{ source('sandwich_staging', 'employees') }}
```

#### dim inventory ####
- Create a new file inside of the sandwich model called `dim_inventory.sql`

```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
inventory_id as inventory_key,
inventory_id,
order_date,
product_id,
store_id,
supplier_id,
quantity_order,
product_cost

FROM {{ source('sandwich_staging', 'inventory') }}
```

#### dim_orderlines ####
- Create a new file inside of the sandwich model called `dim_orderlines.sql`

```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
order_line_id as order_line_key,
order_line_id,
product_id,
order_number as order_id,
order_line_qty as quantity,
order_line_price as price

FROM {{ source('sandwich_staging', 'orderlines') }}
```

#### dim_orders ####
- Create a new file inside of the sandwich model called `dim_orders.sql`
  
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
order_number as order_key,
order_number as order_id,
order_date,
store_id,
customer_id,
employee_id,
order_method,
order_total_price,
order_points_earned

FROM {{ source('sandwich_staging', 'orders') }}
```
#### dim_products ####
- Create a new file inside of the sandwich model called `dim_products.sql`
 
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
product_id as product_key,
product_id,
sandwich_id,
product_name as name,
product_cost as cost,
product_type as type,
product_calories as calories

FROM {{ source('sandwich_staging', 'products') }}
```
#### dim_sandwich ####
- Create a new file inside of the sandwich model called `dim_sandwich.sql`
 
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
sandwich_id as sandwich_key,
sandwich_id,
sandwich_length as lenght,
sandwich_bread_type as bread_type

FROM {{ source('sandwich_staging', 'sandwiches') }}
```
#### dim_stores ####
- Create a new file inside of the sandwich model called `dim_stores.sql`
 
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
store_id as store_key,
store_id,
store_zip as zip,
store_city as city,
store_state as state,
store_address as address

FROM {{ source('sandwich_staging', 'stores') }}
```

#### dim_supplier ####
- Create a new file inside of the sandwich model called `dim_supplier.sql`
  
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
    )
}}


SELECT
supplier_id as supplier_key,
supplier_id,
supplier_name as name,
supplier_state as state,
supplier_phone as phone

FROM {{ source('sandwich_staging', 'supplier') }}
```

#### fact_sales ####
- Create a new file inside of the sandwich model called `fact_sales.sql`

```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
) }}

SELECT
l.order_line_id as sales_key,
l.order_line_id,
c.customer_key,
d.date_key,
e.employee_key,
o.order_key,
p.product_key,
s.store_key,
CASE WHEN l.product_id = p.product_id THEN l.order_line_qty else 0 end as quantity,
CASE WHEN l.product_id = p.product_id THEN p.cost end  as price

FROM {{ source('sandwich_staging', 'orderlines') }} l
LEFT JOIN {{ ref('dim_orders') }} o ON l.order_number = o.order_id
LEFT JOIN {{ ref('dim_products') }} p ON l.product_id = p.product_id
LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
LEFT JOIN {{ ref('dim_date') }} d ON o.order_date = d.date_day
LEFT JOIN {{ ref('dim_employees') }} e ON o.employee_id = e.employee_id
LEFT JOIN {{ ref('dim_stores') }} s ON o.store_id = s.store_id
```
#### fact_inventory ####
- Create a new file inside of the sandwich model called `fact_inventory.sql`
 
```
{{ config(
    materialized = 'table',
    schema = 'sandwich'
) }}

SELECT
    i.inventory_id AS inventory_key,
    i.inventory_id AS ordered_inventory_key,
    p.product_key,
    d.date_key,
    s.store_key,
    ss.supplier_key,
    (SUM(CASE WHEN l.product_id = i.product_id THEN l.order_line_qty ELSE 0 END) - 50) AS quantity_on_hand,
    (SUM(CASE WHEN l.product_id = i.product_id THEN l.order_line_qty ELSE 0 END)) AS quantity_sold,
    i.quantity_order,
    i.product_cost AS inventory_cost

FROM {{ source('sandwich_staging', 'inventory') }} i
LEFT JOIN {{ ref('dim_date') }} d ON d.date_day = i.order_date
LEFT JOIN {{ ref('dim_products') }} p ON i.product_id = p.product_id
LEFT JOIN {{ ref('dim_stores') }} s ON i.store_id = s.store_id
LEFT JOIN {{ ref('dim_supplier') }} ss ON i.supplier_id = ss.supplier_id
LEFT JOIN {{ source('sandwich_staging', 'orderlines') }} l ON l.product_id = i.product_id

GROUP BY
    i.inventory_id,
    d.date_key,
    s.store_key,
    p.product_key,
    ss.supplier_key,
    i.quantity_order,
    i.product_cost

```

#### schema yaml file ####
- Create a new file inside the sandwich model called `_schema_sandwich.yml`
- This file contains metadata about the models you build
- Populate the code that we will use in this file below: 
```
version: 2

models:
     - name: dim_customer
          description: "Customer Dimension"
     - name: dim_date
         description: "Date Dimension"
     - name: dim_inventory
         description: "Inventory Dimension"
     - name: dim_orderlines
         description: "Dimension"
     - name: dim_orders
         description: "Orders Dimension"
     - name: dim_products
         description: "Product Dimension"
     - name: dim_sandwiches
         description: "sandwiches Dimension"
     - name: dim_stores
         description: "Stores Dimension"
     - name: dim_supplier
         description: "Suppliers Dimension"
     - name: fact_inventory
         description: "Inventory fact"
     - name: fact_claim
         description: "Sales Fact"
```


