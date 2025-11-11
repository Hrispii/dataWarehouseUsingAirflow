# 🗂️ Data Catalog — Gold Layer

This catalog describes the analytics-ready tables produced in the **Gold Layer** of the warehouse.  
These models combine multiple Silver Layer sources into clean, denormalized structures optimized  
for BI dashboards, analytical queries, and downstream business reporting.

---------------------------------------------------------------------------------------------

## 🧩 1. `gold_layer.dim_products`

📝 **Purpose:**  
Provides a complete, analytics-friendly product dimension that merges product attributes, warehouse  
metadata, inventory levels, and customer review information into a single table.

| Column Name          | Data Type        | Description |
|----------------------|------------------|-------------|
| `product_id`         | INT              | Unique identifier of the product. |
| `warehouse_id`       | INT              | Identifier of the warehouse where the product is stored. |
| `product_name`       | VARCHAR          | Human-readable name of the product. |
| `product_category`   | VARCHAR          | Category associated with the product. |
| `brand`              | VARCHAR          | Brand or manufacturer of the product. |
| `product_price`      | NUMERIC          | Price of the product. |
| `currency`           | VARCHAR          | Currency in which the product price is expressed. |
| `product_amount`     | INT              | Stock quantity available at the warehouse. |
| `warehouse_name`     | VARCHAR          | Name of the warehouse storing the product. |
| `warehouse_location` | VARCHAR          | Geographical location of the warehouse. |
| `users_rating`       | INT              | Customer rating for the product, if available. |
| `users_comments`     | VARCHAR          | Review text or feedback from users. |

---------------------------------------------------------------------------------------------

## 📦 2. `gold_layer.dim_customers`

📝 **Purpose:**  
Delivers a refined customer dimension containing cleaned and standardized identifiers, demographic  
information, and geography fields suitable for customer segmentation and analysis.

| Column Name     | Data Type | Description |
|------------------|-----------|-------------|
| `customer_id`    | INT       | Unique identifier for each customer. |
| `email`          | VARCHAR   | Primary email address of the customer. |
| `name`           | VARCHAR   | Cleaned and standardized full name. |
| `signup_date`    | DATE      | The date when the customer registered. |
| `country`        | VARCHAR   | Country of residence. |
| `city`           | VARCHAR   | City of residence. |
| `segment`        | VARCHAR   | Customer segment or classification. |

---------------------------------------------------------------------------------------------

## 🚚 3. `gold_layer.fact_orders`

📝 **Purpose:**  
Captures all order-level metrics by combining order items, order metadata, payment details, and  
shipment information into a single fact table with one row per order item.

| Column Name         | Data Type | Description |
|----------------------|-----------|-------------|
| `order_id`           | INT       | Unique identifier for the order. |
| `product_id`         | INT       | Identifier of the purchased product. |
| `quantity_of_purchased` | INT   | Number of units included in the order line. |
| `unit_price`         | NUMERIC   | Price per product unit at order time. |
| `total_price`        | NUMERIC   | Total price for the order line. |
| `payment_method`     | VARCHAR   | Payment method used by the customer. |
| `currency`           | VARCHAR   | Currency of the transaction. |
| `status_of_order`    | VARCHAR   | Current order status (delivered, pending, etc.). |
| `order_date`         | DATE      | Date when the order was placed. |
| `carrier_company`    | VARCHAR   | Shipping provider assigned to the order. |
| `shipment_status`    | VARCHAR   | Delivery status of the shipment. |
| `delivery_date`      | DATE      | Actual delivery date of the order. |

---------------------------------------------------------------------------------------------

# ⚙️ Notes

* All entities in the **Gold Layer** rely exclusively on **Silver Layer** models as upstream sources.  
* The Gold Layer contains **denormalized**, **business-friendly**, and **ready-to-query** datasets.  
* These models are designed for **BI tools**, **KPI dashboards**, **ad-hoc analytics**, and **trend analysis**.  
* Fact tables represent events or transactions, while dimension tables store descriptive attributes.  
* All datasets ensure **cleaning**, **type standardization**, and **referential consistency** inherited  
  from the Silver Layer transformations.

---------------------------------------------------------------------------------------------
