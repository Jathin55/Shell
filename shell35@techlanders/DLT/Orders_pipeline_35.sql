-- Databricks notebook source
CREATE OR REPLACE STREAMING TABLE orders_bronze_35
COMMENT "order bronze table_35"
TBLPROPERTIES ("quality"="bronze")
AS SELECT current_timestamp() as processing_time, input_file_name() as source_file, * FROM cloud_files("${path}Orders","csv", map("cloudFiles.inferColumnTypes","True"))

-- COMMAND ----------

CREATE STREAMING TABLE orders_silver_35
(CONSTRAINT valid_orderid EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
COMMENT "ALLOWING only valid orderid"
TBLPROPERTIES ("quality"="silver")
AS SELECT * EXCEPT(processing_time,source_file,_rescued_data ) FROM STREAM(LIVE.orders_bronze_35)

-- COMMAND ----------

CREATE LIVE TABLE count_product_35
as 
SELECT product, count(product) count, sum(quantity) total_quantity, sum(amount) total_amount FROM LIVE.orders_silver_35
GROUP BY product
