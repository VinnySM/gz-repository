{{ config(schema='transaction') }}

WITH 

  sales AS (SELECT * FROM `gz_raw_data.raw_gz_sales`)

  ,product AS (SELECT * FROM `gz_raw_data.raw_gz_product`)

SELECT
  s.date_date
  ### Key ###
  ,s.orders_id
  ,s.pdt_id AS products_id
  ###########
	-- qty --
	,s.quantity AS qty
  -- revenue --
  ,s.revenue AS turnover
  -- cost --
  ,CAST(p.purchSE_PRICE AS FLOAT64) AS purchase_price
	,ROUND(s.quantity*CAST(p.purchSE_PRICE AS FLOAT64),2) AS purchase_cost
	-- margin --
<<<<<<< HEAD
	,s.revenue - s.quantity*CAST(p.purchSE_PRICE AS FLOAT64) AS product_margin
=======
	,ROUND(s.revenue-s.quantity*CAST(p.purchSE_PRICE AS FLOAT64),2) AS margin
>>>>>>> b89cb69ae676b8c0385a3eb1ba328a4c9f988a78
FROM sales s
INNER JOIN product p ON s.pdt_id = p.products_id