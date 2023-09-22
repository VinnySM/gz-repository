{{ config(schema='transaction') }}

WITH 

  --sales AS (SELECT * FROM `gz_raw_data.raw_gz_sales`)
  sales AS (SELECT * FROM {{ ref('stg_sales') }} )

  --,product AS (SELECT * FROM `gz_raw_data.raw_gz_product`)
  ,product AS (SELECT * FROM {{ ref('stg_product') }} )

SELECT
  s.date_date
  ### Key ###
  ,s.orders_id
  ,s.products_id
  ###########
  -- qty --
  ,s.qty
  -- revenue --
  ,s.turnover
  -- cost --
  ,purchase_price
	,ROUND(s.qty*purchase_price,2) AS purchase_cost
	-- margin --
	--,s.revenue - s.quantity*CAST(p.purchSE_PRICE AS FLOAT64) AS product_margin
    ,
   ROUND(s.turnover - s.qty*purchase_price , 2) AS product_margin
	,ROUND(s.turnover-s.qty*purchase_price,2) AS margin
    ,
   ROUND( SAFE_DIVIDE( (s.turnover - s.qty*purchase_price) , s.turnover ) , 2) AS product_margin_percent
FROM sales s
INNER JOIN product p ON s.products_id = p.products_id