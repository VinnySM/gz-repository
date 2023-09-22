{{ config(schema='campaigns') }}
{{ config(materialized='table') }}
WITH 
  adwords AS (SELECT * FROM {{ ref('stg_adwords') }} )
  ,bing AS (SELECT * FROM {{ ref('stg_bing') }} )
  ,criteo AS (SELECT * FROM {{ ref('stg_criteo') }} )
  ,facebook AS (SELECT * FROM {{ ref('stg_facebook') }}) 
SELECT * 
FROM adwords
UNION ALL
SELECT * 
FROM bing
UNION ALL
SELECT * 
FROM criteo
UNION ALL
SELECT * 
FROM facebook
