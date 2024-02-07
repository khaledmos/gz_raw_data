WITH join_ship AS
(
    SELECT
margine.*
,ship.shipping_fee
,ship.logCost
,ship.ship_cost
FROM {{ ref("int_orders_margin")}} AS margine
join {{ ref("stg_raw__ship")}} AS ship 
USING (orders_id)
)

SELECT orders_id
,date_date
,CAST(revenue AS float64) AS revenue
,quantity
,CAST(purchase_cost AS float64) AS purchase_cost
,CAST(margin AS float64) AS margin
,CAST(shipping_fee AS float64) AS shipping_fee
,CAST(logcost AS float64) AS logcost
,CAST(ship_cost AS float64) AS ship_cost
,round(margin + shipping_fee - logcost - (CAST (ship_cost as float64)),2) AS operational_margin
FROM join_ship





--Operational margin = margin + shipping_fee - log_cost - cast(ship_cost as float64)

