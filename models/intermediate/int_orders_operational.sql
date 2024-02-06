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
,revenue
,quantity
,purchase_cost
,margin
,round(margin + shipping_fee - logcost - (CAST (ship_cost as float64)),2) AS operational_margin
FROM join_ship





--Operational margin = margin + shipping_fee - log_cost - cast(ship_cost as float64)

