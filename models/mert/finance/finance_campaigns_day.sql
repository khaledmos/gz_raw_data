WITH joint AS (
SELECT 
finance. *
,campaigns. ads_cost, ads_impression, ads_clicks
FROM {{ ref("finance_days")}} AS finance
INNER JOIN {{ ref("int_campaigns_day")}} AS campaigns
USING (date_date)
)

SELECT date_date,
(operational_margin - ads_cost) AS ads_margin,
revenue,
average_basket,
margin,
operational_margin,
purchase_cost,
shipping_fee,
logcost,
ship_cost,
quantity,
ads_cost,
ads_impression,
ads_clicks
FROM joint
ORDER BY date_date ASC
