with
    datemonth as (
        SELECT 
        date_trunc(date_date, month) as date_month,
        *
        from {{ ref("finance_campaigns_day") }}
    )

select
    date_month,
    sum(ads_margin) as ads_margin,
    sum(revenue) as revenue,
    ROUND(sum(average_basket),1) as average_basket,
    sum(margin) as margin,
    sum(operational_margin) as operational_margin,
    sum(purchase_cost) as purchase_cost,
    sum(shipping_fee) as shipping_fee,
    sum(logcost) as logcost,
    sum(ship_cost) as ship_cost,
    sum(quantity) as quantity,
    sum(ads_cost) as ads_cost,
    sum(ads_impression) as ads_impression,
    sum(ads_clicks) as ads_clicks
from datemonth
group by 1