with
    purchase_subq as (
        select
            sales.*,
            
                round(quantity * (cast(purchase_price as float64)), 2) as purchase_cost,
                products.purchase_price
                from {{ ref("stg_raw__sales") }} as sales
                left join {{ ref("stg_raw__product") }} as products using (products_id)
            )

,purchase_fl_subq as(
select *,(CAST (purchase_cost as float64)) as purchase_cost_fl
from purchase_subq
    )

        select purchase_subq.*, (round(revenue-purchase_cost_fl), 2) as margin
        from purchase_fl_subq