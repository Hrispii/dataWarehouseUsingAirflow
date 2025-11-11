-- models/gold_layer/dim_products.sql

select
    p.product_id,
    i.warehouse_id,

    p.name as product_name,
    p.category as product_category,
    p.brand,
    p.price as product_price,
    p.currency,

    i.stock as product_amount,

    w.name as warehouse_name,
    w.location as warehouse_location,

    r.rating as users_rating,
    r.comment as users_comments

from {{ ref('products') }} p
join {{ ref('inventory') }} i
    on p.product_id = i.product_id
join {{ ref('warehouses') }} w
    on i.warehouse_id = w.warehouse_id
left join {{ ref('reviews') }} r
    on p.product_id = r.product_id
