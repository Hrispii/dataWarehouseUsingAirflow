
select 
    oi.order_id,
    oi.product_id,
    oi.quantity as quantity_of_purchased,
    oi.unit_price,
    oi.total_price,

    o.payment_method,
    p.currency,
    o.status as status_of_order,
    o.order_date,

    s.carrier as carrier_company,
    s.status as shipment_status,
    s.delivery_date

from {{ ref('order_items') }} oi
join {{ ref('orders') }} o
    on oi.order_id = o.order_id
join {{ ref('shipments') }} s
    on oi.order_id = s.order_id
join {{ ref('payments') }} p
    on oi.order_id = p.order_id