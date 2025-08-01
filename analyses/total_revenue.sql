with payments as (
    select * from {{ source('stripe', 'payments') }}
),
    aggregated as (
        select sum(amounts) as total_revenue from payments where status='success'
    )

select * from aggregated