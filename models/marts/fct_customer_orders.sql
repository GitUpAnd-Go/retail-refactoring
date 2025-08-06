-- import CTEs
With 
    customers as (
        select * from {{ source('jaffle_shop', 'customers') }}
    ),
    orders as (
        select * from {{ source('jaffle_shop', 'orders') }}
    ),
    payments as (
        select * from {{ source('stripe', 'payments') }}
    ),
-- logical CTEs
    successfull_payments as 
    (
    select 
        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
    ),
    customer_orders as 
        (
        select customers.id as customer_id
            , min(order_date) as first_order_date
            , max(order_date) as most_recent_order_date
            , count(orders.id) as number_of_orders
        from customers 
        left join orders
        on orders.user_id = customers.id 
        group by 1
        ),
    paid_orders as 
        (
            select orders.id as order_id,
                orders.user_id	as customer_id,
                orders.order_date as order_placed_at,
                orders.status as order_status,
                employees.employee_id  is not null as is_employee,
                successfull_payments.total_amount_paid,
                successfull_payments.payment_finalized_date,
                customers.first_name    as customer_first_name,
                customers.last_name as customer_last_name
            from orders
            left join successfull_payments
            on orders.id = successfull_payments.order_id
            left join customers  
            on orders.user_id = customers.id 
            left join employees on (customer_id)
    ),

-- Final CTE
    final_cte as (
    select payments.*,
        --Customer Transaction Sequence
        row_number() over (order by payments.order_id) as transaction_seq,
        -- Customer Sales Sequence
        row_number() over (partition by customer_id order by payments.order_id) as customer_sales_seq,
        -- New or Returning Customer
        case when (
            rank() over (
                partition by customer_id
                order by order_date,order_id
            ) =1 )then 'new'
        else 'return' end as nvsr,
        --Customer Lifetime Value
        sum(total_amount_paid) over (
            partition by customer_id
            order by order_placed_at
        ) as customer_lifetime_value,
    -- first day of sale
        first_value(paid_orders.order_placed_at) 
            over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at
            ) as fdos
    from paid_orders
    )



select * from final_cte