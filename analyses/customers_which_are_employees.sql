with customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),
    employees as (
        select * from {{ ref('employees_tab') }}
    )
select customers.customer_first_name,
    customers.customer_last_name,
    employee.employee_id is not null as is_employee,
    customers.customer_id
from customers 
left join employees on customers.customer_id=employee.employee_id