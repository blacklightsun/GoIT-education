/*
 Концепт такий:
 Для факторного аналіза ревен'ю треба розрахувати різниці суми доходу поточного та попереднього місяця.
 Тому для churned users треба додати рядок з сумою = 0 та періодом = місяць останнього платежа +1.
 Якщо ж юзер живий до останнього місяця, то новий рядок не додаємо.
 */
with ----- агрегуємо ревенью помісячно, додаємо стовпець для розрахунку останнього місяця датасету (який не потрібно вважати за відтік юзерів) -----
trunc_data as (
    select user_id,
        cast(date_trunc('month', payment_date) as date) as payment_month,
        1 as all_records_mark,
        sum(revenue_amount_usd) as revenue
    from project.games_payments
    group by user_id,
        payment_month,
        all_records_mark
    order by user_id,
        payment_month
),
----- add last month in data -----
group_data as (
    select user_id,
        revenue,
        max(payment_month) over (partition by all_records_mark) as last_month,
        ---------------------------
        payment_month
    from trunc_data
),
------------------------------------------------------------------------------------------------------------------------------------------------------
next_data as (
    ------ make record with next month date after last payment for each users -----
    select user_id,
        --max(payment_month) as max_payment_month, -- for debugging
        --max(last_month) as last_month, -- for debugging
        case
            when max(payment_month) < max(last_month) then cast(
                date_trunc('month', max(payment_month) + INTERVAL '1 MONTH') as date
            )
            else cast(date_trunc('month', max(payment_month)) as date)
        end next_month
    from group_data
    group by user_id --order by user_id, max_payment_month -- for debugging
),
----- add string with next month date to main table-----
join_next_data as (
    select gd.user_id as gd_user_id,
        nd.user_id as nd_user_id,
        gd.revenue,
        gd.payment_month,
        nd.next_month,
        gd.last_month
    from group_data gd
        full join next_data nd on (gd.user_id = nd.user_id)
        and (gd.payment_month = nd.next_month)
),
----- coalesce null data in joined data-----
coalesce_data as (
    select coalesce(gd_user_id, nd_user_id) as user_id,
        coalesce(revenue, 0) as revenue,
        coalesce(payment_month, next_month) as payment_month
    from join_next_data
    order by user_id,
        payment_month
),
----- join user data, add engagement month, churn month, last month in data -----
user_data as (
    select cd.user_id,
        revenue,
        payment_month,
        cast(
            min(payment_month) over (PARTITION BY cd.user_id) as date
        ) as engagement_month,
        case
            --- if payment is in last month of data then user don't churned
            when (
                last_value(revenue) over (PARTITION BY cd.user_id)
            ) = 0 ----
            then cast(
                date_trunc(
                    'month',
                    (
                        max(payment_month) over (PARTITION BY cd.user_id)
                    )
                ) as date
            ) ----
            else null ----
        end churn_month,
        1,
        cast(
            date_trunc(
                'month',
                max(payment_month) over (PARTITION BY 1)
            ) as date
        ) as last_month,
        game_name,
        pgpu.language,
        has_older_device_model,
        pgpu.age
    from coalesce_data cd
        left join project.games_paid_users pgpu on (cd.user_id = pgpu.user_id)
    order by cd.user_id,
        payment_month
),
final_data as (
    select user_id,
        revenue,
        payment_month,
        engagement_month,
        churn_month,
        last_month,
        case
            -- знаходимо ревен'ю нових юзерів
            when payment_month = engagement_month then revenue
            else NULL
        end new_user_revenue,
        case
            -- знаходимо експаншн ревен'ю
            when (
                revenue > (
                    lag(revenue, 1, revenue) over (
                        PARTITION BY user_id
                        order by payment_month
                    )
                )
            )
            and coalesce(payment_month != churn_month, true) then revenue - (
                lag(revenue, 1, revenue) over (
                    PARTITION BY user_id
                    order by payment_month
                )
            )
            else NULL
        end expansion_revenue,
        case
            -- знаходимо контракшн ревен'ю
            when (
                revenue < (
                    lag(revenue, 1, revenue) over (
                        PARTITION BY user_id
                        order by payment_month
                    )
                )
            )
            and coalesce(payment_month != churn_month, true) then revenue - (
                lag(revenue, 1, revenue) over (
                    PARTITION BY user_id
                    order by payment_month
                )
            )
            else NULL
        end contraction_revenue,
        case
            -- знаходимо черн ревен'ю
            when payment_month = churn_month then (
                revenue - lag(revenue, 1, revenue) over (
                    PARTITION BY user_id
                    order by payment_month
                )
            )
            else NULL
        end churn_revenue,
        game_name,
        ud.language,
        has_older_device_model,
        ud.age,
        abs(
            date_part(
                'month',
                age(
                    engagement_month,
                    coalesce(churn_month, (last_month + INTERVAL '1 MONTH'))
                )
            )
        ) as lifetime_in_month -- if churn_month is null then LT calculate to last month in data + 1 month
    from user_data ud
)
select user_id,
    payment_month,
    engagement_month,
    churn_month,
    revenue,
    new_user_revenue,
    expansion_revenue,
    contraction_revenue,
    churn_revenue,
    coalesce(new_user_revenue, 0) + coalesce(expansion_revenue, 0) + coalesce(contraction_revenue, 0) + coalesce(churn_revenue, 0) as revenue_changes,
    case
        when new_user_revenue is not NULL then 'new user revenue'
        when expansion_revenue is not NULL then 'expansion revenue'
        when contraction_revenue is not NULL then 'contraction revenue'
        when churn_revenue is not NULL then 'churn revenue'
        else 'without changes'
    end revenue_change_factors,
    game_name,
    final_data.language,
    has_older_device_model,
    final_data.age,
    lifetime_in_month
from final_data
-- where user_id = '06RPvlsFPCmM9ag+iSM/Ag==' -- for debugging
    -- '06RPvlsFPCmM9ag+iSM/Ag==' churn -- for debugging
    -- '2FYEYdoJMUOtbJ6TruruWA==' not churn -- for debugging
    -- 'oQQmPjqm7DR+exPmp6xDhg==' with same value of revenue -- for debugging