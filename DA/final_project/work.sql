/*with
 ----- trunc payment_date -----
 trunc_data as (
 select
 user_id,
 cast(date_trunc('month', payment_date) as date) as payment_month,
 revenue_amount_usd
 from project.games_payments
 ),
 ----- group payments by month -----
 group_data as (
 select
 user_id,
 sum(revenue_amount_usd) as revenue,
 payment_month
 from trunc_data
 group by
 user_id,
 payment_month
 order by
 user_id,
 payment_month
 ),
 */
with ----- trunc payment_date, group by month payment sum -----
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
    /*
     select
     user_id,
     cast(date_trunc('month', max(payment_month)+ INTERVAL '1 MONTH') as date) as next_month
     from group_data
     group by user_id
     */
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
----- join next month date -----
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
----- coalesce null data -----
coalesce_data as (
    select
        coalesce(gd_user_id, nd_user_id) as user_id,
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
        --cast(date_trunc('month', (max(payment_month) over (PARTITION BY cd.user_id))) as date) as churn_month, ----------------------------
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
        ----
        --last_value(revenue) over (PARTITION BY cd.user_id order by payment_month) as last_rev,-----
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
    order by cd.user_id, payment_month
),
final_data as (
select user_id,
    revenue,
    payment_month,
    engagement_month,
    churn_month,
    last_month,
    abs(date_part('month', age(payment_month, lag(payment_month) over (partition by user_id)))) as month_gap, -- розрив між місяцями для пошуку Back from churn
    case -- знаходимо ревен'ю нових юзерів
        when payment_month = engagement_month then revenue
        else 0
    end new_user_revenue,
    case
        when abs(date_part('month', age(payment_month, lag(payment_month) over (partition by user_id)))) > 1
        then revenue
        else 0
    end back_from_churn_revenue,
    case
        when (revenue > (lag(revenue, 1, revenue) over (PARTITION BY user_id order by payment_month))) and (abs(
    date_part(
        'month',
        age(
            payment_month,
            lag(payment_month) over (partition by user_id)
        )
    )
) = 1) and coalesce(payment_month != churn_month, true)
        then revenue - (lag(revenue, 1, revenue) over (PARTITION BY user_id order by payment_month))
        else 0
    end expansion_revenue,
    case
        when (revenue < (lag(revenue, 1, revenue) over (PARTITION BY user_id order by payment_month))) and (abs(
    date_part(
        'month',
        age(
            payment_month,
            lag(payment_month) over (partition by user_id)
        )
    )
) = 1) and coalesce(payment_month != churn_month, true)
        then revenue - (lag(revenue, 1, revenue) over (PARTITION BY user_id order by payment_month))
        else 0
    end contraction_revenue,
    case
        when payment_month = churn_month
        then (revenue - lag(revenue, 1, revenue) over (PARTITION BY user_id order by payment_month))
        else 0
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
                coalesce(churn_month, last_month)
            )
        )
    ) as lifetime_in_month -- if churn_month is null then LT calculate to last month in data
from user_data ud
)
select
    user_id,
    payment_month,
    engagement_month,
    churn_month,
    revenue,
    month_gap,---
    new_user_revenue,
    back_from_churn_revenue,
    expansion_revenue,
    contraction_revenue,
    churn_revenue,
    new_user_revenue + back_from_churn_revenue + expansion_revenue + contraction_revenue + churn_revenue as revenue_changes,
    case
        when new_user_revenue != 0 then 'new user revenue'
        when back_from_churn_revenue != 0 then 'back from churn revenue'
        when expansion_revenue != 0 then 'expansion revenue'
        when contraction_revenue != 0 then 'contraction revenue'
        when churn_revenue != 0 then 'churn revenue'
    end revenue_change_factors,
    game_name,
    final_data.language,
    has_older_device_model,
    final_data.age,
    lifetime_in_month
from final_data
--where back_from_churn_revenue < 0
--where user_id = '14RMzQMoG017OLkaDBFjng =='
-- '06RPvlsFPCmM9ag+iSM/Ag==' churn
    -- '2FYEYdoJMUOtbJ6TruruWA==' not churn



    