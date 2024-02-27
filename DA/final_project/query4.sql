with joined_data as (
select
    pgp.user_id,
    pgp.game_name,
    cast(date_trunc('month', pgp.payment_date) as date) as payment_month,
    pgp.revenue_amount_usd,
    pgpu.language,
    pgpu.has_older_device_model,
    pgpu.age,
    cast(date_trunc('month', min(pgp.payment_date) over (PARTITION BY pgp.user_id)) as date) as engagement_month,
    cast(date_trunc('month', (max(pgp.payment_date) over (PARTITION BY pgp.user_id)) + INTERVAL '1 MONTH') as date) as churn_month,
    1,
    cast(date_trunc('month', max(pgp.payment_date) over (PARTITION BY 1)) as date) as last_month
from project.games_payments AS pgp
LEFT JOIN project.games_paid_users AS pgpu ON pgp.user_id = pgpu.user_id
),
grouped_data as (
SELECT
    user_id,
    game_name,
    payment_month,
    sum(revenue_amount_usd) as revenue,
    joined_data.language,
    has_older_device_model,
    age,
    engagement_month,
    churn_month,
    last_month
from joined_data
GROUP BY
    user_id,
    game_name,
    payment_month,
    joined_data.language,
    has_older_device_model,
    age,
    engagement_month,
    churn_month,
    last_month
),
next_data as (
select
    user_id,
    payment_month + interval '1 month' as next_month,
    revenue
from grouped_data
)
SELECT
    gd.user_id,
    gd.revenue,
    gd.payment_month,
    nd.next_month,
    nd.revenue,
    gd.engagement_month,
    --churn_month, -- for query testing
    --last_month, -- for query testing
    case
        when gd.churn_month > gd.last_month then NULL
        else gd.churn_month
    end mod_churn_month,
    extract('month' from gd.churn_month) - extract('month' from gd.engagement_month) as lifetime_in_month, -- is formula right?
    --
    extract('month' from gd.payment_month) - extract('month' from (lag(gd.payment_month, 1, gd.payment_month) over (PARTITION BY gd.user_id order by gd.payment_month))) as month_gap,
    --
    gd.revenue - lag(gd.revenue, 1, gd.revenue) over (PARTITION BY gd.user_id order by gd.payment_month) as revenue_change,
    --  
    case
        when gd.payment_month = gd.engagement_month then gd.revenue
        else 0
    end new_user_revenue,
    --
    nd.revenue as churn_revenue,
    case
        when (gd.revenue > lag(gd.revenue, 1, gd.revenue) over (PARTITION BY gd.user_id order by gd.payment_month)) and (extract('month' from gd.payment_month) - extract('month' from (lag(gd.payment_month, 1, gd.payment_month) over (PARTITION BY gd.user_id order by gd.payment_month))) <= 1) then (gd.revenue - lag(gd.revenue, 1, gd.revenue) over (PARTITION BY gd.user_id order by gd.payment_month))
        else 0
    end expansion_revenue,
    --
    case
        when gd.revenue < lag(gd.revenue, 1, gd.revenue) over (PARTITION BY gd.user_id order by gd.payment_month) and (extract('month' from gd.payment_month) - extract('month' from (lag(gd.payment_month, 1, gd.payment_month) over (PARTITION BY gd.user_id order by gd.payment_month))) <= 1) then (gd.revenue - lag(gd.revenue, 1, gd.revenue) over (PARTITION BY gd.user_id order by gd.payment_month))
        else 0
    end contraction_revenue,
    --
    case
        when (extract('month' from gd.payment_month) - extract('month' from (lag(gd.payment_month, 1, gd.payment_month) over (PARTITION BY gd.user_id order by gd.payment_month))) > 1) then (gd.revenue - lag(gd.revenue, 1, gd.revenue) over (PARTITION BY gd.user_id order by gd.payment_month))
        else 0
    end back_from_churn,
    gd.language,
    gd.has_older_device_model,
    gd.age,
    gd.game_name
from grouped_data as gd
inner join next_data as nd on (gd.user_id = nd.user_id) -- and (gd.payment_month = nd.next_month) 
where gd.user_id = '1BBMagZRq/39AV1l2rm4Iw==' -- for query testing
--where round((extract('month' from churn_month) - extract('month' from engagement_month)),0) = 0 -- for query testing
order by gd.user_id, gd.payment_month