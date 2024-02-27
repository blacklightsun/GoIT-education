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
)
SELECT
    user_id,
    game_name,
    payment_month,
    revenue,
    grouped_data.language,
    has_older_device_model,
    age,
    engagement_month,
    case
        when churn_month > last_month then NULL
        else churn_month
    end mod_churn_month,
    --churn_month, -- for query testing
    --last_month, -- for query testing
    round((extract('month' from churn_month) - extract('month' from engagement_month)),0) as lifetime_in_month
from grouped_data
--where user_id = '1BBMagZRq/39AV1l2rm4Iw==' -- for query testing
--where round((extract('month' from churn_month) - extract('month' from engagement_month)),0) = 0 -- for query testing
order by user_id, payment_month