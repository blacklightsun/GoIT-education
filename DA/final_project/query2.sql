with joined_data as (
select
    pgp.user_id,
    pgp.game_name,
    pgp.payment_date,
    pgp.revenue_amount_usd,
    pgpu.language,
    pgpu.has_older_device_model,
    pgpu.age,
    min(pgp.payment_date) over (PARTITION BY pgp.user_id) as engagement_date,
    max(pgp.payment_date) over (PARTITION BY pgp.user_id) as churn_date,
    1,
    max(pgp.payment_date) over (PARTITION BY 1) as last_date
from project.games_payments AS pgp
LEFT JOIN project.games_paid_users AS pgpu ON pgp.user_id = pgpu.user_id
),
trunced_data as (
SELECT
    user_id,
    game_name,
    date_trunc('month', payment_date) as payment_month,
    revenue_amount_usd,
    joined_data.language,
    has_older_device_model,
    age,
    date_trunc('month', engagement_date) as engagement_month,
    date_trunc('month', churn_date) as churn_month,
    date_trunc('month', last_date) as last_month
from joined_data
), -- 3026
grouped_data as (
select
    user_id,
    game_name,
    payment_month,
    sum(revenue_amount_usd) as revenue,
    trunced_data.language,
    has_older_device_model,
    age,
    engagement_month,
    churn_month,
    last_month
from trunced_data
GROUP BY
    user_id,
    game_name,
    payment_month,
    trunced_data.language,
    has_older_device_model,
    age,
    engagement_month,
    churn_month,
    last_month
) --1392
SELECT
    user_id,
    game_name,
    payment_month,
    revenue,
    grouped_data.language,
    has_older_device_model,
    age,
    engagement_month,
    churn_month,
    last_month,
    case 
        WHEN payment_month = engagement_month and payment_month = churn_month and churn_month != last_month THEN 'new-churn'
        WHEN payment_month = engagement_month and payment_month = churn_month and churn_month = last_month THEN 'new'
        WHEN payment_month = engagement_month and payment_month != churn_month THEN 'new'
        WHEN payment_month != engagement_month and payment_month = churn_month and churn_month != last_month THEN 'churn'
        WHEN payment_month != engagement_month and payment_month = churn_month and churn_month = last_month THEN 'regular'
        WHEN payment_month != engagement_month and payment_month != churn_month THEN 'regular'
    end user_status,
    round((extract('month' from churn_month) - extract('month' from engagement_month) + 1),0) as lifetime_in_month
from grouped_data
--where user_id = 'DqezK7HAqZ+GovyRuzxD1A==' -- for query testing