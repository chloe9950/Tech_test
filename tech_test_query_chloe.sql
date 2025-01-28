WITH 
-- Calculate daily volume per user, server, symbol, and currency
day_trade AS (
    SELECT 
        CASE 
            WHEN DATE(close_time) >= '2021-01-01' THEN DATE(close_time) - INTERVAL '2 years' -- To correct the strange close time (the close time happened two years after open time)
            ELSE DATE(close_time) 
        END AS dt_report, 
        login_hash, 
        server_hash, 
        symbol, 
        currency, 
        SUM(volume) AS volume_day,  -- Sum volume for the day
        ROW_NUMBER() OVER (ORDER BY DATE(close_time), login_hash, server_hash, symbol) AS row_number
    FROM 
        trades t
        NATURAL JOIN users u
    WHERE 
        u.enable = 1  -- Only include enabled users
        AND DATE(close_time) < '2020-10-01' 
        AND DATE(close_time) > '2020-05-31' -- Report for Jun, Jul, Aug, Sep
    GROUP BY 
        DATE(close_time), login_hash, server_hash, symbol, currency
),

-- Count daily trades per user
day_trade_by_login AS (
    SELECT   
        CASE 
            WHEN DATE(close_time) >= '2021-01-01' THEN DATE(close_time) - INTERVAL '2 years'
            ELSE DATE(close_time) 
        END AS dt_report, 
        login_hash, 
        COUNT(ticket_hash) AS trade_day  -- Count trades per day
    FROM 
        trades t
        NATURAL JOIN users u
    WHERE 
        u.enable = 1
        AND DATE(close_time) < '2020-10-01' 
        AND DATE(close_time) > '2020-05-31'
    GROUP BY 
        DATE(close_time), login_hash
),

-- Rank volume by user and symbol over the previous 7 days
volume_rank AS (
    SELECT 
        dt_main.dt_report AS dt_report_main,
        dt_sub.dt_report AS dt_report_sub,
        dt_main.login_hash, 
        dt_main.server_hash, 
        dt_main.symbol, 
        dt_main.volume_day,
        dt_main.currency,
        dt_main.row_number,
        DENSE_RANK() OVER (
            PARTITION BY dt_main.login_hash, dt_main.server_hash, dt_main.symbol, dt_main.dt_report
            ORDER BY dt_sub.volume_day DESC
        ) AS rank_volume_symbol_prev_7d  -- Rank volume within the previous 7 days
    FROM 
        day_trade AS dt_main
    LEFT JOIN 
        day_trade AS dt_sub
    ON 
        dt_main.login_hash = dt_sub.login_hash
        AND dt_main.server_hash = dt_sub.server_hash
        AND dt_main.symbol = dt_sub.symbol
        AND dt_sub.dt_report BETWEEN dt_main.dt_report - INTERVAL '6 days' AND dt_main.dt_report
),

-- Calculate rolling sum of volumes over the previous 7 days and all past days
sum_volume AS (
    SELECT 
        dt_report, 
        login_hash, 
        server_hash, 
        symbol, 
        currency,
        SUM(SUM(volume_day)) OVER (
            PARTITION BY login_hash, server_hash, symbol 
            ORDER BY dt_report 
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
        ) AS sum_volume_prev_7d,  -- Rolling sum over 7 days
        SUM(SUM(volume_day)) OVER (
            PARTITION BY login_hash, server_hash, symbol 
            ORDER BY dt_report 
            ROWS UNBOUNDED PRECEDING
        ) AS sum_volume_prev_all  -- Sum of all previous days
    FROM 
        day_trade
    GROUP BY 
        dt_report, login_hash, server_hash, symbol, currency
),

-- Rank the number of trades over the previous 7 days
trade_rank AS (
    SELECT 
        dt_main.dt_report AS dt_report_trade_main,
        dt_sub.dt_report AS dt_report_trade_sub,
        dt_main.login_hash, 
        dt_main.trade_day,
        DENSE_RANK() OVER (
            PARTITION BY dt_main.login_hash, dt_main.dt_report
            ORDER BY dt_sub.trade_day DESC
        ) AS rank_trade_prev_7d  -- Rank trades count within the previous 7 days
    FROM 
        day_trade_by_login AS dt_main
    LEFT JOIN 
        day_trade_by_login AS dt_sub
    ON 
        dt_main.login_hash = dt_sub.login_hash
        AND dt_sub.dt_report BETWEEN dt_main.dt_report - INTERVAL '6 days' AND dt_main.dt_report
),

-- Calculate cumulative volume in August 2020
sum_volume_August AS (
    SELECT 
        dt_report, 
        login_hash, 
        server_hash, 
        symbol, 
        currency,
        SUM(SUM(volume_day)) OVER (
            PARTITION BY login_hash, server_hash, symbol 
            ORDER BY dt_report 
            ROWS UNBOUNDED PRECEDING
        ) AS sum_volume_2020_08  -- Cumulative sum for August 2020
    FROM 
        day_trade
    WHERE 
        EXTRACT(MONTH FROM dt_report) = 8 
        AND EXTRACT(YEAR FROM dt_report) = 2020
    GROUP BY 
        dt_report, login_hash, server_hash, symbol, currency
),

-- Find the first trade date per user, server, and symbol
first_trade AS (
    SELECT 
        login_hash, 
        server_hash, 
        symbol, 
        MIN(dt_report) AS date_first_trade  -- Earliest trade date
    FROM 
        day_trade
    GROUP BY 
        login_hash, server_hash, symbol
)

-- Final selection with all calculated fields
SELECT  
    sv.dt_report::date,
    sv.login_hash, 
    sv.server_hash, 
    sv.symbol, 
    sv.currency,
    sum_volume_prev_7d,
    sum_volume_prev_all,
    rank_volume_symbol_prev_7d,
    rank_trade_prev_7d,
    sum_volume_2020_08,
    date_first_trade,
    row_number
FROM 
    volume_rank vr
JOIN 
    sum_volume sv 
    ON sv.dt_report = vr.dt_report_main 
    AND sv.login_hash = vr.login_hash 
    AND sv.symbol = vr.symbol
JOIN 
    trade_rank tr 
    ON tr.dt_report_trade_main = vr.dt_report_main 
    AND tr.login_hash = vr.login_hash
LEFT JOIN 
    sum_volume_August sva 
    ON sva.dt_report = vr.dt_report_main 
    AND sva.login_hash = vr.login_hash 
    AND sva.symbol = vr.symbol
JOIN 
    first_trade ft 
    ON ft.login_hash = vr.login_hash 
    AND ft.symbol = vr.symbol 
    AND ft.server_hash = vr.server_hash
WHERE 
    dt_report_main = dt_report_sub 
    AND dt_report_trade_main = dt_report_trade_sub
ORDER BY 
    row_number DESC;