-- ============================================================
-- ADVANCED SQL QUERIES
-- CTEs, Window Functions, Analytical Queries
-- ============================================================

-- ──────────────────────────────────────
-- 1. ON-TIME PERFORMANCE WITH RUNNING TOTALS
-- ──────────────────────────────────────
WITH monthly_stats AS (
    SELECT
        d.year,
        d.month_num,
        d.month_name,
        a.airline_code,
        a.airline_name,
        COUNT(*)                                            AS total_flights,
        SUM(CASE WHEN f.arr_delay_min > 15 THEN 1 ELSE 0 END) AS delayed_flights,
        SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END)    AS cancelled_flights,
        AVG(f.arr_delay_min)                                AS avg_delay,
        SUM(f.total_revenue)                                AS revenue
    FROM fact_flight f
    JOIN dim_date    d ON f.date_key    = d.date_key
    JOIN dim_airline a ON f.airline_key = a.airline_key
    WHERE d.year = 2024
    GROUP BY 1,2,3,4,5
),
ranked AS (
    SELECT *,
        ROUND(100.0 * (total_flights - delayed_flights - cancelled_flights) / total_flights, 2)
                                        AS on_time_pct,
        -- Running total of revenue YTD per airline
        SUM(revenue) OVER (
            PARTITION BY airline_code
            ORDER BY month_num
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                               AS revenue_ytd,
        -- Month-over-month delay change
        LAG(avg_delay, 1) OVER (
            PARTITION BY airline_code ORDER BY month_num
        )                               AS prev_month_delay,
        avg_delay - LAG(avg_delay, 1) OVER (
            PARTITION BY airline_code ORDER BY month_num
        )                               AS delay_mom_change,
        -- Rank airlines by on-time performance each month
        RANK() OVER (
            PARTITION BY month_num
            ORDER BY (total_flights - delayed_flights - cancelled_flights) / total_flights DESC
        )                               AS monthly_rank
    FROM monthly_stats
)
SELECT * FROM ranked ORDER BY year, month_num, monthly_rank;


-- ──────────────────────────────────────
-- 2. DELAY ROOT CAUSE ANALYSIS WITH RECURSIVE CTE
-- ──────────────────────────────────────
WITH RECURSIVE delay_chain AS (
    -- Base: flights delayed more than 15 min by late aircraft
    SELECT
        f.flight_number,
        f.date_key,
        a.airline_code,
        ap_o.iata_code  AS origin,
        ap_d.iata_code  AS dest,
        f.late_aircraft_delay,
        f.arr_delay_min,
        1               AS chain_level,
        f.flight_number AS root_flight
    FROM fact_flight f
    JOIN dim_airline a           ON f.airline_key        = a.airline_key
    JOIN dim_airport ap_o        ON f.origin_airport_key = ap_o.airport_key
    JOIN dim_airport ap_d        ON f.dest_airport_key   = ap_d.airport_key
    WHERE f.late_aircraft_delay > 30
      AND f.date_key BETWEEN 20240101 AND 20241231

    UNION ALL

    -- Recursive: follow the aircraft to its next flight
    SELECT
        f2.flight_number,
        f2.date_key,
        a.airline_code,
        ap_o.iata_code,
        ap_d.iata_code,
        f2.late_aircraft_delay,
        f2.arr_delay_min,
        dc.chain_level + 1,
        dc.root_flight
    FROM fact_flight f2
    JOIN delay_chain dc          ON f2.aircraft_key = (
                                        SELECT aircraft_key FROM fact_flight
                                        WHERE flight_number = dc.flight_number
                                        LIMIT 1)
    JOIN dim_airline a           ON f2.airline_key        = a.airline_key
    JOIN dim_airport ap_o        ON f2.origin_airport_key = ap_o.airport_key
    JOIN dim_airport ap_d        ON f2.dest_airport_key   = ap_d.airport_key
    WHERE dc.chain_level < 5     -- Limit recursion depth
)
SELECT
    root_flight,
    MAX(chain_level)        AS cascade_depth,
    SUM(arr_delay_min)      AS total_system_delay_min,
    COUNT(DISTINCT flight_number) AS impacted_flights
FROM delay_chain
GROUP BY root_flight
HAVING MAX(chain_level) > 2
ORDER BY total_system_delay_min DESC;


-- ──────────────────────────────────────
-- 3. PASSENGER VALUE SEGMENTATION (RFM)
-- ──────────────────────────────────────
WITH rfm_base AS (
    SELECT
        p.passenger_id,
        p.first_name || ' ' || p.last_name AS passenger_name,
        p.loyalty_tier,
        MAX(d.full_date)        AS last_flight_date,
        COUNT(r.reservation_key)AS frequency,
        SUM(r.total_fare)       AS monetary,
        CURRENT_DATE - MAX(d.full_date) AS recency_days
    FROM fact_reservation r
    JOIN dim_passenger p ON r.passenger_key = p.passenger_key
    JOIN dim_date      d ON r.date_key      = d.date_key
    WHERE r.status = 'Confirmed'
    GROUP BY 1,2,3
),
rfm_scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,   -- Lower days = higher score
        NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)       AS m_score
    FROM rfm_base
),
rfm_segment AS (
    SELECT *,
        r_score + f_score + m_score AS rfm_total,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent Customers'
            WHEN r_score <= 2 AND f_score >= 4 THEN 'At Risk'
            WHEN r_score <= 2 AND m_score >= 4 THEN 'Cant Lose Them'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
            ELSE 'Potential Loyalists'
        END AS segment
    FROM rfm_scored
)
SELECT
    segment,
    COUNT(*)                    AS passenger_count,
    ROUND(AVG(monetary), 2)     AS avg_spend,
    ROUND(AVG(frequency), 1)    AS avg_flights,
    ROUND(AVG(recency_days), 0) AS avg_days_since_flight
FROM rfm_segment
GROUP BY segment
ORDER BY avg_spend DESC;


-- ──────────────────────────────────────
-- 4. ROUTE PROFITABILITY WITH PERCENTILE BANDS
-- ──────────────────────────────────────
WITH route_metrics AS (
    SELECT
        apo.iata_code               AS origin,
        apd.iata_code               AS destination,
        a.airline_code,
        COUNT(*)                    AS flight_count,
        SUM(f.total_revenue)        AS total_revenue,
        AVG(f.load_factor)          AS avg_load_factor,
        AVG(f.arr_delay_min)        AS avg_delay,
        SUM(f.distance_miles)       AS total_miles,
        SUM(f.total_revenue) / NULLIF(SUM(f.distance_miles), 0)
                                    AS revenue_per_mile
    FROM fact_flight f
    JOIN dim_airport apo ON f.origin_airport_key = apo.airport_key
    JOIN dim_airport apd ON f.dest_airport_key   = apd.airport_key
    JOIN dim_airline a   ON f.airline_key         = a.airline_key
    WHERE f.date_key BETWEEN 20240101 AND 20241231
      AND NOT f.is_cancelled
    GROUP BY 1,2,3
),
route_ranked AS (
    SELECT *,
        PERCENT_RANK() OVER (ORDER BY total_revenue DESC)      AS revenue_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_load_factor DESC)    AS load_percentile,
        PERCENT_RANK() OVER (ORDER BY revenue_per_mile DESC)   AS rpm_percentile,
        -- Rolling average revenue for nearby routes (by distance band)
        AVG(total_revenue) OVER (
            ORDER BY total_miles
            ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
        ) AS smoothed_revenue
    FROM route_metrics
)
SELECT
    origin,
    destination,
    airline_code,
    flight_count,
    ROUND(total_revenue, 0)         AS total_revenue,
    ROUND(avg_load_factor, 2)       AS avg_load_factor,
    ROUND(avg_delay, 1)             AS avg_delay_min,
    ROUND(revenue_per_mile, 4)      AS revenue_per_mile,
    ROUND(revenue_percentile * 100, 1) AS revenue_pct_rank,
    CASE
        WHEN revenue_percentile >= 0.8 THEN 'Platinum Route'
        WHEN revenue_percentile >= 0.6 THEN 'Gold Route'
        WHEN revenue_percentile >= 0.4 THEN 'Silver Route'
        ELSE 'Review Route'
    END AS route_tier
FROM route_ranked
ORDER BY total_revenue DESC
LIMIT 50;


-- ──────────────────────────────────────
-- 5. REAL-TIME DELAY PREDICTION FEATURES (WINDOW)
-- ──────────────────────────────────────
WITH hourly_airport_load AS (
    SELECT
        f.origin_airport_key,
        d.full_date,
        t.hour_24,
        COUNT(*) AS departures_this_hour,
        AVG(f.dep_delay_min) AS avg_dep_delay,
        SUM(CASE WHEN f.weather_delay_min > 0 THEN 1 ELSE 0 END)
                             AS weather_impacted_flights
    FROM fact_flight f
    JOIN dim_date d ON f.date_key    = d.date_key
    JOIN dim_time t ON f.dep_time_key = t.time_key
    GROUP BY 1,2,3
),
features AS (
    SELECT *,
        -- Prior hour delay (runway congestion signal)
        LAG(avg_dep_delay, 1) OVER (
            PARTITION BY origin_airport_key, full_date
            ORDER BY hour_24
        ) AS prev_hour_delay,
        -- 3-hour rolling average
        AVG(avg_dep_delay) OVER (
            PARTITION BY origin_airport_key, full_date
            ORDER BY hour_24
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ) AS rolling_3h_delay,
        -- Airport congestion index
        departures_this_hour * 1.0 /
            NULLIF(MAX(departures_this_hour) OVER (
                PARTITION BY origin_airport_key, full_date), 0)
                             AS congestion_index
    FROM hourly_airport_load
)
SELECT
    ap.iata_code,
    ap.airport_name,
    f.full_date,
    f.hour_24,
    f.departures_this_hour,
    ROUND(f.avg_dep_delay, 1)       AS avg_delay,
    ROUND(f.prev_hour_delay, 1)     AS prev_hour_delay,
    ROUND(f.rolling_3h_delay, 1)    AS rolling_3h_avg,
    ROUND(f.congestion_index, 3)    AS congestion_index
FROM features f
JOIN dim_airport ap ON f.origin_airport_key = ap.airport_key
WHERE f.full_date = CURRENT_DATE - 1
ORDER BY ap.iata_code, f.hour_24;
