-- ============================================================
-- AIRLINE RESERVATION & DELAY ANALYTICS
-- Star Schema Data Warehouse DDL
-- ============================================================

-- ──────────────────────────────────────
-- DIMENSION TABLES
-- ──────────────────────────────────────

CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    day_of_week     TINYINT,
    day_name        VARCHAR(10),
    day_of_month    TINYINT,
    month_num       TINYINT,
    month_name      VARCHAR(10),
    quarter         TINYINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN,
    season          VARCHAR(10)  -- Winter/Spring/Summer/Fall
);

CREATE TABLE dim_time (
    time_key        INT PRIMARY KEY,
    full_time       TIME NOT NULL,
    hour_24         TINYINT,
    hour_12         TINYINT,
    minute_num      TINYINT,
    am_pm           CHAR(2),
    time_of_day     VARCHAR(15)  -- Morning/Afternoon/Evening/Night
);

CREATE TABLE dim_airline (
    airline_key     SERIAL PRIMARY KEY,
    airline_code    CHAR(2) NOT NULL UNIQUE,
    airline_name    VARCHAR(100) NOT NULL,
    alliance        VARCHAR(50),
    hub_airport     CHAR(3),
    country         VARCHAR(50),
    fleet_size      INT,
    founded_year    SMALLINT,
    is_active       BOOLEAN DEFAULT TRUE,
    effective_date  DATE,
    expiry_date     DATE,          -- SCD Type 2
    is_current      BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_airport (
    airport_key     SERIAL PRIMARY KEY,
    iata_code       CHAR(3) NOT NULL UNIQUE,
    icao_code       CHAR(4),
    airport_name    VARCHAR(150) NOT NULL,
    city            VARCHAR(100),
    state_code      CHAR(2),
    country         VARCHAR(50),
    region          VARCHAR(50),
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    timezone        VARCHAR(50),
    utc_offset      SMALLINT,
    elevation_ft    INT,
    airport_type    VARCHAR(20),   -- Large/Medium/Small/Regional
    num_terminals   TINYINT,
    is_hub          BOOLEAN
);

CREATE TABLE dim_aircraft (
    aircraft_key    SERIAL PRIMARY KEY,
    tail_number     VARCHAR(10) NOT NULL,
    aircraft_type   VARCHAR(50),
    manufacturer    VARCHAR(50),
    model           VARCHAR(50),
    seat_capacity   SMALLINT,
    range_miles     INT,
    year_built      SMALLINT,
    engine_type     VARCHAR(30),
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_passenger (
    passenger_key   SERIAL PRIMARY KEY,
    passenger_id    VARCHAR(20) NOT NULL UNIQUE,
    first_name      VARCHAR(50),
    last_name       VARCHAR(50),
    email           VARCHAR(100),
    phone           VARCHAR(20),
    nationality     VARCHAR(50),
    dob             DATE,
    frequent_flyer_num  VARCHAR(20),
    loyalty_tier    VARCHAR(20),   -- Bronze/Silver/Gold/Platinum
    total_miles     INT DEFAULT 0,
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE dim_delay_reason (
    delay_reason_key    SERIAL PRIMARY KEY,
    reason_code         VARCHAR(10) NOT NULL,
    reason_category     VARCHAR(50),   -- Carrier/Weather/NAS/Security/Late Aircraft
    reason_description  VARCHAR(200),
    is_controllable     BOOLEAN        -- Airline-controllable vs external
);

-- ──────────────────────────────────────
-- FACT TABLES
-- ──────────────────────────────────────

CREATE TABLE fact_flight (
    flight_key          BIGSERIAL PRIMARY KEY,
    -- Dimension Foreign Keys
    date_key            INT  REFERENCES dim_date(date_key),
    dep_time_key        INT  REFERENCES dim_time(time_key),
    arr_time_key        INT  REFERENCES dim_time(time_key),
    airline_key         INT  REFERENCES dim_airline(airline_key),
    origin_airport_key  INT  REFERENCES dim_airport(airport_key),
    dest_airport_key    INT  REFERENCES dim_airport(airport_key),
    aircraft_key        INT  REFERENCES dim_aircraft(aircraft_key),
    -- Degenerate Dimensions
    flight_number       VARCHAR(10),
    tail_number         VARCHAR(10),
    -- Scheduled vs Actual
    sched_dep_time      TIME,
    actual_dep_time     TIME,
    sched_arr_time      TIME,
    actual_arr_time     TIME,
    -- Measures
    dep_delay_min       INT DEFAULT 0,
    arr_delay_min       INT DEFAULT 0,
    elapsed_time_min    INT,
    distance_miles      INT,
    -- Delay Breakdown (minutes)
    carrier_delay_min   INT DEFAULT 0,
    weather_delay_min   INT DEFAULT 0,
    nas_delay_min       INT DEFAULT 0,
    security_delay_min  INT DEFAULT 0,
    late_aircraft_delay INT DEFAULT 0,
    -- Status Flags
    is_cancelled        BOOLEAN DEFAULT FALSE,
    is_diverted         BOOLEAN DEFAULT FALSE,
    cancellation_code   CHAR(1),    -- A=Carrier, B=Weather, C=NAS, D=Security
    -- Seat Metrics
    seats_available     SMALLINT,
    seats_booked        SMALLINT,
    load_factor         DECIMAL(5,2),
    -- Revenue
    avg_ticket_price    DECIMAL(10,2),
    total_revenue       DECIMAL(12,2)
)
PARTITION BY RANGE (date_key);   -- Partition by year

-- Monthly partitions example
CREATE TABLE fact_flight_2024_q1 PARTITION OF fact_flight
    FOR VALUES FROM (20240101) TO (20240401);
CREATE TABLE fact_flight_2024_q2 PARTITION OF fact_flight
    FOR VALUES FROM (20240401) TO (20240701);
CREATE TABLE fact_flight_2024_q3 PARTITION OF fact_flight
    FOR VALUES FROM (20240701) TO (20241001);
CREATE TABLE fact_flight_2024_q4 PARTITION OF fact_flight
    FOR VALUES FROM (20241001) TO (20250101);

CREATE TABLE fact_reservation (
    reservation_key     BIGSERIAL PRIMARY KEY,
    date_key            INT  REFERENCES dim_date(date_key),
    time_key            INT  REFERENCES dim_time(time_key),
    flight_key          BIGINT,
    passenger_key       INT  REFERENCES dim_passenger(passenger_key),
    airline_key         INT  REFERENCES dim_airline(airline_key),
    -- Degenerate Dimensions
    booking_ref         VARCHAR(10),
    ticket_number       VARCHAR(20),
    -- Measures
    cabin_class         VARCHAR(10),   -- Economy/Business/First
    fare_basis          VARCHAR(20),
    base_fare           DECIMAL(10,2),
    taxes_fees          DECIMAL(10,2),
    total_fare          DECIMAL(10,2),
    miles_earned        INT,
    booking_channel     VARCHAR(30),   -- Web/App/Agent/API
    days_before_flight  SMALLINT,
    is_refundable       BOOLEAN,
    status              VARCHAR(20)    -- Confirmed/Waitlisted/Cancelled/No-show
);

-- ──────────────────────────────────────
-- INDEXES
-- ──────────────────────────────────────

-- Clustered indexes on fact tables
CREATE INDEX idx_fact_flight_date     ON fact_flight(date_key);
CREATE INDEX idx_fact_flight_airline  ON fact_flight(airline_key, date_key);
CREATE INDEX idx_fact_flight_origin   ON fact_flight(origin_airport_key, date_key);
CREATE INDEX idx_fact_flight_delays   ON fact_flight(arr_delay_min) WHERE arr_delay_min > 0;
CREATE INDEX idx_fact_flight_composite ON fact_flight(date_key, airline_key, origin_airport_key, dest_airport_key);

-- Covering index for common reporting query
CREATE INDEX idx_flight_reporting ON fact_flight(date_key, airline_key)
    INCLUDE (dep_delay_min, arr_delay_min, is_cancelled, distance_miles, total_revenue);

CREATE INDEX idx_reservation_passenger ON fact_reservation(passenger_key, date_key);
CREATE INDEX idx_reservation_booking   ON fact_reservation(booking_ref);

-- ──────────────────────────────────────
-- MATERIALIZED VIEWS
-- ──────────────────────────────────────

CREATE MATERIALIZED VIEW mv_daily_airline_performance AS
SELECT
    d.full_date,
    d.year,
    d.month_num,
    d.month_name,
    a.airline_code,
    a.airline_name,
    COUNT(*)                                        AS total_flights,
    SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END)  AS cancelled_flights,
    SUM(CASE WHEN arr_delay_min > 15 THEN 1 ELSE 0 END) AS delayed_flights,
    AVG(arr_delay_min)                              AS avg_arr_delay,
    AVG(dep_delay_min)                              AS avg_dep_delay,
    SUM(total_revenue)                              AS total_revenue,
    AVG(load_factor)                                AS avg_load_factor
FROM fact_flight f
JOIN dim_date    d ON f.date_key    = d.date_key
JOIN dim_airline a ON f.airline_key = a.airline_key
GROUP BY 1,2,3,4,5,6;

CREATE UNIQUE INDEX ON mv_daily_airline_performance(full_date, airline_code);
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_airline_performance;
