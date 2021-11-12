CREATE TABLE IF NOT EXISTS finnhub_raw (
    symbol text,
    inserted timestamp(0) without time zone NOT NULL,
    current_price NUMERIC(15, 10),
    change NUMERIC(15, 10),
    percent_change NUMERIC(15, 10),
    daily_high NUMERIC(15, 10),
    daily_low NUMERIC(15, 10),
    daily_opening NUMERIC(15, 10),
    previous_closing_price NUMERIC(15, 10),
    timestamp_finnhub NUMERIC(25, 0)
);