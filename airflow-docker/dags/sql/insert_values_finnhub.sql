INSERT INTO finnhub_raw(
    symbol,
    inserted,
    current_price,
    change,
    percent_change,
    daily_high,
    daily_low,
    daily_opening,
    previous_closing_price,
    timestamp_finnhub
) VALUES (
    'CS',
    CURRENT_TIMESTAMP(0),
    {{params.c}},
    {{params.d}},
    {{params.dp}},
    {{params.h}},
    {{params.l}},
    {{params.o}},
    {{params.pc}},
    {{params.t}}
);