        --
-- Name: raw_pricing; Type: TABLE; Schema: public; Owner: tsdbadmin
--

CREATE TABLE public.raw_pricing (
    "time" timestamp with time zone NOT NULL,
    fund text NOT NULL,
    expiry_date date NOT NULL,
    expiry_time time without time zone NOT NULL,
    expiry_cycle text NOT NULL,
    contract_type text NOT NULL,
    option_kind public.option_kind NOT NULL
    strike real NOT NULL,
    price real NOT NULL,
    volatility real NOT NULL,
    delta real NOT NULL,
    skew_delta real NOT NULL,
    gamma real NOT NULL,
    skew_gamma real NOT NULL,
    theta real NOT NULL,
    additional_event_variance real NOT NULL,
    forward real NOT NULL,
    base_price real NOT NULL,
    rho real NOT NULL,
    moving_skew real NOT NULL,
    moving_smile real NOT NULL,
    underlying_delta real NOT NULL,
    underlying_skew_delta real NOT NULL,
    vega real NOT NULL,
    tw_vega real NOT NULL,
    time_to_expiry real NOT NULL,
    vola_time_to_expiry real NOT NULL,
    strike_moneyness real NOT NULL,
    base_volatility real NOT NULL,
    basevol_moneyness real NOT NULL,
    market_width real NOT NULL,
    vanna real NOT NULL,
    charm real NOT NULL,
    price_error real NOT NULL,
    vola_error real NOT NULL
);

SELECT create_hypertable('raw_pricing', 'time', chunk_time_interval => INTERVAL '24 hours');
SELECT add_dimension('raw_pricing' 'fund', 16);

CREATE INDEX raw_pricing_fund_basevol_moneyness_vola_time_to_expiry_time_idx ON public.raw_pricing USING btree (fund, basevol_moneyness, vola_time_to_expiry, "time" DESC);
CREATE INDEX raw_pricing_fund_expiry_date_time_idx ON public.raw_pricing USING btree (fund, expiry_date, "time" DESC);
CREATE INDEX raw_pricing_time_idx ON public.raw_pricing USING btree ("time" DESC);

ALTER TABLE raw_pricing set (timescaledb.compress, timescaledb.compress_segmentby = 'fund, expiry_date');
SELECT add_compression_policy('raw_pricing', INTERVAL '7 days');


CREATE MATERIALIZED VIEW ohlc_daily
WITH (timescaledb.continuous) AS
SELECT fund, time_bucket(INTERVAL '1 day', "time") as bucket,
       first("time", "time") as open_time,
       first(base_price, "time") as "open",
       last("time", "time") as close_time,
       last(base_price, "time") as "close",
       count(*) as observations,
       max(base_price) as high,
       min(base_price) as low
FROM raw_pricing
  GROUP BY fund, bucket
  WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlc_daily',
  start_offset => NULL,
  end_offset   => INTERVAL '1 day',
  schedule_interval => INTERVAL '12 hours');
-- CALL refresh_continuous_aggregate('ohlc_daily', NULL, localtimestamp - INTERVAL '1 day');


CREATE MATERIALIZED VIEW daily_open_data
WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 day', rp."time") as bucket,
       (first(rp.*, "time")).*
FROM raw_pricing as rp
  GROUP BY bucket
  WITH NO DATA;

SELECT add_continuous_aggregate_policy('daily_open_data',
  start_offset => NULL,
  end_offset   => INTERVAL '1 day',
  schedule_interval => INTERVAL '12 hours');
-- CALL refresh_continuous_aggregate('daily_open_data', NULL, localtimestamp - INTERVAL '1 day');
