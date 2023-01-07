from datetime import time


EXPIRY_TIME = time(15, 30)
MIN_VTEXP = 0.001
OPTION_KINDS = ["call", "put"]
SECONDS_IN_A_YEAR = 365 * 24 * 60 * 60.0
BUSINESS_SECONDS_IN_A_YEAR = 252 * 24 * 60 * 60.0

PRICING_TABLE = "raw_pricing"
PRICING_COLUMNS = [
    "time",
    "fund",
    "expiry_date",
    "expiry_time",
    "expiry_cycle",
    "strike",
    "price",
    "volatility",
    "delta",
    "skew_delta",
    "gamma",
    "skew_gamma",
    "theta",
    "additional_event_variance",
    "forward",
    "base_price",
    "rho",
    "moving_skew",
    "moving_smile",
    "underlying_delta",
    "underlying_skew_delta",
    "vega",
    "tw_vega",
    "time_to_expiry",
    "vola_time_to_expiry",
    "strike_moneyness",
    "base_volatility",
    "basevol_moneyness",
    "market_width",
    "vanna",
    "charm",
    "price_error",
    "vola_error",
    "contract_type",
    "option_kind",
]
