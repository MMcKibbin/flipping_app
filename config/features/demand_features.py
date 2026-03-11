import pandas as pd


def build_demand_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["high_interest_flag"] = out["interest_score"] >= 70
    out["medium_interest_flag"] = (
        (out["interest_score"] >= 40) & (out["interest_score"] < 70)
    )
    out["low_interest_flag"] = out["interest_score"] < 40

    return out