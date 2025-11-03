from __future__ import annotations

from typing import Dict

import pandas as pd


def adv_dec_unch(snapshots: pd.DataFrame) -> Dict[str, pd.Series]:
    grouped = snapshots.groupby("market")
    adv_mask = grouped["change"].apply(lambda s: (s > 0).sum())
    dec_mask = grouped["change"].apply(lambda s: (s < 0).sum())
    unch_mask = grouped["change"].apply(lambda s: (s == 0).sum())
    adv_value = snapshots.loc[snapshots["change"] > 0].groupby("market")["value_traded"].sum()
    dec_value = snapshots.loc[snapshots["change"] < 0].groupby("market")["value_traded"].sum()
    limit_up = grouped["limit_flag"].apply(lambda s: (s == "upper").sum())
    limit_down = grouped["limit_flag"].apply(lambda s: (s == "lower").sum())
    markets = sorted(snapshots["market"].unique())
    result = {
        "adv_count": adv_mask.reindex(markets, fill_value=0),
        "dec_count": dec_mask.reindex(markets, fill_value=0),
        "unch_count": unch_mask.reindex(markets, fill_value=0),
        "adv_value": adv_value.reindex(markets, fill_value=0),
        "dec_value": dec_value.reindex(markets, fill_value=0),
        "limit_up": limit_up.reindex(markets, fill_value=0),
        "limit_down": limit_down.reindex(markets, fill_value=0),
    }
    return result


def trin(adv_cnt: float, dec_cnt: float, adv_val: float, dec_val: float) -> float:
    if dec_cnt == 0 or dec_val == 0:
        return float("nan")
    return (adv_cnt / dec_cnt) / (adv_val / dec_val)
