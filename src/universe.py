from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd


def load_universe(config: Dict) -> pd.DataFrame:
    entries: List[Dict] = []
    for market, items in config.get("universe", {}).items():
        for item in items:
            entries.append({
                "market": market,
                "code": item["code"],
                "name": item.get("name", ""),
                "weight": float(item.get("weight", 0)),
            })
    return pd.DataFrame(entries)
