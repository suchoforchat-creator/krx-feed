import math
from statistics import pstdev
import pytest

from src import compute


def test_compute_hv_matches_manual():
    prices = [100, 101, 102, 100, 99, 101, 103]
    window = 4
    hv = compute.compute_hv(prices, window=window)
    log_returns = [math.log(prices[i] / prices[i - 1]) for i in range(1, len(prices))]
    expected = math.sqrt(252) * pstdev(log_returns[-window:])
    assert hv == pytest.approx(expected)


def test_compute_correlation():
    a = [1, 2, 3, 4, 5, 6, 7, 8]
    b = [2, 1, 4, 3, 6, 5, 8, 7]
    corr = compute.compute_correlation(a, b, window=6)
    subset_a = a[-6:]
    subset_b = b[-6:]
    mean_a = sum(subset_a) / 6
    mean_b = sum(subset_b) / 6
    cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(subset_a, subset_b))
    var_a = sum((x - mean_a) ** 2 for x in subset_a)
    var_b = sum((y - mean_b) ** 2 for y in subset_b)
    expected = cov / math.sqrt(var_a * var_b)
    assert corr == pytest.approx(expected)


def test_compute_basis():
    future = 105.0
    spot = 100.0
    basis = compute.compute_basis(future, spot)
    assert basis == pytest.approx(0.05)
