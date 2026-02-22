from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Iterable


@dataclass(frozen=True)
class WeightedCounts:
    w_services: float
    w_cancelled: float
    w_disrupted: float


@dataclass(frozen=True)
class SlotMetricComputed:
    disruption_prob: float
    cancellation_prob: float
    reliability_score: int
    effective_sample_size: float
    confidence_band: str


def exp_recency_weight(age_days: int, half_life_days: float) -> float:
    """
    Exponential recency weighting with a half-life:
      weight(age=0) = 1
      weight(age=half_life_days) = 0.5
    """
    if age_days < 0:
        # future data shouldn't happen; treat as full weight
        age_days = 0
    if half_life_days <= 0:
        return 1.0
    return math.exp(-math.log(2.0) * (age_days / half_life_days))


def accumulate_weighted_counts(
    *,
    metric_date: date,
    rows: Iterable[dict],
    half_life_days: float,
) -> WeightedCounts:
    """
    Rows are dicts with keys:
      service_date (date), n_services (int), n_cancelled (int), n_disrupted (int)
    """
    w_services = 0.0
    w_cancelled = 0.0
    w_disrupted = 0.0

    for r in rows:
        service_date: date = r["service_date"]
        age_days = (metric_date - service_date).days
        w = exp_recency_weight(age_days=age_days, half_life_days=half_life_days)

        n_services = float(r["n_services"])
        n_cancelled = float(r["n_cancelled"])
        n_disrupted = float(r["n_disrupted"])

        w_services += w * n_services
        w_cancelled += w * n_cancelled
        w_disrupted += w * n_disrupted

    return WeightedCounts(w_services=w_services, w_cancelled=w_cancelled, w_disrupted=w_disrupted)


def beta_binomial_smooth(
    *,
    successes: float,
    trials: float,
    prior_p: float,
    prior_strength: float,
) -> float:
    """
    Beta-Binomial smoothing:
      prior alpha = prior_p * prior_strength
      prior beta  = (1 - prior_p) * prior_strength
      posterior mean = (alpha + successes) / (alpha + beta + trials)

    successes/trials can be weighted.
    """
    if trials <= 0:
        # no data -> fall back to prior
        return float(min(max(prior_p, 0.0), 1.0))

    prior_p = float(min(max(prior_p, 0.0), 1.0))
    prior_strength = float(max(prior_strength, 0.0))

    alpha = prior_p * prior_strength
    beta = (1.0 - prior_p) * prior_strength

    return float((alpha + successes) / (alpha + beta + trials))


def confidence_band(effective_sample_size: float) -> str:
    """
    Simple banding. Tune later.
    """
    if effective_sample_size >= 20.0:
        return "high"
    if effective_sample_size >= 8.0:
        return "medium"
    return "low"


def compute_slot_metric(
    *,
    w_counts: WeightedCounts,
    operator_prior_disruption: float,
    operator_prior_cancel: float,
    prior_strength: float,
) -> SlotMetricComputed:
    """
    Computes smoothed disruption and cancellation probabilities and converts to score.
    """
    n_eff = w_counts.w_services

    p_disruption = beta_binomial_smooth(
        successes=w_counts.w_disrupted,
        trials=w_counts.w_services,
        prior_p=operator_prior_disruption,
        prior_strength=prior_strength,
    )

    p_cancel = beta_binomial_smooth(
        successes=w_counts.w_cancelled,
        trials=w_counts.w_services,
        prior_p=operator_prior_cancel,
        prior_strength=prior_strength,
    )

    # reliability score: 100*(1 - disruption prob)
    score = int(round(100.0 * (1.0 - p_disruption)))
    score = max(0, min(100, score))

    return SlotMetricComputed(
        disruption_prob=p_disruption,
        cancellation_prob=p_cancel,
        reliability_score=score,
        effective_sample_size=n_eff,
        confidence_band=confidence_band(n_eff),
    )
