# Phase 3 detector scorecard

Generated 2026-10-14 19:26 UTC by `python -m ml.train` (scikit-learn 1.9.1, 46s). All numbers below come from the **test** simulation, which the models never saw.

## Data

- **train**: 74,422 transfers over 28.0 days (2,658/day), 2,290 attack transfers; incidents: drain: 15, fan_out: 15, low_slow: 15, sybil_farm: 15, wash_cycle: 15
- **val**: 32,809 transfers over 14.0 days (2,344/day), 796 attack transfers; incidents: drain: 6, fan_out: 6, low_slow: 6, sybil_farm: 6, wash_cycle: 6
- **test**: 73,592 transfers over 28.0 days (2,629/day), 2,584 attack transfers; incidents: drain: 20, fan_out: 20, low_slow: 20, sybil_farm: 19, wash_cycle: 20

## Headline: attacks caught vs false alarms

An attack incident counts as caught if at least one of its transfers scores 70+ (ALERT).

| Detector | Incidents caught (70+) | Caught at 40+ | False alarms/day (70+) | False alarms/day (40+) | False-alarm Telegram messages/day | Attack transfers flagged (70+) |
|---|---|---|---|---|---|---|
| Phase 2 rules | 9/99 (9%) | 43% | 105.57 | 277.39 | 180.1 | 1% |
| Anomaly model | 18/99 (18%) | 26% | 0.43 | 2.82 | 2.7 | 5% |
| Fraud model | 99/99 (100%) | 100% | 0.43 | 1.07 | 1.1 | 94% |
| Combined (new) | 99/99 (100%) | 100% | 0.86 | 3.86 | 3.7 | 94% |

## By attack type (caught at 70+, median time to first alert)

| Attack | Phase 2 rules | Anomaly model | Fraud model | Combined (new) |
|---|---|---|---|---|
| drain | 5/20, 0 min | 4/20, 0 min | 20/20, 1 min | 20/20, 1 min |
| fan_out | 0/20 | 0/20 | 20/20, 9 min | 20/20, 9 min |
| low_slow | 2/20, 6.4 h | 0/20 | 20/20, 45 min | 20/20, 45 min |
| sybil_farm | 0/19 | 14/19, 7 min | 19/19, 0 min | 19/19, 0 min |
| wash_cycle | 2/20, 55 min | 0/20 | 20/20, 0 min | 20/20, 0 min |

## How far each attack got before the combined detector alerted

Median share of the attack's transfers already made when the first 70+ alert fired (100% = never caught).

| Attack | Phase 2 rules | Combined |
|---|---|---|
| drain | 100% | 37% |
| fan_out | 100% | 12% |
| low_slow | 100% | 17% |
| sybil_farm | 100% | 2% |
| wash_cycle | 100% | 3% |

## What raises false alarms (normal transfers scoring 40+ in the test period)

| Detector | Normal activity flagged |
|---|---|
| Phase 2 rules | exchange: 4207, market_maker: 2707, new_user: 609, payroll: 106, merchant: 87, promo: 30, migration: 8, user: 7, split: 6 |
| Anomaly model | merchant: 49, promo: 11, migration: 7, exchange: 6, new_user: 4, split: 1, user: 1 |
| Fraud model | split: 16, migration: 7, user: 6, new_user: 1 |
| Combined (new) | merchant: 49, split: 16, migration: 14, promo: 11, user: 7, exchange: 6, new_user: 5 |

## Unseen attack types (leave-one-attack-out)

The fraud model is retrained **without** one attack type, then tested on that type. This shows what happens when a new kind of attack appears that it has never seen.

| Held-out attack | Incidents caught: fraud model alone | Incidents caught: combined | Its transfers flagged: combined | Transfers flagged when trained on it |
|---|---|---|---|---|
| drain | 95% | 95% | 49% | 82% |
| fan_out | 85% | 85% | 36% | 84% |
| low_slow | 100% | 100% | 14% | 83% |
| sybil_farm | 100% | 100% | 56% | 100% |
| wash_cycle | 100% | 100% | 98% | 100% |

## Method and limits

- Traffic is **simulated**. The models learn the habits of simulated users, exchanges and attackers; real traffic will differ, so expect lower scores on a real chain until the models are retrained on it.
- Detection is per transfer, using only past events, exactly as the live monitor sees them.
- False alarms are counted per transfer before the monitor's grouping of repeats, so real Telegram volume is lower.
- Score bands are calibrated on a separate validation simulation to false-alarm budgets (WARNING 2.0/day, ALERT 0.3/day, CRITICAL 0.05/day per model).
- The Phase 2 rules stay in charge of admin events (pause, role changes). For transfers they become inputs to the fraud model.

