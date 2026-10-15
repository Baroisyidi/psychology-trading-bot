"""
Train, calibrate and evaluate the Phase 3 detectors.

    python -m ml.train            # full run (about a minute)
    python -m ml.train --quick    # tiny run for tests

Three SEPARATE simulations are used, so the scorecard is honest:
  train       the models learn from this
  validation  sets the score thresholds (false-alarm budgets)
  test        never seen during training; all reported numbers come from it

Two models:
  anomaly   Isolation Forest trained on NORMAL traffic only. It knows nothing
            about attacks, so it can flag new kinds of attack.
  fraud     Gradient-boosted trees trained on labeled normal + attack traffic.
            Strong on attack types it has seen; the leave-one-attack-out
            check shows how it copes with a type it has never seen.

Scores are calibrated to false-alarm budgets measured on normal validation
traffic: a model may push at most ~2 normal transfers a day to WARNING (40),
~0.3 a day to ALERT (70) and ~0.05 a day to CRITICAL (90).
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import sklearn
from sklearn.ensemble import HistGradientBoostingClassifier, IsolationForest
from sklearn.metrics import average_precision_score

from .features import FEATURES, FeatureExtractor
from .simulate import ATTACK_KINDS, DAY, simulate

ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = ROOT / "models" / "guard_model.joblib"
REPORT_PATH = ROOT / "ml" / "report.md"
METRICS_PATH = ROOT / "ml" / "metrics.json"

BUDGETS = {40: 2.0, 70: 0.3, 90: 0.05}  # max normal transfers per day at each band


@dataclass
class Dataset:
    X: np.ndarray
    y: np.ndarray
    kind: np.ndarray
    incident: np.ndarray
    t: np.ndarray
    value: np.ndarray
    rule_score: np.ndarray
    sender: np.ndarray
    days: float
    summary: str = ""
    extra: dict = field(default_factory=dict)


def build(seed: int, days: int, incidents: int, users: int) -> Dataset:
    from .simulate import summary

    events = simulate(seed, days=days, n_users=users, incidents_per_kind=incidents)
    fx = FeatureExtractor()
    X, y, kind, inc, t, val, rs, snd = [], [], [], [], [], [], [], []
    for e in events:
        out = fx.observe(e.t, e.sender, e.receiver, e.value)
        if out is None:
            continue
        x, rule_score, _ = out
        X.append(x)
        y.append(e.label)
        kind.append(e.kind)
        inc.append(e.incident)
        t.append(e.t)
        val.append(e.value)
        rs.append(rule_score)
        snd.append(e.sender)
    return Dataset(
        np.array(X, dtype=float), np.array(y), np.array(kind), np.array(inc),
        np.array(t), np.array(val), np.array(rs, dtype=float), np.array(snd), days, summary(events),
    )


# ------------------------------------------------------------ calibration


def calibrate(raw_normal: np.ndarray, normals_per_day: float, top: float) -> dict:
    """Map raw model output to 0-100 so each band meets its false-alarm budget."""
    lo = float(np.min(raw_normal))
    cuts = []
    for band in (40, 70, 90):
        q = max(0.0, 1 - BUDGETS[band] / normals_per_day)
        cuts.append(float(np.quantile(raw_normal, q)))
    for i in range(1, 3):  # keep strictly increasing
        cuts[i] = max(cuts[i], cuts[i - 1] + 1e-9)
    hi = max(top, cuts[2] + 1e-9)
    return {"x": [lo, *cuts, hi], "y": [0, 40, 70, 90, 100]}


def to_score(raw: np.ndarray, cal: dict) -> np.ndarray:
    return np.interp(raw, cal["x"], cal["y"])


@dataclass
class Guard:
    clf: HistGradientBoostingClassifier
    iso: IsolationForest
    cal_clf: dict
    cal_iso: dict

    def raw(self, X):
        return self.clf.predict_proba(X)[:, 1], -self.iso.score_samples(X)

    def scores(self, X):
        p, a = self.raw(X)
        fraud, anomaly = to_score(p, self.cal_clf), to_score(a, self.cal_iso)
        return fraud, anomaly, np.maximum(fraud, anomaly)


def fit_classifier(ds: Dataset, exclude_kind: str | None = None) -> HistGradientBoostingClassifier:
    keep = np.ones(len(ds.y), bool) if exclude_kind is None else (ds.kind != exclude_kind)
    clf = HistGradientBoostingClassifier(
        max_iter=250,
        learning_rate=0.08,
        max_leaf_nodes=31,
        min_samples_leaf=40,
        l2_regularization=1.0,
        class_weight="balanced",
        random_state=0,
    )
    return clf.fit(ds.X[keep], ds.y[keep])


def fit_guard(train: Dataset, val: Dataset, exclude_kind=None, iso=None, cal_iso=None) -> Guard:
    clf = fit_classifier(train, exclude_kind)
    normal = val.y == 0
    per_day = normal.sum() / val.days
    if iso is None:
        iso = IsolationForest(n_estimators=200, max_samples=4096, random_state=0)
        iso.fit(train.X[train.y == 0])
        a = -iso.score_samples(val.X[normal])
        cal_iso = calibrate(a, per_day, top=float(a.max()) + 0.05)
    p = clf.predict_proba(val.X[normal])[:, 1]
    cal_clf = calibrate(p, per_day, top=1.0)
    return Guard(clf, iso, cal_clf, cal_iso)


# ------------------------------------------------------------ evaluation


def incident_stats(ds: Dataset, score: np.ndarray, threshold: float) -> dict:
    """Per attack type: detection rate, minutes to first alert, attack progress at alert."""
    out = {}
    for kind in ATTACK_KINDS:
        ids = sorted(set(ds.incident[(ds.kind == kind) & (ds.y == 1)]))
        hits, minutes, progress = 0, [], []
        for i in ids:
            m = np.where(ds.incident == i)[0]
            m = m[np.argsort(ds.t[m], kind="stable")]
            flagged = np.where(score[m] >= threshold)[0]
            if len(flagged):
                hits += 1
                first = flagged[0]
                minutes.append((ds.t[m[first]] - ds.t[m[0]]) / 60)
                progress.append((first + 1) / len(m))
            else:
                progress.append(1.0)
        out[kind] = {
            "incidents": len(ids),
            "detected": hits,
            "rate": hits / len(ids) if ids else 0.0,
            "median_minutes": float(np.median(minutes)) if minutes else None,
            "median_progress": float(np.median(progress)) if progress else None,
        }
    total = sum(v["incidents"] for v in out.values())
    out["all"] = {
        "incidents": total,
        "detected": sum(v["detected"] for v in out.values()),
        "rate": sum(v["detected"] for v in out.values()) / total if total else 0.0,
    }
    return out


def false_alarms_per_day(ds: Dataset, score: np.ndarray, threshold: float) -> float:
    return float(((score >= threshold) & (ds.y == 0)).sum() / ds.days)


def messages_per_day(ds: Dataset, score: np.ndarray, threshold: float, window: int = 300) -> float:
    """False-alarm Telegram messages per day after the monitor's grouping of repeats
    (same wallet within 5 minutes, unless the score gets worse)."""
    last: dict = {}
    sent = 0
    for i in np.where((score >= threshold) & (ds.y == 0))[0]:
        key, t, sc = ds.sender[i], ds.t[i], score[i]
        prev = last.get(key)
        if prev and t - prev[0] < window and sc <= prev[1]:
            continue
        last[key] = (t, sc)
        sent += 1
    return sent / ds.days


def normal_kinds_flagged(ds: Dataset, score: np.ndarray, threshold: float) -> dict:
    m = (score >= threshold) & (ds.y == 0)
    kinds, counts = np.unique(ds.kind[m], return_counts=True)
    return {str(k): int(c) for k, c in zip(kinds, counts)}


def evaluate(test: Dataset, guard: Guard) -> dict:
    fraud, anomaly, combined = guard.scores(test.X)
    detectors = {"rules": test.rule_score, "anomaly": anomaly, "fraud": fraud, "combined": combined}
    res = {}
    for name, s in detectors.items():
        res[name] = {
            "fa_per_day_40": false_alarms_per_day(test, s, 40),
            "fa_per_day_70": false_alarms_per_day(test, s, 70),
            "msgs_per_day_40": messages_per_day(test, s, 40),
            "fa_kinds_40": normal_kinds_flagged(test, s, 40),
            "pr_auc": float(average_precision_score(test.y, s)),
            "transfer_recall_70": float((s[test.y == 1] >= 70).mean()),
            "per_kind_transfer_recall": {
                k: float((s[(test.kind == k) & (test.y == 1)] >= 70).mean()) for k in ATTACK_KINDS
            },
            "at_70": incident_stats(test, s, 70),
            "at_40": incident_stats(test, s, 40),
        }
    return res


# ------------------------------------------------------------ report


def pct(x):
    return "n/a" if x is None else f"{x:.0%}"


def mins(x):
    return "-" if x is None else (f"{x:.0f} min" if x < 120 else f"{x / 60:.1f} h")


def write_report(path: Path, cfg: dict, sets: dict, res: dict, loo: dict, took: float):
    names = ["rules", "anomaly", "fraud", "combined"]
    label = {"rules": "Phase 2 rules", "anomaly": "Anomaly model", "fraud": "Fraud model", "combined": "Combined (new)"}
    L = []
    L.append("# Phase 3 detector scorecard\n")
    L.append(
        f"Generated {cfg['when']} by `python -m ml.train`{' --quick' if cfg['quick'] else ''} "
        f"(scikit-learn {sklearn.__version__}, {took:.0f}s). All numbers below come from the "
        "**test** simulation, which the models never saw.\n"
    )
    L.append("## Data\n")
    for k, s in sets.items():
        L.append(f"- **{k}**: {s}")
    L.append("")
    L.append("## Headline: attacks caught vs false alarms\n")
    L.append("An attack incident counts as caught if at least one of its transfers scores 70+ (ALERT).\n")
    L.append("| Detector | Incidents caught (70+) | Caught at 40+ | False alarms/day (70+) | False alarms/day (40+) | False-alarm Telegram messages/day | Attack transfers flagged (70+) |")
    L.append("|---|---|---|---|---|---|---|")
    for n in names:
        r = res[n]
        L.append(
            f"| {label[n]} | {r['at_70']['all']['detected']}/{r['at_70']['all']['incidents']} "
            f"({pct(r['at_70']['all']['rate'])}) | {pct(r['at_40']['all']['rate'])} | "
            f"{r['fa_per_day_70']:.2f} | {r['fa_per_day_40']:.2f} | {r['msgs_per_day_40']:.1f} | {pct(r['transfer_recall_70'])} |"
        )
    L.append("")
    L.append("## By attack type (caught at 70+, median time to first alert)\n")
    L.append("| Attack | " + " | ".join(label[n] for n in names) + " |")
    L.append("|---|" + "---|" * len(names))
    for kind in ATTACK_KINDS:
        cells = []
        for n in names:
            s = res[n]["at_70"][kind]
            cells.append(f"{s['detected']}/{s['incidents']}" + (f", {mins(s['median_minutes'])}" if s["detected"] else ""))
        L.append(f"| {kind} | " + " | ".join(cells) + " |")
    L.append("")
    L.append("## How far each attack got before the combined detector alerted\n")
    L.append("Median share of the attack's transfers already made when the first 70+ alert fired (100% = never caught).\n")
    L.append("| Attack | Phase 2 rules | Combined |")
    L.append("|---|---|---|")
    for kind in ATTACK_KINDS:
        L.append(f"| {kind} | {pct(res['rules']['at_70'][kind]['median_progress'])} | {pct(res['combined']['at_70'][kind]['median_progress'])} |")
    L.append("")
    L.append("## What raises false alarms (normal transfers scoring 40+ in the test period)\n")
    L.append("| Detector | Normal activity flagged |")
    L.append("|---|---|")
    for n in names:
        kinds = res[n]["fa_kinds_40"]
        L.append(f"| {label[n]} | " + (", ".join(f"{k}: {v}" for k, v in sorted(kinds.items(), key=lambda kv: -kv[1])) or "none") + " |")
    L.append("")
    L.append("## Unseen attack types (leave-one-attack-out)\n")
    L.append(
        "The fraud model is retrained **without** one attack type, then tested on that type. "
        "This shows what happens when a new kind of attack appears that it has never seen.\n"
    )
    L.append("| Held-out attack | Incidents caught: fraud model alone | Incidents caught: combined | Its transfers flagged: combined | Transfers flagged when trained on it |")
    L.append("|---|---|---|---|---|")
    for kind in ATTACK_KINDS:
        trained = res["combined"]["per_kind_transfer_recall"][kind]
        L.append(
            f"| {kind} | {pct(loo[kind]['fraud'])} | {pct(loo[kind]['combined'])} | "
            f"{pct(loo[kind]['transfers'])} | {pct(trained)} |"
        )
    L.append("")
    L.append("## Method and limits\n")
    L.append(
        "- Traffic is **simulated**. The models learn the habits of simulated users, exchanges and attackers; "
        "real traffic will differ, so expect lower scores on a real chain until the models are retrained on it.\n"
        "- Detection is per transfer, using only past events, exactly as the live monitor sees them.\n"
        "- False alarms are counted per transfer before the monitor's grouping of repeats, so real Telegram "
        "volume is lower.\n"
        "- Score bands are calibrated on a separate validation simulation to false-alarm budgets "
        f"(WARNING {BUDGETS[40]}/day, ALERT {BUDGETS[70]}/day, CRITICAL {BUDGETS[90]}/day per model).\n"
        "- The Phase 2 rules stay in charge of admin events (pause, role changes). For transfers they become "
        "inputs to the fraud model.\n"
    )
    path.write_text("\n".join(L) + "\n")


# ------------------------------------------------------------ main


def main(argv=None):
    ap = argparse.ArgumentParser(description="Train the Phase 3 detectors")
    ap.add_argument("--quick", action="store_true", help="tiny datasets (for tests)")
    ap.add_argument("--model", default=str(MODEL_PATH))
    ap.add_argument("--report", default=str(REPORT_PATH))
    ap.add_argument("--metrics", default=str(METRICS_PATH))
    args = ap.parse_args(argv)

    start = time.time()
    if args.quick:
        cfg = {"train": (11, 5, 4, 120), "val": (12, 4, 2, 120), "test": (13, 4, 3, 120)}
    else:
        cfg = {"train": (101, 28, 15, 400), "val": (202, 14, 6, 400), "test": (303, 28, 20, 400)}

    sets = {}
    for name, (seed, days, inc, users) in cfg.items():
        print(f"Simulating {name} set ...", flush=True)
        sets[name] = build(seed, days, inc, users)
        print(f"  {sets[name].summary}", flush=True)
    train, val, test = sets["train"], sets["val"], sets["test"]

    print("Training models ...", flush=True)
    guard = fit_guard(train, val)
    res = evaluate(test, guard)

    print("Leave-one-attack-out checks ...", flush=True)
    loo = {}
    for kind in ATTACK_KINDS:
        g = fit_guard(train, val, exclude_kind=kind, iso=guard.iso, cal_iso=guard.cal_iso)
        fraud, _, combined = g.scores(test.X)
        m = (test.kind == kind) & (test.y == 1)
        loo[kind] = {
            "fraud": incident_stats(test, fraud, 70)[kind]["rate"],
            "combined": incident_stats(test, combined, 70)[kind]["rate"],
            "transfers": float((combined[m] >= 70).mean()) if m.any() else 0.0,
        }

    normal_val = val.X[val.y == 0]
    p99 = {f: float(np.quantile(normal_val[:, i], 0.99)) for i, f in enumerate(FEATURES)}
    when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    bundle = {
        "format": 1,
        "features": FEATURES,
        "clf": guard.clf,
        "iso": guard.iso,
        "cal_clf": guard.cal_clf,
        "cal_iso": guard.cal_iso,
        "normal_p99": p99,
        "sklearn": sklearn.__version__,
        "trained": when,
        "quick": args.quick,
    }
    Path(args.model).parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, args.model)

    took = time.time() - start
    summaries = {k: v.summary for k, v in sets.items()}
    write_report(Path(args.report), {"when": when, "quick": args.quick}, summaries, res, loo, took)
    Path(args.metrics).write_text(json.dumps({"results": res, "leave_one_out": loo}, indent=1))

    c, r = res["combined"], res["rules"]
    print(
        f"\nDone in {took:.0f}s. Test set: combined caught {c['at_70']['all']['detected']}/"
        f"{c['at_70']['all']['incidents']} attacks at 70+ with {c['fa_per_day_70']:.2f} false alarms/day; "
        f"rules caught {r['at_70']['all']['detected']}/{r['at_70']['all']['incidents']} with "
        f"{r['fa_per_day_70']:.2f}/day."
    )
    print(f"Model saved to {args.model}\nReport: {args.report}")


if __name__ == "__main__":
    main()
