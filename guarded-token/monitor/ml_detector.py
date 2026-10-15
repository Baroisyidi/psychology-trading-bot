"""
Phase 3 detector: trained models for transfers, Phase 2 rules for the rest.

- Transfers are scored by the fraud model and the anomaly model (0-100,
  the higher of the two wins). The Phase 2 transfer rules still run, but as
  inputs to the fraud model, and the ones that fired are listed in the alert.
- Admin events (pause, blocklist, role changes) and mints keep their fixed
  Phase 2 scores.

The model file is created by `python -m ml.train`. Model files are Python
pickles: loading one runs code, so only load files you trained yourself.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

# The `ml` package sits next to `monitor`. Make sure it is importable even
# when this module is loaded lazily (e.g. under `ape run`, which only puts the
# project folder on the import path while a script is being loaded).
_ROOT = str(Path(__file__).resolve().parent.parent)
if _ROOT not in sys.path:
    sys.path.append(_ROOT)

from ml.features import FEATURES, FeatureExtractor, explain  # noqa: E402

from .alerts import Alert, fmt_tokens, short
from .rules import ZERO, Context, Detector, Meta

WEI = 10**18
MIN_REPORTED_SCORE = 20  # quieter transfers are not even logged


def _to_score(raw: float, cal: dict) -> float:
    return float(np.interp(raw, cal["x"], cal["y"]))


class GuardDetector:
    def __init__(self, bundle: dict):
        if bundle.get("features") != FEATURES:
            raise ValueError("model was trained with different features; run: python -m ml.train")
        self.bundle = bundle
        self.clf = bundle["clf"]
        self.iso = bundle["iso"]
        self.rules = Detector()  # admin events and mints
        self.fx: FeatureExtractor | None = None

    @classmethod
    def load(cls, path: str | Path) -> "GuardDetector":
        import joblib
        import sklearn

        bundle = joblib.load(path)
        if bundle.get("sklearn") != sklearn.__version__:
            print(
                f"Warning: model trained with scikit-learn {bundle.get('sklearn')}, "
                f"running {sklearn.__version__}. Retrain with: python -m ml.train"
            )
        return cls(bundle)

    @property
    def description(self) -> str:
        kind = "quick test model" if self.bundle.get("quick") else "model"
        return f"trained {kind} from {self.bundle.get('trained', '?')}"

    def score(self, x: list[float]) -> tuple[float, float]:
        X = np.asarray([x], dtype=float)
        p = float(self.clf.predict_proba(X)[0, 1])
        a = float(-self.iso.score_samples(X)[0])
        return _to_score(p, self.bundle["cal_clf"]), _to_score(a, self.bundle["cal_iso"])

    def observe(self, name: str, args: dict, meta: Meta, ctx: Context) -> list[Alert]:
        if name != "Transfer":
            return self.rules.observe(name, args, meta, ctx)
        if self.fx is None:
            # Features stay normalised to the limit the model was trained with,
            # so tightening the live limit doesn't change what "normal" means.
            self.fx = FeatureExtractor(admin=ctx.admin)

        sender, receiver, value = args["sender"], args["receiver"], args["value"]
        out = self.fx.observe(meta.timestamp, sender, receiver, value / WEI)
        if sender == ZERO:
            return self.rules.observe(name, args, meta, ctx)  # mints: Phase 2 scoring
        if out is None:
            return []

        x, _, fired = out
        fraud, anomaly = self.score(x)
        score = round(max(fraud, anomaly))
        if score < MIN_REPORTED_SCORE:
            return []

        lead = "ml_fraud" if fraud >= anomaly else "ml_anomaly"
        title = (
            "Transfer matches attack patterns" if lead == "ml_fraud" else "Highly unusual transfer"
        )
        reasons = explain(x, self.bundle["normal_p99"])
        details = [
            f"from {sender}",
            f"to {short(receiver)}",
            f"amount {fmt_tokens(value)}",
            f"fraud model {fraud:.0f}, anomaly model {anomaly:.0f}",
        ]
        details += [f"why: {r}" for r in reasons] or ["why: unusual combination of features"]
        if fired:
            details.append("Phase 2 rules also matched: " + ", ".join(fired))
        return [
            Alert(
                score=score,
                title=title,
                details=details,
                rules=[lead],
                block=meta.block,
                timestamp=meta.timestamp,
                tx_hash=meta.tx_hash,
                subject=sender,
                related=self._related(sender, receiver, meta.timestamp),
                value=value,
            )
        ]

    def _related(self, sender: str, receiver: str, now: int) -> list[str]:
        """Wallets that tie this alert to an incident: the sender, a young receiver,
        and any brand-new wallets the sender paid in the last hour (likely mules)."""
        out = [sender]
        if self._young(receiver, now):
            out.append(receiver)
        for t, r, _, was_new in self.fx.out[sender]:
            if was_new and t > now - 3600 and r not in out:
                out.append(r)
        return out

    def _young(self, addr: str, now: int) -> bool:
        first = self.fx.first_seen.get(addr)
        return first is not None and now - first < 86_400
