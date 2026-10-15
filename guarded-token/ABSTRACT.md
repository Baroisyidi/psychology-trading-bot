# Abstract

**Title:** Guarded Token: an AI-guarded ERC-20 token with automated detection and response

**Author:** IBRAHIM BARO YIDI, *MOSCOW TECHNICAL UNIVERSITY OF COMMUNICATION AND INFORMATIC, Masters in Quantum Communication, first year]*
**Contact:** *[Baroisyidi@gmail.com]*
**Format:** Git repository with documentation, tests and a live testnet deployment
**Version:** 1.0.0, 15 October 2026

**Keywords:** artificial intelligence in information security, blockchain
security, fraud detection, anomaly detection, gradient boosting, Isolation
Forest, smart contracts, ERC-20, automated incident response, client puzzles,
time-lock puzzles, Ed25519, Ethereum Sepolia

---

## 1. Setting the task

Digital tokens on public blockchains are attractive targets: when an
attacker steals a wallet's private key, or abuses a token's own rules, the
funds can be moved within minutes and blockchain transactions cannot be
reversed. Most monitoring tools only *report* suspicious activity; by the
time a person reads the alert, the funds are gone. Hand-written detection
rules are also easy to evade, because attackers keep each transaction just
below the thresholds.

The task of this work is to design, build and test a system that (a)
detects fraudulent and anomalous activity on a token automatically, using
machine learning; (b) **contains** an attack within seconds through actions
on the blockchain itself; and (c) does so safely, so that an automated
decision that turns out to be wrong cannot cause irreversible harm. The
system also has to protect itself: it must notice when its own monitoring
goes silent, and resist forged alerts and denial-of-service attacks on its
public interface.

## 2. Relevance

Theft through compromised keys, wallet drainers and laundering through many
fresh wallets ("fan-out") remain among the most common forms of fraud in
crypto-assets. For developers in African countries, where mobile money and
digital assets are growing quickly, low-cost and well-documented security
tooling is particularly valuable. The work also addresses a central
question of AI safety: how to give an AI system enough authority to be
useful in a crisis, while keeping its power bounded, interpretable and
reversible.

## 3. The approach used

The system is built in four layers, each assuming the previous one can fail.

**Layer 1: a token with built-in safety controls.** An ERC-20 token written
in Vyper includes a pause switch, a blocklist, an hourly transfer limit per
wallet, a hard supply cap and a 48-hour timelock on administrative changes.
Two roles are separated: a human *admin*, whose key is stored encrypted and
only unlocked by password, and an automated *guardian*, which can only block,
tighten limits and pause. The guardian can never move, mint or burn funds,
and can never undo its own actions.

**Layer 2: continuous monitoring.** A monitor reads every new block and
decodes the token's events. Alerts are signed with Ed25519, so a forged "all
clear" message can be detected, and are delivered through the Telegram Bot
API. A local watchdog and a remote watchdog (hosted separately on Render)
report if the monitor stops; remote heartbeats are signed and protected
against replay. The monitor was tuned for free, rate-limited blockchain
providers, using adaptive query ranges and back-off.

**Layer 3: machine-learning detection.** Each transfer is converted into 27
streaming features computed only from past events, as a live system
requires: wallet age, payments to brand-new wallets, outflow relative to the
wallet's own history, circular flows, and flags from the earlier rules. Two
models score every transfer: a gradient-boosted fraud classifier
(scikit-learn's HistGradientBoostingClassifier), which learns known attack
patterns, and an Isolation Forest anomaly detector, which learns only normal
behaviour and flags anything unusual. The higher score is used. Every alert
includes plain-language reasons, and related alerts are grouped into
incidents by following the money between wallets. The models were trained
on synthetic traffic from a simulator of more than 400 users, exchanges,
merchants, market makers, payroll and airdrops, plus five attack types:
drain, fan-out, low-and-slow, sybil farm and wash trading. Normal look-alike
behaviour, such as wallet migrations and new users, was deliberately
included to avoid shortcut learning. Training, threshold calibration and
testing used three separate simulations with fixed random seeds, and
thresholds were calibrated to a fixed false-alarm budget.

**Layer 4: active defence.** A guardian turns detections into an escalating
response: blocklist the wallets involved, then tighten the hourly limit, then
pause the token, under strict safeguards. It requires corroboration before
acting, is capped at 30 blocks per hour and 25 wallets per incident, never
touches protected wallets, offers a dry-run mode, and announces each action
with instructions for undoing it. Only the human admin can undo actions, and
weakening protections waits 48 hours. Separately, the public API uses
client puzzles (Hashcash-style proof-of-work and Rivest–Shamir–Wagner
time-lock puzzles) whose difficulty rises with the threat level and with a
client's suspicious behaviour, so flooding the service becomes expensive
while honest users are barely affected.

## 4. Key results

- **Detection on unseen data.** On a 28-day test simulation containing 99
  attacks, the models caught **99 of 99 attacks at 0.9 false alarms per
  day**, compared with 9 of 99 attacks and 106 false alarms per day for the
  hand-written rules.
- **Generalisation.** When the fraud model was retrained without one attack
  type and tested on it, it still caught most incidents but flagged fewer of
  their individual transfers (14% for low-and-slow, versus 83% when trained
  on it). This is reported as a limitation.
- **Red-team drill.** Against all five attack types, the guardian let **0
  tokens escape**, compared with 2,600 to 14,435 tokens without it, and
  blocked **no innocent wallets**.
- **Puzzles.** For 10,000 requests at the critical level, an honest client
  spends about 30 minutes of computation, while a flooding attacker spends
  about 28 hours.
- **Live deployment.** On the public Ethereum Sepolia test network, a
  stolen-key drill was stopped after **2 payments, in 18 seconds**,
  preserving 80% of the funds. The incident and the guardian's action were
  reported on Telegram and are publicly visible on the block explorer.
- **Quality.** 93 automated tests pass. The whole system runs from Docker,
  and every reported number can be reproduced with the commands in the
  repository.

## 5. Practical significance

The project shows a complete, reproducible pattern for AI-assisted security
on token platforms that a small team can run at almost no cost: free
testnet, free provider tiers, and a single computer with Docker. Its main
contribution is not a single model but the combination of interpretable
machine-learning detection with an automated response whose power is
limited by the smart contract itself. That combination makes automatic
containment acceptable even though models make mistakes. The same design
(bounded guardian role, signed alerts, watchdogs, puzzle-protected
interfaces) can be applied to other digital-asset and payment systems, and
the repository is documented as a learning resource.

## 6. Limitations and risks

The models were trained and tested on **simulated** traffic and must be
retrained on real data before any real use. The monitor acts on confirmed
blocks, so transactions already in flight still complete; in the live drill
2 payments got out before the block. Tightening limits affects all holders,
not only the attacker, and blocklisting is a centralised power that users
must be told about. Phishing, malware and smart-contract vulnerability
detection were planned but are not part of this version. The contract has
not been audited and the project is restricted to test networks: its tools
refuse to run on Ethereum mainnet.

The repository contains attack *simulators*, needed to train and test the
detector. They only produce transfers of the project's own test token
between wallets the operator creates, and contain no exploits, malware or
techniques against third-party systems. No real user data is used, all
secrets are kept out of the repository, and wallet keys are stored
encrypted.

**AI assistance:** *[describe your own contribution and the role of AI
tools, consistent with the README's "Authorship and AI assistance" section.]*
