# Vytals — Features (Plain English)

> For non-technical readers: what the system does and why it matters.
> Technical details live in [TECHNICAL.md](TECHNICAL.md); setup steps in [INSTALLATION.md](INSTALLATION.md).

## What Vytals is

Vytals is a **real-time health-risk monitoring system**. It watches a stream of patient vital signs — the numbers a hospital monitor shows continuously, like heart rate and blood oxygen — and turns them into **one easy-to-understand risk number for each patient, from 0 to 100**.

Instead of nurses and doctors having to scan a wall of changing numbers, they can glance at one score per patient and immediately see who needs attention first.

## The risk score

| Score | Colour | Meaning |
|---|---|---|
| 0–24 | 🟢 Green | **LOW** — vitals are in normal ranges |
| 25–49 | 🟡 Yellow | **MODERATE** — some readings are off; worth watching |
| 50–74 | 🟠 Orange | **HIGH** — several readings are abnormal or getting worse |
| 75–100 | 🔴 Red | **CRITICAL** — severely abnormal; needs immediate attention |

The score is produced by a machine-learning model trained on patient data, using the same physiological rules a clinician would apply (e.g. "very low oxygen plus a rising heart rate is dangerous") — but applied continuously, across all patients, second by second.

## What you can see on the dashboard

**Overview page (`/`)**
- **Live patient ranking** — every patient sorted by risk, worst first
- **Risk distribution** — how many patients are currently low / moderate / high / critical
- **Critical watch list** — the patients who need attention right now, with their key vitals
- **Key stats** — average risk, number of monitored patients, highest current score

**Patient page (`/patients/[id]`)**
- **Live gauges** for heart rate, blood oxygen, respiratory rate, temperature, and blood pressure — each with a clear normal/abnormal colour
- **Heart-rate trend chart** over time
- **Blood-oxygen + risk chart** — how oxygen and the risk score move together
- **Raw readings table** — the most recent individual measurements as they arrived

Everything on both pages **updates live** — no refresh button needed.

## Honest data handling

- **Missing readings are never invented.** If a sensor didn't report a value, the dashboard shows `—` instead of making one up. This matters in healthcare: a fabricated number is worse than a visible gap.
- Out-of-range readings (e.g. a blood pressure of 900) are treated as missing rather than trusted.
- The system flags **low-quality data windows** (too few readings) so a risk score based on thin data is visibly marked as such.

## Real patient data

The system runs on **real ICU patient data** — not just made-up numbers:

- **Source:** PhysioNet/CinC Challenge 2012 dataset — 4,000 real intensive-care patients, recorded over 48 hours each. It is publicly available for research, **no private/identifiable data** (patients are anonymous numbered records).
- It includes genuine vital signs: heart rate, oxygen saturation, breathing rate, temperature, and blood pressure.
- It also includes **real hospital outcomes** (length of stay, survival), which we can later use to check whether high risk scores actually correspond to sicker patients.
- A synthetic (computer-generated) dataset is also included for a quick, fully-controlled demo.

> Note: this dataset records arterial oxygen saturation (SaO₂) rather than the finger-clip SpO₂ — clinically the same purpose for detecting low oxygen. It is charted intermittently (as real ICU staff do), so some readings are genuinely absent — handled as described above.

## Two ways to run it

1. **Demo mode** — starts with a single command. No databases, no message queues, no Java. The system replays patient data through its own simulation engine so you can explore the full dashboard in under a minute. *Great for presentations and evaluation.*
2. **Full streaming mode** — the complete big-data pipeline: data flows into **Kafka** (an industry-standard message queue), is processed by **Apache Spark** (distributed stream processing), written to **PostgreSQL**, and served by the same dashboard. *This is the architecture the course project is about.*

Both modes produce the same dashboard and the same risk scores.

## What makes it "big data" ready

- Designed to ingest data **continuously** (like real bedside monitors and wearables produce it)
- **Windowed analysis**: risk is computed over rolling time windows (5-minute window, sliding every minute) per patient — so trends are captured, not just single readings
- Storage and processing components (HDFS, Spark, Kafka) are the standard tools used in industry for datasets far larger than one machine can handle
- The pipeline is **source-agnostic**: point it at any CSV/JSON export with patient IDs, timestamps, and vitals, and it works

## What it is *not*

- **Not a medical device.** It is an academic prototype and decision-support demonstration. It does not diagnose, does not replace clinical judgement, and is not certified for patient care.
- The "ground truth" the model learns from is a transparent set of physiology rules (not hospital-labelled outcomes) — this makes the model explainable but is a stand-in for clinical validation.

## What's coming next

- **HDFS** — distributed file storage for the raw vital-sign data (large-scale storage story)
- **Streaming validation** — end-to-end run of Kafka → Spark → Postgres on the real dataset
- **Model health check** — comparing risk scores against the real hospital outcomes in the dataset
- **Explainability** — showing *which* vital drove each patient's score (SHAP)
- **Distributed vs single-machine benchmark** — measuring the speed-up the big-data pipeline gives

## Quick glossary

| Term | What it means |
|---|---|
| **Vital signs** | Body measurements tracked continuously (heart rate, oxygen, breathing, temperature, blood pressure) |
| **SpO₂ / SaO₂** | Blood oxygen level, in percent (normal ≈ 95–100%) |
| **HR** | Heart rate, beats per minute (normal ≈ 60–100) |
| **RR** | Respiratory rate, breaths per minute (normal ≈ 12–20) |
| **SBP / DBP** | Top/bottom number of blood pressure (normal ≈ 120/80) |
| **Risk window** | The last few minutes of readings considered when computing a score |
| **Real-time** | Data is processed as it arrives, not after everything stops |
| **Streaming** | Continuous flow of data, like a river, rather than one big batch |
