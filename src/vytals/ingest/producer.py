"""Kafka producer: replay CSV/JSONL -> vitals.raw. key=patient_id, acks=all, preserve event_time."""
from __future__ import annotations
import argparse, json, time
from datetime import datetime, timezone
from vytals.data.loader import load_any
from vytals.config import load_config

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--topic", default=None)
    ap.add_argument("--speed", type=float, default=60, help="1=realtime, 60=60x, 0=max throughput")
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--loop", action="store_true", help="loop forever")
    return ap.parse_args()

def main():
    a = parse_args()
    cfg = load_config()
    topic = a.topic or cfg["kafka"]["topic"]
    bootstrap = a.bootstrap or cfg["kafka"]["bootstrap_servers"]
    df = load_any(a.input)
    df = df.sort_values(["event_time"]).reset_index(drop=True)
    print(f"replaying {len(df)} records -> {bootstrap}/{topic} speed={a.speed}")

    from kafka import KafkaProducer
    try:
        prod = KafkaProducer(
            bootstrap_servers=bootstrap.split(","),
            key_serializer=lambda k: k.encode(),
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all",
            linger_ms=20,
            retries=5,
            request_timeout_ms=8000,
            max_block_ms=8000,  # fail fast when Kafka down instead of blocking forever
        )
    except Exception as e:
        # kafka-python raises NoBrokersAvailable (name varies by fork/version) on connect
        raise SystemExit(f"cannot reach Kafka at {bootstrap}. Start infra first: docker compose up -d kafka (or make infra). detail: {e}")
    errors: list = []
    def _on_err(exc, _msg=None):
        errors.append(str(exc))
    sent, t0 = 0, time.time()
    # inter-arrival based on event_time deltas / speed
    prev_et = None
    while True:
        for _, r in df.iterrows():
            msg = {c: (None if pd_isna(r[c]) else r[c]) for c in df.columns}
            # normalize numpy types
            for k, v in list(msg.items()):
                try:
                    import math
                    if v is None or (isinstance(v, float) and math.isnan(v)):
                        msg[k] = None
                    elif hasattr(v, "item"):
                        msg[k] = v.item()
                except Exception:
                    pass
            et = r["event_time"]
            if a.speed > 0 and prev_et is not None:
                try:
                    dt = (datetime.fromisoformat(str(et).replace("Z", "+00:00")) - datetime.fromisoformat(str(prev_et).replace("Z", "+00:00"))).total_seconds()
                    sleep_s = max(0, dt / a.speed)
                    if sleep_s > 0 and sleep_s < 5:
                        time.sleep(sleep_s)
                except Exception:
                    pass
            prev_et = et
            try:
                prod.send(topic, key=str(r["patient_id"]), value=msg).add_errback(_on_err)
            except Exception as e:
                raise SystemExit(f"cannot reach Kafka at {bootstrap}. Start infra first: docker compose up -d kafka (or make infra). detail: {e}")
            sent += 1
            if sent % 500 == 0:
                el = time.time() - t0
                print(f"sent {sent}/{len(df)} ({sent/el:.1f} msg/s)")
        prod.flush()
        el = time.time() - t0
        if errors:
            print(f"WARN {len(errors)} delivery errors, e.g. {errors[0][:200]}")
        print(f"DONE sent={sent} elapsed={el:.1f}s throughput={sent/max(el,1e-6):.1f} msg/s")
        if not a.loop:
            break
        print("looping...")
    prod.close()

def pd_isna(v) -> bool:
    try:
        import pandas as pd
        return bool(pd.isna(v))
    except Exception:
        return v is None

if __name__ == "__main__":
    main()
