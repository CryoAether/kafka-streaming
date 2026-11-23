import argparse
import os
import signal
import time
import orjson
import redis
from confluent_kafka import Consumer, Producer
from detector import TelemetryDetector

TOPIC = "drone-telemetry"
DLQ_TOPIC = "drone-telemetry-dlq"
REQUIRED_FIELDS = ("drone_id", "ts", "voltage", "altitude")

running = True


def handle_sig(*_):
    global running
    running = False


def parse_packet(raw):
    data = orjson.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("payload is not a JSON object")
    for k in REQUIRED_FIELDS:
        if k not in data:
            raise ValueError(f"missing field: {k}")
    float(data["ts"])
    float(data["voltage"])
    float(data["altitude"])
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--group", default=os.getenv("KAFKA_GROUP", "telemetry-processors"))
    parser.add_argument("--redis-host", default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    parser.add_argument("--batch", type=int, default=250)
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    consumer = Consumer({
        "bootstrap.servers": args.broker,
        "group.id": args.group,
        "enable.auto.commit": False,
        "auto.offset.reset": "latest",
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 10,
    })
    consumer.subscribe([TOPIC])

    dlq = Producer({"bootstrap.servers": args.broker})
    r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=False)
    detector = TelemetryDetector()

    print(f"worker pid={os.getpid()} joined group={args.group} topic={TOPIC}")

    try:
        while running:
            msgs = consumer.consume(num_messages=args.batch, timeout=0.02)
            if not msgs:
                continue

            latest_by_drone = {}
            ok_count = 0
            dlq_count = 0

            for msg in msgs:
                if msg.error():
                    continue

                t0 = time.perf_counter()
                raw = msg.value()
                try:
                    pkt = parse_packet(raw)
                    did = str(pkt["drone_id"])
                    ts = float(pkt["ts"])
                    v = float(pkt["voltage"])
                    alt = float(pkt["altitude"])

                    vert_vel, volt_drop, faults = detector.update(did, ts, v, alt)
                    proc_ms = (time.perf_counter() - t0) * 1000.0

                    pkt["vert_vel"] = round(vert_vel, 3)
                    pkt["volt_drop_5s"] = round(volt_drop, 3)
                    pkt["faults"] = faults
                    pkt["partition"] = msg.partition()
                    pkt["proc_ms"] = round(proc_ms, 3)
                    pkt["ingest_ts"] = time.time()

                    latest_by_drone[did] = pkt
                    ok_count += 1
                except Exception as exc:
                    dlq.produce(
                        DLQ_TOPIC,
                        key=msg.key(),
                        value=raw,
                        headers=[("error", str(exc).encode("utf-8"))],
                    )
                    dlq_count += 1

            if dlq_count:
                dlq.flush(1.0)

            if latest_by_drone or dlq_count:
                pipe = r.pipeline(transaction=False)
                for did, state in latest_by_drone.items():
                    pipe.set(f"drone:state:{did}", orjson.dumps(state), ex=30)
                if ok_count:
                    pipe.incrby("telemetry:processed", ok_count)
                if dlq_count:
                    pipe.incrby("telemetry:dlq", dlq_count)
                pipe.execute()

            # commit only after sliding window checks, DLQ forwarding, and Redis writes succeed
            consumer.commit(asynchronous=False)
    finally:
        dlq.flush(2.0)
        consumer.close()


if __name__ == "__main__":
    main()