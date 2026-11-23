import argparse
import asyncio
import math
import random
import signal
import time
import orjson
from confluent_kafka import Producer

TOPIC = "drone-telemetry"
stop_event = asyncio.Event()


class VirtualDrone:
    def __init__(self, idx, base_alt):
        self.drone_id = f"drone-sim-{idx:02d}"
        self.key = self.drone_id.encode("utf-8")
        self.voltage = random.uniform(23.8, 25.0)
        self.altitude = base_alt
        self.target_alt = base_alt
        self.climb = 0.0
        self.phase = random.uniform(0, math.tau)
        self.fault_mode = None
        self.fault_until = 0.0

    def step(self, dt, now, fault_prob):
        if self.fault_mode is None and random.random() < fault_prob * dt:
            self.fault_mode = random.choice(["battery", "descent"])
            self.fault_until = now + random.uniform(2.0, 4.0)

        if self.fault_mode and now >= self.fault_until:
            self.fault_mode = None
            if self.voltage < 20.5:
                self.voltage = 24.4

        # gradual drain + occasional sag
        drain = 0.004 * dt
        if self.fault_mode == "battery":
            drain += 0.45 * dt
        self.voltage = max(18.0, self.voltage - drain)
        if self.voltage <= 18.5:
            self.voltage = 24.8

        # altitude motion + occasional rapid descent
        if self.fault_mode == "descent":
            self.climb = -6.5 + random.gauss(0.0, 0.3)
        else:
            self.target_alt = 25.0 + 10.0 * math.sin(now * 0.15 + self.phase)
            self.climb = max(-2.0, min(2.0, (self.target_alt - self.altitude) * 0.5))

        self.altitude = max(0.5, self.altitude + self.climb * dt)
        if self.altitude <= 1.0 and self.fault_mode == "descent":
            self.fault_mode = None
            self.altitude = 25.0

        return {
            "drone_id": self.drone_id,
            "source": "sim",
            "ts": now,
            "voltage": round(self.voltage + random.gauss(0.0, 0.015), 3),
            "altitude": round(self.altitude + random.gauss(0.0, 0.04), 3),
            "climb": round(self.climb + random.gauss(0.0, 0.05), 3),
            "roll": round(0.12 * math.sin(now * 0.8 + self.phase) + random.gauss(0.0, 0.01), 4),
            "pitch": round(0.10 * math.cos(now * 0.6 + self.phase) + random.gauss(0.0, 0.01), 4),
            "yaw": round(((now * 0.25 + self.phase) % math.tau) - math.pi, 4),
            "ax": round(random.gauss(0.0, 0.15), 3),
            "ay": round(random.gauss(0.0, 0.15), 3),
            "az": round(9.81 + random.gauss(0.0, 0.2), 3),
        }


async def run_drone(drone, producer, rate_hz, corrupt_rate, fault_prob, counter):
    slice_sec = 0.01
    per_slice = max(1, int(round(rate_hz * slice_sec)))
    sub_dt = slice_sec / per_slice
    next_tick = time.perf_counter()

    while not stop_event.is_set():
        now = time.time()
        for _ in range(per_slice):
            if corrupt_rate > 0.0 and random.random() < corrupt_rate:
                payload = b'{"drone_id":"bad","corrupt":\xff'
            else:
                payload = orjson.dumps(drone.step(sub_dt, now, fault_prob))

            try:
                producer.produce(TOPIC, key=drone.key, value=payload)
                counter[0] += 1
            except BufferError:
                producer.poll(0.005)
                break

        producer.poll(0)
        next_tick += slice_sec
        delay = next_tick - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        else:
            next_tick = time.perf_counter()
            await asyncio.sleep(0)


async def stats_printer(counter):
    last = 0
    while not stop_event.is_set():
        await asyncio.sleep(1.0)
        cur = counter[0]
        print(f"sim throughput: {cur - last:,} events/sec (total {cur:,})")
        last = cur


async def run_sim(args):
    producer = Producer({
        "bootstrap.servers": args.broker,
        "queue.buffering.max.messages": 200000,
        "batch.num.messages": 1000,
        "linger.ms": 5,
        "compression.type": "lz4",
    })

    counter = [0]
    drones = [VirtualDrone(i + 1, 15.0 + i * 2.5) for i in range(args.drones)]

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    print(f"starting {args.drones} simulated drones @ {args.rate} Hz each -> {args.broker}")
    tasks = [
        asyncio.create_task(
            run_drone(d, producer, args.rate, args.corrupt_rate, args.fault_prob, counter)
        )
        for d in drones
    ]
    tasks.append(asyncio.create_task(stats_printer(counter)))

    await stop_event.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    producer.flush(3.0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--drones", type=int, default=10)
    parser.add_argument("--rate", type=float, default=1000.0)
    parser.add_argument("--corrupt-rate", type=float, default=0.0005)
    parser.add_argument("--fault-prob", type=float, default=0.04)
    parser.add_argument("--broker", default="localhost:9092")
    args = parser.parse_args()
    asyncio.run(run_sim(args))


if __name__ == "__main__":
    main()