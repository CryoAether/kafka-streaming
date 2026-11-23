import argparse
import signal
import time
import orjson
from confluent_kafka import Producer
from pymavlink import mavutil

TOPIC = "drone-telemetry"
running = True


def handle_sig(*_):
    global running
    running = False


def set_msg_rate(conn, msg_id, hz):
    interval_us = int(1_000_000 / hz)
    conn.mav.command_long_send(
        conn.target_system,
        conn.target_component,
        mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL,
        0,
        msg_id,
        interval_us,
        0, 0, 0, 0, 0,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--conn", default="udpin:0.0.0.0:14550")
    parser.add_argument("--baud", type=int, default=57600)
    parser.add_argument("--drone-id", default="drone-real-01")
    parser.add_argument("--rate", type=float, default=30.0)
    parser.add_argument("--broker", default="localhost:9092")
    args = parser.parse_args()

    rate_hz = max(1.0, min(args.rate, 50.0))
    min_period = 1.0 / rate_hz

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    producer = Producer({
        "bootstrap.servers": args.broker,
        "linger.ms": 5,
        "compression.type": "lz4",
    })

    conn = mavutil.mavlink_connection(args.conn, baud=args.baud)
    print(f"waiting for heartbeat on {args.conn}...")
    conn.wait_heartbeat()
    print(f"connected sys={conn.target_system} comp={conn.target_component}")

    # cap radio telemetry bandwidth so control link stays clear
    set_msg_rate(conn, mavutil.mavlink.MAVLINK_MSG_ID_ATTITUDE, rate_hz)
    set_msg_rate(conn, mavutil.mavlink.MAVLINK_MSG_ID_VFR_HUD, rate_hz)
    set_msg_rate(conn, mavutil.mavlink.MAVLINK_MSG_ID_HIGHRES_IMU, rate_hz)
    set_msg_rate(conn, mavutil.mavlink.MAVLINK_MSG_ID_SYS_STATUS, min(rate_hz, 10.0))

    state = {
        "drone_id": args.drone_id,
        "source": "mavlink",
        "ts": 0.0,
        "voltage": 0.0,
        "altitude": 0.0,
        "climb": 0.0,
        "roll": 0.0,
        "pitch": 0.0,
        "yaw": 0.0,
        "ax": 0.0,
        "ay": 0.0,
        "az": 9.81,
    }

    key_bytes = args.drone_id.encode("utf-8")
    last_emit = 0.0
    dirty = False

    while running:
        msg = conn.recv_match(blocking=True, timeout=0.05)
        if msg is not None:
            mtype = msg.get_type()
            if mtype == "ATTITUDE":
                state["roll"] = msg.roll
                state["pitch"] = msg.pitch
                state["yaw"] = msg.yaw
                dirty = True
            elif mtype == "VFR_HUD":
                state["altitude"] = msg.alt
                state["climb"] = msg.climb
                dirty = True
            elif mtype == "HIGHRES_IMU":
                state["ax"] = msg.xacc
                state["ay"] = msg.yacc
                state["az"] = msg.zacc
                dirty = True
            elif mtype == "SYS_STATUS":
                if msg.voltage_battery != 65535:
                    state["voltage"] = msg.voltage_battery / 1000.0
                dirty = True

        now = time.time()
        if dirty and (now - last_emit) >= min_period:
            state["ts"] = now
            try:
                producer.produce(TOPIC, key=key_bytes, value=orjson.dumps(state))
                producer.poll(0)
            except BufferError:
                producer.poll(0.01)
            last_emit = now
            dirty = False

    producer.flush(2.0)


if __name__ == "__main__":
    main()