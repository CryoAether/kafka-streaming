from collections import deque


class TelemetryDetector:
    def __init__(self, window_sec=5.0, volt_drop_thresh=0.8, descent_thresh=-4.0):
        self.window_sec = window_sec
        self.volt_drop_thresh = volt_drop_thresh
        self.descent_thresh = descent_thresh
        self.windows = {}

    def update(self, drone_id, ts, voltage, altitude):
        buf = self.windows.get(drone_id)
        if buf is None:
            buf = deque()
            self.windows[drone_id] = buf

        buf.append((ts, voltage, altitude))
        cutoff = ts - self.window_sec
        while len(buf) > 2 and buf[0][0] < cutoff:
            buf.popleft()

        oldest_ts, oldest_v, oldest_alt = buf[0]
        dt = ts - oldest_ts
        if dt < 0.1:
            return 0.0, 0.0, []

        volt_drop = oldest_v - voltage
        vert_vel = (altitude - oldest_alt) / dt

        faults = []
        if volt_drop >= self.volt_drop_thresh:
            faults.append("BATTERY_DROP")
        if vert_vel <= self.descent_thresh:
            faults.append("RAPID_DESCENT")

        return vert_vel, volt_drop, faults