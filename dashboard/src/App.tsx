import { useEffect, useRef, useState } from "react";
import { DroneState, HistoryPoint, TelemetrySnapshot } from "./types";

const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws/telemetry";
const MAX_POINTS = 100;

const EMPTY_SNAP: TelemetrySnapshot = {
  ts: 0,
  redis_ok: false,
  stats: { msg_rate: 0, total_processed: 0, dlq_count: 0, active_drones: 0 },
  drones: []
};

function Sparkline({ values, color, unit }: { values: number[]; color: string; unit: string }) {
  if (values.length < 2) {
    return <svg className="spark" viewBox="0 0 300 76" />;
  }

  let min = Math.min(...values);
  let max = Math.max(...values);
  if (max - min < 0.01) {
    min -= 0.5;
    max += 0.5;
  }

  const pts = values
    .map((v, i) => {
      const x = (i / (MAX_POINTS - 1)) * 296 + 2;
      const y = 72 - ((v - min) / (max - min)) * 66;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <div>
      <div className="chart-title mono">
        <span>min {min.toFixed(2)}{unit}</span>
        <span>max {max.toFixed(2)}{unit}</span>
      </div>
      <svg className="spark" viewBox="0 0 300 76" preserveAspectRatio="none">
        <polyline fill="none" stroke={color} strokeWidth="2" points={pts} />
      </svg>
    </div>
  );
}

export default function App() {
  const [snap, setSnap] = useState<TelemetrySnapshot>(EMPTY_SNAP);
  const [wsStatus, setWsStatus] = useState<"connecting" | "live" | "disconnected">("connecting");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const historyRef = useRef<Record<string, HistoryPoint[]>>({});

  useEffect(() => {
    let ws: WebSocket | null = null;
    let timer: number | null = null;
    let alive = true;

    const connect = () => {
      if (!alive) return;
      setWsStatus("connecting");
      ws = new WebSocket(WS_URL);

      ws.onopen = () => {
        if (alive) setWsStatus("live");
      };

      ws.onmessage = (ev) => {
        const data: TelemetrySnapshot = JSON.parse(ev.data);
        const hist = historyRef.current;

        // append 100ms snapshots for sparklines
        for (const d of data.drones) {
          let arr = hist[d.drone_id];
          if (!arr) {
            arr = [];
            hist[d.drone_id] = arr;
          }
          arr.push({ v: d.voltage, alt: d.altitude });
          if (arr.length > MAX_POINTS) arr.shift();
        }
        setSnap(data);
      };

      ws.onclose = () => {
        if (!alive) return;
        setWsStatus("disconnected");
        timer = window.setTimeout(connect, 2000);
      };

      ws.onerror = () => {
        ws?.close();
      };
    };

    connect();
    return () => {
      alive = false;
      if (timer !== null) clearTimeout(timer);
      ws?.close();
    };
  }, []);

  const activeSelection: DroneState | undefined =
    snap.drones.find((d) => d.drone_id === selectedId) || snap.drones[0];
  const activeHistory = activeSelection ? historyRef.current[activeSelection.drone_id] || [] : [];

  return (
    <div className="shell">
      <div className="topbar">
        <div className="title-row">
          <h1>Fleet Telemetry</h1>
          <span className={`pill mono ${wsStatus === "live" ? "ok" : "err"}`}>
            WS {wsStatus.toUpperCase()}
          </span>
          <span className={`pill mono ${snap.redis_ok ? "ok" : "warn"}`}>
            REDIS {snap.redis_ok ? "ONLINE" : "OFFLINE"}
          </span>
        </div>
        <div className="mono" style={{ color: "#8492a6", fontSize: 12 }}>
          100ms snapshot poll
        </div>
      </div>

      <div className="metrics">
        <div className="card">
          <div className="card-label">Throughput</div>
          <div className="card-val mono">{snap.stats.msg_rate.toLocaleString()} ev/s</div>
        </div>
        <div className="card">
          <div className="card-label">Active Drones</div>
          <div className="card-val mono">{snap.stats.active_drones}</div>
        </div>
        <div className="card">
          <div className="card-label">Processed</div>
          <div className="card-val mono">{snap.stats.total_processed.toLocaleString()}</div>
        </div>
        <div className="card">
          <div className="card-label">DLQ Routed</div>
          <div
            className="card-val mono"
            style={{ color: snap.stats.dlq_count > 0 ? "#f39c12" : "#e6ebf2" }}
          >
            {snap.stats.dlq_count.toLocaleString()}
          </div>
        </div>
      </div>

      <div className="layout">
        <div className="table-wrap">
          {snap.drones.length === 0 ? (
            <div className="empty-state">
              <h2>No telemetry streaming</h2>
              <div>
                {wsStatus !== "live"
                  ? "Start the FastAPI WebSocket gateway to receive snapshots."
                  : "Gateway is live and waiting for Kafka consumer updates in Redis."}
              </div>
              <div className="cmd-box mono">
                <div>docker compose up -d</div>
                <div>python consumer/worker.py</div>
                <div>python sim/fleet_sim.py --drones 10 --rate 1000</div>
              </div>
            </div>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Drone</th>
                  <th>Part</th>
                  <th>Voltage</th>
                  <th>Altitude</th>
                  <th>Vz (5s)</th>
                  <th>Proc</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody className="mono">
                {snap.drones.map((d) => {
                  const hasFault = d.faults.length > 0;
                  const isSel = activeSelection?.drone_id === d.drone_id;
                  const rowCls = [
                    isSel ? "selected" : "",
                    hasFault ? "fault-row" : "",
                    d.stale ? "stale-row" : ""
                  ].join(" ");

                  return (
                    <tr key={d.drone_id} className={rowCls} onClick={() => setSelectedId(d.drone_id)}>
                      <td>
                        {d.drone_id}{" "}
                        {d.source === "mavlink" && <span className="pill accent">REAL</span>}
                      </td>
                      <td>P{d.partition}</td>
                      <td>{d.voltage.toFixed(2)} V</td>
                      <td>{d.altitude.toFixed(1)} m</td>
                      <td>{d.vert_vel >= 0 ? "+" : ""}{d.vert_vel.toFixed(2)} m/s</td>
                      <td>{d.proc_ms.toFixed(2)} ms</td>
                      <td>
                        {d.stale ? (
                          <span className="pill warn">STALE</span>
                        ) : hasFault ? (
                          d.faults.map((f) => (
                            <span key={f} className="pill err" style={{ marginRight: 4 }}>
                              {f}
                            </span>
                          ))
                        ) : (
                          <span className="pill ok">OK</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        <div className="detail-panel">
          {!activeSelection ? (
            <div style={{ color: "#8492a6", padding: "12px 0" }}>
              Select a drone once telemetry arrives to inspect attitude and 10s rolling traces.
            </div>
          ) : (
            <>
              <div className="detail-head">
                <h2 className="mono">{activeSelection.drone_id}</h2>
                <span className="pill mono">P{activeSelection.partition}</span>
              </div>

              <div className="kv-grid mono">
                <div className="kv">
                  <span>Roll / Pitch</span>
                  {(activeSelection.roll * 57.2958).toFixed(1)} / {(activeSelection.pitch * 57.2958).toFixed(1)} deg
                </div>
                <div className="kv">
                  <span>Yaw</span>
                  {(activeSelection.yaw * 57.2958).toFixed(1)} deg
                </div>
                <div className="kv">
                  <span>5s Volt Delta</span>
                  -{activeSelection.volt_drop_5s.toFixed(2)} V
                </div>
                <div className="kv">
                  <span>Decode + Window</span>
                  {activeSelection.proc_ms.toFixed(2)} ms
                </div>
              </div>

              <div className="chart-block">
                <div className="card-label">Battery Voltage (10s)</div>
                <Sparkline values={activeHistory.map((p) => p.v)} color="#3b82f6" unit="V" />
              </div>

              <div className="chart-block">
                <div className="card-label">Altitude (10s)</div>
                <Sparkline values={activeHistory.map((p) => p.alt)} color="#2ecc71" unit="m" />
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}