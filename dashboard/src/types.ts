export interface DroneState {
  drone_id: string;
  source: string;
  ts: number;
  voltage: number;
  altitude: number;
  climb: number;
  vert_vel: number;
  volt_drop_5s: number;
  roll: number;
  pitch: number;
  yaw: number;
  ax?: number;
  ay?: number;
  az?: number;
  faults: string[];
  partition: number;
  proc_ms: number;
  stale: boolean;
}

export interface FleetStats {
  msg_rate: number;
  total_processed: number;
  dlq_count: number;
  active_drones: number;
}

export interface TelemetrySnapshot {
  ts: number;
  redis_ok: boolean;
  stats: FleetStats;
  drones: DroneState[];
}

export interface HistoryPoint {
  v: number;
  alt: number;
}