export const API = process.env.NEXT_PUBLIC_API || "";

export type LivePoint = {
  patient_id: string; event_time: string; seq?: number;
  hr?: number|null; spo2?: number|null; rr?: number|null;
  temp_c?: number|null; sbp?: number|null; dbp?: number|null;
  p_risk: number; risk_score: number; risk_level: "LOW"|"MODERATE"|"HIGH"|"CRITICAL";
};

export async function getPatients() {
  const r = await fetch(`${API}/api/patients`, { cache: "no-store" });
  return r.json();
}
export async function getLive(pid: string): Promise<LivePoint> {
  const r = await fetch(`${API}/api/patients/${pid}/live`, { cache: "no-store" });
  return r.json();
}
export async function getHistory(pid: string, limit=160) {
  const r = await fetch(`${API}/api/patients/${pid}/history?limit=${limit}`, { cache: "no-store" });
  return r.json();
}
export async function getAnalysis() {
  const r = await fetch(`${API}/api/analysis`, { cache: "no-store" });
  return r.json();
}

export function levelColor(l: string) {
  if (l === "CRITICAL") return "#ef4444";
  if (l === "HIGH") return "#f97316";
  if (l === "MODERATE") return "#eab308";
  return "#22c55e";
}
export function fmt(n?: number|null, d=1) {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return Number(n).toFixed(d);
}
