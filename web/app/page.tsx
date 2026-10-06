"use client";
import { useEffect, useState } from "react";
import Link from "next/link";
import { BarChart, Bar, Cell, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts";
import { levelColor, fmt } from "../lib/api";

function appleLevel(hex: string) {
  if (hex === "#ef4444") return "#FF3B30";
  if (hex === "#f97316") return "#FF9500";
  if (hex === "#eab308") return "#FFCC00";
  return "#34C759";
}

export default function Dashboard() {
  const [patients, setPatients] = useState<any[]>([]);
  const [analysis, setAnalysis] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  async function tick() {
    try {
      const [pRes, aRes] = await Promise.all([
        fetch("/api/patients", { cache: "no-store" }),
        fetch("/api/analysis", { cache: "no-store" }),
      ]);
      if (!pRes.ok || !aRes.ok) throw new Error(`API ${pRes.status}/${aRes.status}`);
      const [p, a] = await Promise.all([pRes.json(), aRes.json()]);
      setPatients(p.patients || []); setAnalysis(a); setError(null);
    } catch (e: any) {
      setError("API unreachable — start backend: make api (port 8000)");
    }
  }
  useEffect(() => { tick(); const t = setInterval(tick, 2000); return () => clearInterval(t); }, []);

  const dist = analysis ? Object.entries(analysis.counts || {}).map(([k, v]) => ({ name: k, value: v })) : [];
  const order = ["LOW", "MODERATE", "HIGH", "CRITICAL"];
  dist.sort((a: any, b: any) => order.indexOf(a.name) - order.indexOf(b.name));

  return (
    <div className="space-y-5">
      {error && <div className="apple-card px-4 py-3 text-sm text-[#FF3B30]">{error}</div>}

      {/* Apple Health-style large title */}
      <div>
        <h1 className="apple-title text-[34px] leading-tight">Summary</h1>
        <p className="apple-secondary text-[15px]">Live patient risk • Kafka → Spark → XGBoost • auto-refresh 2s</p>
      </div>

      {/* stat cards */}
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {[
          { label: "Patients", value: analysis?.n_patients ?? "—", sub: "vitals.raw live", color: "#1D1D1F" },
          { label: "Avg risk", value: analysis ? `${analysis.avg_risk}` : "—", sub: "0–100 score", color: "#1D1D1F" },
          { label: "Max risk", value: analysis ? `${analysis.max_risk}` : "—", sub: "highest now", color: "#1D1D1F" },
          { label: "Critical", value: analysis?.counts?.CRITICAL ?? 0, sub: "needs attention", color: Number(analysis?.counts?.CRITICAL ?? 0) > 0 ? "#FF3B30" : "#1D1D1F" },
        ].map((s, i) => (
          <div key={i} className="apple-card p-5">
            <div className="text-[13px] apple-secondary">{s.label}</div>
            <div className="apple-title mt-0.5 text-[34px]" style={{ color: s.color }}>{s.value}</div>
            <div className="text-[13px] apple-secondary">{s.sub}</div>
          </div>
        ))}
      </div>

      <div className="grid gap-3 lg:grid-cols-3">
        <div className="apple-card p-5 lg:col-span-2">
          <div className="mb-3 flex items-center justify-between">
            <h2 className="apple-title text-[20px]">Patients</h2>
            <span className="text-[13px] apple-secondary">tap for live stream</span>
          </div>
          <div className="scroll-thin max-h-[460px] space-y-2 overflow-auto pr-1">
            {patients.map((p) => {
              const c = appleLevel(levelColor(p.risk_level));
              return (
                <Link key={p.patient_id} href={`/patients/${p.patient_id}`}
                  className="flex items-center gap-3 rounded-2xl p-3 transition hover:bg-black/[0.04]" style={{ border: "1px solid rgba(0,0,0,0.06)" }}>
                  {/* activity-ring style dot */}
                  <div className="grid h-11 w-11 shrink-0 place-items-center rounded-full text-[13px] font-bold text-white" style={{ background: `conic-gradient(${c} ${p.risk_score}%, #E8E8ED 0)` }}>
                    <div className="grid h-8 w-8 place-items-center rounded-full bg-white text-[11px] text-black">{fmt(p.risk_score, 0)}</div>
                  </div>
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="text-[17px] font-semibold">{p.patient_id}</span>
                      <span className="rounded-full px-2 py-0.5 text-[12px] font-semibold" style={{ background: c + "1A", color: c }}>{p.risk_level}</span>
                    </div>
                    <div className="mt-1.5 h-1.5 overflow-hidden rounded-full bg-black/[0.08]">
                      <div className="h-full rounded-full" style={{ width: `${p.risk_score}%`, background: c }} />
                    </div>
                    <div className="mt-1 text-[13px] apple-secondary">HR {fmt(p.hr, 0)} • SpO₂ {fmt(p.spo2, 1)}% • {p.event_time}</div>
                  </div>
                  <div className="apple-title text-[22px]" style={{ color: c }}>{fmt(p.risk_score, 0)}</div>
                </Link>
              );
            })}
            {patients.length === 0 && <div className="p-8 text-center apple-secondary">connecting to live stream… start API: <code>make api</code></div>}
          </div>
        </div>

        <div className="space-y-3">
          <div className="apple-card p-5">
            <h2 className="apple-title mb-2 text-[20px]">Distribution</h2>
            <div className="h-52">
              <ResponsiveContainer>
                <BarChart data={dist}>
                  <XAxis dataKey="name" tick={{ fill: "#86868B", fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: "#86868B", fontSize: 11 }} allowDecimals={false} axisLine={false} tickLine={false} />
                  <Tooltip contentStyle={{ background: "#fff", border: "1px solid rgba(0,0,0,0.1)", borderRadius: 12 }} />
                  <Bar dataKey="value" radius={[8, 8, 8, 8]}>
                    {dist.map((d: any) => <Cell key={d.name} fill={appleLevel(levelColor(d.name))} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
          <div className="apple-card p-5">
            <h2 className="apple-title mb-1 text-[20px]">Needs attention</h2>
            {(analysis?.critical || []).map((c: any) => (
              <Link key={c.patient_id} href={`/patients/${c.patient_id}`} className="flex items-center justify-between border-b py-2.5 text-[15px] last:border-0" style={{ borderColor: "rgba(0,0,0,0.06)" }}>
                <span className="font-semibold">{c.patient_id}</span>
                <span className="apple-secondary text-[13px]">HR {fmt(c.hr, 0)} • SpO₂ {fmt(c.spo2, 0)}</span>
                <span className="font-bold" style={{ color: appleLevel(levelColor(c.risk_level)) }}>{fmt(c.risk_score, 0)}</span>
              </Link>
            ))}
            {(!analysis?.critical || analysis.critical.length === 0) && <div className="text-[15px] apple-secondary">All clear 🎉</div>}
          </div>
        </div>
      </div>

      <div className="apple-card p-5">
        <h2 className="apple-title text-[20px]">Pipeline</h2>
        <p className="mt-1 text-[14px] apple-secondary">Patient vitals → Kafka producer (key=patient_id, acks=all) → <code>vitals.raw</code> → Spark Structured Streaming (watermark 2m, window 5m/1m) → <code>build_features()</code> → XGBoost → Risk 0–100 → PostgreSQL → this dashboard + per-patient live routes.</p>
      </div>
    </div>
  );
}
