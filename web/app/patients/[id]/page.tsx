"use client";
import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, AreaChart, Area, ReferenceLine } from "recharts";
import { levelColor, fmt, LivePoint } from "../../../lib/api";

function appleLevel(hex: string) {
  if (hex === "#ef4444") return "#FF3B30";
  if (hex === "#f97316") return "#FF9500";
  if (hex === "#eab308") return "#E5A000";
  return "#34C759";
}

function Vital({ label, value, unit, color, sub }: any) {
  return (
    <div className="apple-card p-4">
      <div className="text-[12px] apple-secondary">{label}</div>
      <div className="apple-title mt-0.5 text-[28px]" style={{ color }}>{value}<span className="ml-1 text-[14px] font-medium apple-secondary">{unit}</span></div>
      {sub && <div className="mt-0.5 text-[12px] apple-secondary">{sub}</div>}
    </div>
  );
}

export default function PatientPage() {
  const { id } = useParams() as { id: string };
  const [live, setLive] = useState<LivePoint | null>(null);
  const [hist, setHist] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let on = true;
    async function tick() {
      try {
        const pid = encodeURIComponent(id);
        const [lRes, hRes] = await Promise.all([
          fetch(`/api/patients/${pid}/live`, { cache: "no-store" }),
          fetch(`/api/patients/${pid}/history?limit=160`, { cache: "no-store" }),
        ]);
        if (lRes.status === 404) { if (on) setError(`Unknown patient ${id}`); return; }
        if (!lRes.ok || !hRes.ok) throw new Error(`API ${lRes.status}/${hRes.status}`);
        const [l, h] = await Promise.all([lRes.json(), hRes.json()]);
        if (!on) return;
        if ((l as any).error) { setError((l as any).error); return; }
        setLive(l);
        setHist((h.points || []).map((p: any) => ({ ...p, t: (p.event_time || "").slice(11, 19) })));
        setError(null);
      } catch {
        if (on) setError("API unreachable — start backend: make api");
      }
    }
    tick();
    const t = setInterval(tick, 2000);
    return () => { on = false; clearInterval(t); };
  }, [id]);

  const base = live ? levelColor(live.risk_level) : "#22c55e";
  const c = appleLevel(base);
  const hrData = hist.map(p => ({ t: p.t, hr: p.hr }));
  const spo2Data = hist.map(p => ({ t: p.t, spo2: p.spo2, risk: p.risk_score }));
  const R = 54, CIRC = 2 * Math.PI * R;
  const frac = live ? Math.min(1, live.risk_score / 100) : 0;

  return (
    <div className="space-y-4">
      <Link href="/" className="text-[15px] text-[#0071E3]">‹ All patients</Link>
      {error && <div className="apple-card px-4 py-3 text-sm text-[#FF3B30]">{error}</div>}

      {/* Apple Health-style header with activity ring gauge */}
      <div className="apple-card flex flex-wrap items-center gap-5 p-6">
        <div className="relative h-28 w-28">
          <svg viewBox="0 0 128 128" className="h-28 w-28 -rotate-90">
            <circle cx="64" cy="64" r={R} fill="none" stroke="#E8E8ED" strokeWidth="12" />
            <circle cx="64" cy="64" r={R} fill="none" stroke={c} strokeWidth="12" strokeLinecap="round"
              strokeDasharray={CIRC} strokeDashoffset={CIRC * (1 - frac)} style={{ transition: "stroke-dashoffset .6s, stroke .6s" }} />
          </svg>
          <div className="absolute inset-0 grid place-items-center">
            <div className="text-center">
              <div className="apple-title text-[28px] leading-none" style={{ color: c }}>{live ? fmt(live.risk_score, 0) : "—"}</div>
              <div className="text-[10px] apple-secondary">RISK</div>
            </div>
          </div>
        </div>
        <div className="flex-1">
          <h1 className="apple-title text-[28px]">Patient {id}</h1>
          <div className="mt-0.5 flex items-center gap-2 text-[13px] apple-secondary">
            <span className="pulse-dot inline-block h-2 w-2 rounded-full bg-green-500" />
            Live • 2s refresh • {live?.event_time} • seq {live?.seq ?? "—"}
          </div>
          <div className="apple-segmented mt-3 inline-flex gap-1 text-[13px] font-semibold">
            {["Vitals", "Trends", "Stream"].map((t, i) => (
              <span key={t} className={`rounded-[10px] px-3 py-1 ${i === 0 ? "bg-white shadow-sm" : "apple-secondary"}`}>{t}</span>
            ))}
          </div>
        </div>
        <div className="text-right">
          <div className="inline-block rounded-full px-3 py-1 text-[13px] font-bold" style={{ background: c + "1A", color: c }}>{live?.risk_level} • P={live ? fmt(live.p_risk, 3) : "—"}</div>
          <div className="mt-1 text-[12px] apple-secondary">XGBoost 0–100</div>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6">
        <Vital label="Heart rate" value={fmt(live?.hr, 0)} unit="bpm" color={live && (live.hr! > 100 || live.hr! < 50) ? "#FF3B30" : "#1D1D1F"} sub="60–100 normal" />
        <Vital label="SpO₂" value={fmt(live?.spo2, 1)} unit="%" color={live && live.spo2! < 94 ? "#FF3B30" : "#34C759"} sub="≥95% normal" />
        <Vital label="Resp rate" value={fmt(live?.rr, 0)} unit="/min" color="#1D1D1F" sub="12–20 normal" />
        <Vital label="Temp" value={fmt(live?.temp_c, 1)} unit="°C" color="#1D1D1F" sub="36.5–37.5" />
        <Vital label="Blood pressure" value={`${fmt(live?.sbp, 0)}/${fmt(live?.dbp, 0)}`} unit="mmHg" color="#1D1D1F" sub="120/80" />
        <Vital label="Samples" value={hist.length} unit="pts" color="#86868B" sub="sliding window" />
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        <div className="apple-card p-5">
          <h2 className="apple-title text-[17px]">Heart rate <span className="font-normal apple-secondary">— live</span></h2>
          <div className="h-64">
            <ResponsiveContainer>
              <LineChart data={hrData}>
                <XAxis dataKey="t" tick={{ fill: "#86868B", fontSize: 10 }} minTickGap={30} axisLine={false} tickLine={false} />
                <YAxis domain={["auto", "auto"]} tick={{ fill: "#86868B", fontSize: 11 }} axisLine={false} tickLine={false} width={36} />
                <Tooltip contentStyle={{ background: "#fff", border: "1px solid rgba(0,0,0,0.1)", borderRadius: 12 }} />
                <ReferenceLine y={100} stroke="#FF3B30" strokeDasharray="4 4" />
                <ReferenceLine y={60} stroke="#FFCC00" strokeDasharray="4 4" />
                <Line type="monotone" dataKey="hr" stroke="#0071E3" strokeWidth={2.2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
        <div className="apple-card p-5">
          <h2 className="apple-title text-[17px]">SpO₂ + risk <span className="font-normal apple-secondary">— overlay</span></h2>
          <div className="h-64">
            <ResponsiveContainer>
              <AreaChart data={spo2Data}>
                <XAxis dataKey="t" tick={{ fill: "#86868B", fontSize: 10 }} minTickGap={30} axisLine={false} tickLine={false} />
                <YAxis yAxisId="l" domain={[80, 100]} tick={{ fill: "#86868B", fontSize: 11 }} axisLine={false} tickLine={false} width={32} />
                <YAxis yAxisId="r" orientation="right" domain={[0, 100]} tick={{ fill: "#86868B", fontSize: 11 }} axisLine={false} tickLine={false} width={32} />
                <Tooltip contentStyle={{ background: "#fff", border: "1px solid rgba(0,0,0,0.1)", borderRadius: 12 }} />
                <ReferenceLine yAxisId="l" y={94} stroke="#FF3B30" strokeDasharray="4 4" />
                <Area yAxisId="l" type="monotone" dataKey="spo2" stroke="#34C759" fill="#34C75922" strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line yAxisId="r" type="monotone" dataKey="risk" stroke={c} strokeWidth={1.5} dot={false} strokeDasharray="6 3" isAnimationActive={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      <div className="apple-card p-5">
        <h2 className="apple-title text-[17px]">Live stream <span className="font-normal apple-secondary text-[13px]">(Kafka vitals.raw → this route)</span></h2>
        <div className="scroll-thin mt-2 max-h-64 overflow-auto rounded-2xl bg-black/[0.04] p-3 font-mono text-[12px] leading-relaxed">
          {[...hist].reverse().slice(0, 40).map((p: any, i: number) => (
            <div key={i} className="whitespace-nowrap text-[#1D1D1F]">
              <span className="apple-secondary">{p.event_time}</span>{" "}
              <span className="font-bold">{p.patient_id}</span>{" "}
              hr={fmt(p.hr, 1)} spo2={fmt(p.spo2, 1)} rr={fmt(p.rr, 1)} t={fmt(p.temp_c, 1)} {fmt(p.sbp, 0)}/{fmt(p.dbp, 0)}{" "}
              <span style={{ color: appleLevel(levelColor(p.risk_level)) }} className="font-bold">risk={fmt(p.risk_score, 1)} {p.risk_level}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
