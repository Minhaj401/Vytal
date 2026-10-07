# Brag plan — Vytals

1. **What is it?** Blurb: real-time ICU vital monitor — Kafka ingest → Spark windows → XGBoost 0–100 risk score → Next.js live dashboard.
2. **Funniest/impressive claim?** "Two buttons. One score tells you who needs a nurse. 0–100."
3. **Visual hook?** Apple-clean white dashboard with ring gauges, red `CRITICAL 97` badges, live ECG-style HR chart on patient page.
4. **UI to show:** `/` dashboard (Summary cards + patient ranking + distribution) and `/patients/P006` (ring gauge, vitals cards, HR live trace, SpO₂+risk overlay, raw stream tail).
5. **Shortest satisfying:** ~18s. Hook 2s → reveal 3s → UI highlights 10s → CTA 3s.
6. **Tone:** `cinematic`; direction: calm ICU monitor energy, low bed, dry pulse SFX.
7. **Audio:** low music bed with pulse hits synced to "97" reveal and ECG spike. No voice.
8. **Caption:** "Every vital. Every patient. One score. Vytals — real-time risk 0–100."
9. **User flow:** vitals stream in → dashboard flips a patient to CRITICAL → nurse opens patient route for live trace.

## Colors (from globals.css)
- BG `#F5F5F7`, card `#FFFFFF`, text `#1D1D1F`, secondary `#86868B`
- Accents: blue `#0071E3`, green `#34C759`, yellow `#FFCC00`, orange `#FF9500`, red `#FF3B30`
- Font: SF stack (`-apple-system`, SF Pro-ish)

## Storyboard (18s)
| # | t | scene | text | SFX |
|---|---|-------|------|-----|
| 1 | 0–2s | black → red pulse dot + ECG line draws | "One heartbeat" (small) | soft ping |
| 2 | 2–5s | dashboard screenshot fades/scales in | "Vytals — live risk, 0–100" | whoosh |
| 3 | 5–9s | cut to P006 ring gauge (high risk) | "P006 flips CRITICAL 97" | heart-thud ×2 |
| 4 | 9–13s | HR live trace + SpO₂ overlay strip | "Every vital, 2s refresh" | subtle ECG beeps |
| 5 | 13–16s | raw stream tail scrolling | "Kafka → Spark → XGBoost → Postgres" | tick |
| 6 | 16–18s | black, logo `V` + CTA | "Vytals. Open the dashboard." | single dry hit |
