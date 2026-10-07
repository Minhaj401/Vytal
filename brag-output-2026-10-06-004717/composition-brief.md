# Hyperframes Composition Brief: Vytals

## Objective
Create a short launch-style brag video for Vytals.

## Output
- Composition directory: `/home/minhaj/vytal/brag-output-2026-10-06-004717/composition/`
- Rendered video: `/home/minhaj/vytal/brag-output-2026-10-06-004717/brag.mp4`
- Format: landscape — 1920x1080
- Duration: 18 seconds

## Source Material
- Project root: /home/minhaj/vytal
- Primary files read: brag-plan.md, screenshots/apple-dash2.png, screenshots/apple-patient.png
- Product name: Vytals
- Tagline / strongest claim: "Every vital. Every patient. One score. Vytals — real-time risk 0–100."
- Key UI or visual moment to recreate: Summary dashboard (12 patients, avg 31.4, max 98.4, 2 critical), P006 patient page with ring gauge, vitals cards, HR live trace, SpO₂+risk overlay, raw stream tail
- Copy that must appear verbatim:
  - Vytals — live risk, 0–100
  - P006 flips CRITICAL
  - Every vital, 2s refresh
  - Kafka → Spark → XGBoost → Postgres
  - Vytals. Open the dashboard.

## Creative Direction
- Tone preset: cinematic
- Creative direction: calm ICU monitor energy, low bed, dry pulse SFX
- Interpretation: restrained pacing, dark-to-light act structure, red accents reserved for critical beats, no voice
- Angle: vitals stream in → dashboard flips a patient to CRITICAL → nurse opens patient route for live trace
- Hook: first 2 seconds — black, red pulse dot, ECG line draws, "One heartbeat"
- Outro / punchline: black, red ECG mark, "Vytals. Open the dashboard."
- Avoid:
  - Generic SaaS language
  - Abstract filler visuals
  - Unrelated visual redesign

## Visual Identity
- Background: #F5F5F7 (light), #000000 (hook/outro)
- Text: #1D1D1F on light, #F5F5F7 on black, secondary #86868B
- Accent: red #FF3B30, blue #0071E3, green #34C759
- Display font: -apple-system, SF Pro-ish stack
- Body font: same system stack
- Visual references from the project: apple-dash2.png, apple-patient.png

## Storyboard
Scene summary (from brag-plan.md):
1. One heartbeat — 2s — black, red pulse dot + ECG line draws, small caption "One heartbeat"
2. Dashboard reveal — 3s — dashboard screenshot fades/scales in, caption "Vytals — live risk, 0–100"
3. P006 critical — 4s — patient header crop with ring gauge + CRITICAL badge, caption "P006 flips CRITICAL"
4. HR trace + SpO₂ — 4s — HR live trace crop with SpO₂ overlay strip, caption "Every vital, 2s refresh"
5. Raw stream — 3s — live stream tail scrolling, caption "Kafka → Spark → XGBoost → Postgres"
6. Outro — 2s — black, red V mark + "Vytals. Open the dashboard."

## Audio
- Audio role: cinematic support
- Audio arc: quiet hook → music bed enters with dashboard reveal → sustained under UI highlights → drops to near-silence for final dry hit
- Music: happy-beats-business-moves-vol-1-by-ende-dot-app.mp3
- Music treatment: low bed (~0.3), fade-in from hook, duck under final scene
- Music cue guidance: cues JSON at composition/.hyperframes/; strong beats 16.02/17.02/18.02s — logo landing beat-locked to 16.02s
- Audio-reactive treatment: subtle — bass (bands[0]) breathes glow behind dashboard card and ECG dot scale; no waveform/EQ visuals
- Audio-coupled moments:
  - Logo hit at 16.02s — beat-locked (// beat-locked: 16.02s)
  - Card row entrance at ~2.5s — snap to beat grid (// beat-grid)
- SFX selection guidance: none selected; music bed carries the mix, final scene intentionally dry except music tail
- SFX analysis guidance: not used
- Exact SFX choice: none
- Audio files: composition/assets/music/happy-beats-business-moves-vol-1-by-ende-dot-app.mp3, composition/assets/music/audio-data.json

## Hyperframes Instructions
Load hyperframes-core, hyperframes-animation, hyperframes-creative, hyperframes-keyframes, hyperframes-cli. Build standalone composition/index.html with one paused GSAP timeline, clip data-start/data-duration windows, real screenshots from assets/img, audio track with id, per-frame audio-data sampling for subtle bass reactivity, logo beat-locked to 16.02s strong cue. Run `npx hyperframes check` (single gate) before render. Duration 18s, 1920x1080.
