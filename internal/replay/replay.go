// Package replay is the in-memory replay engine: it replays canonical CSV
// rows through event_time order, maintains per-patient trailing windows,
// scores them, and exposes latest/history for the API — no Kafka required.
package replay

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Minhaj401/Vytal/internal/features"
	"github.com/Minhaj401/Vytal/internal/risk"
	"github.com/Minhaj401/Vytal/internal/vitals"
)

// LivePoint is one scored live record served to the dashboard.
type LivePoint struct {
	PatientID string   `json:"patient_id"`
	EventTime string   `json:"event_time"`
	Seq       *int64   `json:"seq,omitempty"`
	HR        *float64 `json:"hr"`
	SpO2      *float64 `json:"spo2"`
	RR        *float64 `json:"rr"`
	TempC     *float64 `json:"temp_c"`
	SBP       *float64 `json:"sbp"`
	DBP       *float64 `json:"dbp"`
	PRisk     float64  `json:"p_risk"`
	RiskScore float64  `json:"risk_score"`
	RiskLevel string   `json:"risk_level"`
}

func fptr(f float64) *float64 {
	if math.IsNaN(f) {
		return nil
	}
	v := f
	return &v
}

// Replay holds the dataset cursor, per-patient buffers and scored outputs.
type Replay struct {
	mu      sync.RWMutex
	rows    []vitals.Record
	model   *risk.Model
	window  int
	idx     int
	buffers map[string][]vitals.Record
	Latest  map[string]LivePoint
	History map[string][]LivePoint
	running bool
}

// New loads the CSV and model, warming up with an initial step.
func New(csvPath, modelPath string, windowRows int) (*Replay, error) {
	rows, err := vitals.LoadAny(csvPath)
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].EventTime.Before(rows[j].EventTime) })
	m, err := risk.LoadModel(modelPath)
	if err != nil {
		m = &risk.Model{}
	}
	r := &Replay{
		rows:    rows,
		model:   m,
		window:  windowRows,
		buffers: map[string][]vitals.Record{},
		Latest:  map[string]LivePoint{},
		History: map[string][]LivePoint{},
	}
	r.Step(60)
	go r.loop(3, time.Second)
	return r, nil
}

// Step advances n records, scoring each patient's trailing window.
func (r *Replay) Step(n int) []LivePoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]LivePoint, 0, n)
	for k := 0; k < n; k++ {
		if len(r.rows) == 0 {
			break
		}
		if r.idx >= len(r.rows) {
			r.idx = 0
			r.buffers = map[string][]vitals.Record{}
		}
		rec := r.rows[r.idx]
		r.idx++
		buf := append(r.buffers[rec.PatientID], rec)
		if len(buf) > r.window {
			buf = buf[len(buf)-r.window:]
		}
		r.buffers[rec.PatientID] = buf
		ws := features.BuildFeatures(buf)
		var scored risk.Scored
		if len(ws) > 0 {
			scored = r.model.Predict(ws)[0]
		} else {
			scored = risk.Scored{PatientID: rec.PatientID, RiskScore: 0, RiskLevel: "LOW"}
		}
		lp := LivePoint{
			PatientID: rec.PatientID,
			EventTime: rec.EventTime.Format(time.RFC3339),
			HR:        fptr(rec.Float("hr")),
			SpO2:      fptr(rec.Float("spo2")),
			RR:        fptr(rec.Float("rr")),
			TempC:     fptr(rec.Float("temp_c")),
			SBP:       fptr(rec.Float("sbp")),
			DBP:       fptr(rec.Float("dbp")),
			PRisk:     scored.PRisk,
			RiskScore: scored.RiskScore,
			RiskLevel: scored.RiskLevel,
		}
		if rec.HasSeq {
			s := rec.Seq
			lp.Seq = &s
		}
		r.Latest[rec.PatientID] = lp
		h := append(r.History[rec.PatientID], lp)
		if len(h) > 400 {
			h = h[len(h)-400:]
		}
		r.History[rec.PatientID] = h
		out = append(out, lp)
	}
	return out
}

// Patients lists known patient IDs (live first, else dataset order).
func (r *Replay) Patients() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.Latest) > 0 {
		out := make([]string, 0, len(r.Latest))
		for pid := range r.Latest {
			out = append(out, pid)
		}
		sort.Strings(out)
		return out
	}
	seen := map[string]bool{}
	var out []string
	for _, rec := range r.rows {
		if !seen[rec.PatientID] {
			seen[rec.PatientID] = true
			out = append(out, rec.PatientID)
		}
	}
	sort.Strings(out)
	return out
}

func (r *Replay) loop(batch int, interval time.Duration) {
	r.mu.Lock()
	r.running = true
	r.mu.Unlock()
	for {
		func() {
			defer func() { _ = recover() }()
			r.Step(batch)
		}()
		time.Sleep(interval)
	}
}
