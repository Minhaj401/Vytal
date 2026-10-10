// Package risk implements the transparent physiology rule engine, risk
// levels, and GBM-backed scoring. The rules double as training labels and
// as the safe fallback scorer — identical math to the Python implementation.
package risk

import (
	"math"

	"github.com/Minhaj401/Vytal/internal/features"
	"github.com/Minhaj401/Vytal/internal/gbm"
)

// Levels maps score upper bounds to level names.
func RiskLevel(score float64) string {
	switch {
	case score < 25:
		return "LOW"
	case score < 50:
		return "MODERATE"
	case score < 75:
		return "HIGH"
	default:
		return "CRITICAL"
	}
}

// PToScore converts P(risk) to a 0–100 score.
func PToScore(p float64) float64 {
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	return math.Round(p*1000) / 10
}

func featVal(w features.Window, name string, def float64) float64 {
	if f, ok := w.Vals[name]; ok && !math.IsNaN(f) {
		return f
	}
	return def
}

// RuleScore scores one window with transparent physiology thresholds.
func RuleScore(w features.Window) float64 {
	hr := featVal(w, "hr_current", 75)
	spo2 := featVal(w, "spo2_current", 97)
	rr := featVal(w, "rr_current", 16)
	tmp := featVal(w, "temp_c_current", 37)
	sbp := featVal(w, "sbp_current", 120)
	dbp := featVal(w, "dbp_current", 78)
	hrM := featVal(w, "hr_mean", 75)
	spo2M := featVal(w, "spo2_mean", 97)
	s := 0.0
	switch {
	case hr > 120:
		s += 22
	case hr > 100:
		s += 12
	case hr < 40:
		s += 25
	case hr < 50:
		s += 12
	}
	switch {
	case spo2 < 85:
		s += 30
	case spo2 < 90:
		s += 20
	case spo2 < 94:
		s += 10
	}
	switch {
	case rr > 30 || rr < 8:
		s += 18
	case rr > 24 || rr < 10:
		s += 9
	}
	switch {
	case tmp >= 39 || tmp < 35:
		s += 16
	case tmp >= 38 || tmp < 36:
		s += 8
	}
	switch {
	case sbp > 180 || sbp < 80:
		s += 18
	case sbp > 160 || sbp < 90:
		s += 9
	}
	switch {
	case dbp > 120 || dbp < 40:
		s += 12
	case dbp > 100 || dbp < 50:
		s += 6
	}
	s += clip(featVal(w, "hr_std", 0)/15*8, 0, 10)
	s += clip(featVal(w, "spo2_std", 0)/3*8, 0, 10)
	s += clip(math.Abs(featVal(w, "hr_slope", 0))*12, 0, 8)
	s += clip(math.Abs(featVal(w, "sbp_slope", 0))*6, 0, 8)
	if featVal(w, "low_quality", 0) > 0 {
		s += 5
	}
	if hrM > 110 || hrM < 50 {
		s += 6
	}
	if spo2M < 92 {
		s += 8
	}
	return clip(s, 0, 100)
}

func clip(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// Scored is one scored window.
type Scored struct {
	PatientID string
	PRisk     float64
	RiskScore float64
	RiskLevel string
	WindowEnd string
	WindowStr bool
}

// Model scores windows with an optional trained GBM; without one it falls
// back to the transparent rules (same as the Python fallback path).
type Model struct {
	Booster *gbm.Ensemble
}

// ScoreProba returns P(risk) for a window.
func (m *Model) ScoreProba(w features.Window) float64 {
	if m != nil && m.Booster != nil {
		raw := m.Booster.Predict(w.Vector())
		if raw > 1.5 {
			raw /= 100
		}
		return clip(raw, 0, 1)
	}
	return clip(RuleScore(w)/100, 0, 1)
}

// Predict scores windows into Scored rows.
func (m *Model) Predict(ws []features.Window) []Scored {
	out := make([]Scored, 0, len(ws))
	for _, w := range ws {
		p := m.ScoreProba(w)
		s := PToScore(p)
		sc := Scored{PatientID: w.PatientID, PRisk: math.Round(p*10000) / 10000, RiskScore: s, RiskLevel: RiskLevel(s)}
		if !w.WindowEnd.IsZero() {
			sc.WindowEnd = w.WindowEnd.UTC().Format("2006-01-02T15:04:05Z")
			sc.WindowStr = true
		}
		out = append(out, sc)
	}
	return out
}
