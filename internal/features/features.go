// Package features implements the shared windowed feature engineering.
// BuildFeatures is the single source of truth used by training, the replay
// engine, inference and the stream worker — mirroring the Python version.
package features

import (
	"math"
	"sort"
	"time"

	"github.com/Minhaj401/Vytal/internal/vitals"
)

// Vitals lists the six model input signals.
var Vitals = []string{"hr", "spo2", "rr", "temp_c", "sbp", "dbp"}

// FeatureCols is the model feature vector layout.
var FeatureCols []string

func init() {
	for _, v := range Vitals {
		if v == "temp_c" {
			FeatureCols = append(FeatureCols, v+"_current", v+"_mean", v+"_std", v+"_slope", v+"_delta")
		} else {
			FeatureCols = append(FeatureCols, v+"_current", v+"_mean", v+"_std", v+"_min", v+"_max", v+"_slope", v+"_delta")
		}
	}
	FeatureCols = append(FeatureCols, "n_rows", "coverage", "low_quality")
}

// Neutral fallbacks for missing current/mean/min/max (same as Python).
var neutral = map[string]float64{
	"hr": 75, "spo2": 97, "rr": 16, "temp_c": 37.0, "sbp": 120, "dbp": 78,
}

func neutralFor(col string) float64 {
	base := col
	if idx := index(col, "_"); idx >= 0 {
		base = col[:idx]
	}
	if len(col) >= 5 && col[:5] == "temp_" {
		base = "temp_c"
	}
	if f, ok := neutral[base]; ok {
		return f
	}
	return 0
}

func index(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// Window is one feature row: one patient, one trailing window.
type Window struct {
	PatientID string
	WindowEnd time.Time
	Vals      map[string]float64 // FeatureCols values
}

// slope returns the least-squares slope of y over unit-spaced x, ignoring NaN.
func slope(ys []float64) float64 {
	xs := []float64{}
	vs := []float64{}
	for i, y := range ys {
		if !math.IsNaN(y) {
			xs = append(xs, float64(i))
			vs = append(vs, y)
		}
	}
	n := float64(len(xs))
	if len(xs) < 2 {
		return 0
	}
	var sx, sy, sxx, sxy float64
	for i := range xs {
		sx += xs[i]
		sy += vs[i]
		sxx += xs[i] * xs[i]
		sxy += xs[i] * vs[i]
	}
	den := n*sxx - sx*sx
	if den == 0 {
		return 0
	}
	return (n*sxy - sx*sy) / den
}

// sampleStd matches pandas default (ddof=1); 0 when fewer than 2 values.
func sampleStd(vals []float64) float64 {
	xs := []float64{}
	for _, v := range vals {
		if !math.IsNaN(v) {
			xs = append(xs, v)
		}
	}
	if len(xs) < 2 {
		return 0
	}
	var mean float64
	for _, v := range xs {
		mean += v
	}
	mean /= float64(len(xs))
	var ss float64
	for _, v := range xs {
		ss += (v - mean) * (v - mean)
	}
	return math.Sqrt(ss / float64(len(xs)-1))
}

func meanOf(vals []float64) (float64, bool) {
	var s float64
	var n int
	for _, v := range vals {
		if !math.IsNaN(v) {
			s += v
			n++
		}
	}
	if n == 0 {
		return 0, false
	}
	return s / float64(n), true
}

// BuildFeatures groups records by patient (input must be time-ordered per
// patient or it sorts a copy) and emits one Window per patient.
func BuildFeatures(recs []vitals.Record) []Window {
	if len(recs) == 0 {
		return nil
	}
	// group preserving patient order
	groups := map[string][]vitals.Record{}
	order := []string{}
	for _, r := range recs {
		if _, ok := groups[r.PatientID]; !ok {
			order = append(order, r.PatientID)
		}
		groups[r.PatientID] = append(groups[r.PatientID], r)
	}
	sort.Strings(order)
	out := make([]Window, 0, len(order))
	for _, pid := range order {
		g := append([]vitals.Record(nil), groups[pid]...)
		sort.Slice(g, func(i, j int) bool { return g[i].EventTime.Before(g[j].EventTime) })
		n := len(g)
		w := Window{PatientID: pid, WindowEnd: g[n-1].EventTime, Vals: map[string]float64{}}
		w.Vals["n_rows"] = float64(n)
		var nonNull int
		for _, r := range g {
			for _, v := range Vitals {
				if !math.IsNaN(r.Float(v)) {
					nonNull++
				}
			}
		}
		coverage := float64(nonNull) / float64(n*len(Vitals))
		w.Vals["coverage"] = coverage
		lq := 0.0
		if coverage < 0.5 || n < 3 {
			lq = 1.0
		}
		w.Vals["low_quality"] = lq
		half := n / 2
		if half < 1 {
			half = 1
		}
		prev := g[:half]
		for _, v := range Vitals {
			series := make([]float64, n)
			for i, r := range g {
				series[i] = r.Float(v)
			}
			// current = last non-NaN
			cur := math.NaN()
			for i := n - 1; i >= 0; i-- {
				if !math.IsNaN(series[i]) {
					cur = series[i]
					break
				}
			}
			m, mok := meanOf(series)
			w.Vals[v+"_current"] = cur
			w.Vals[v+"_mean"] = m
			if !mok {
				w.Vals[v+"_mean"] = math.NaN()
			}
			w.Vals[v+"_std"] = sampleStd(series)
			if v != "temp_c" {
				mn, mx := math.NaN(), math.NaN()
				for _, x := range series {
					if math.IsNaN(x) {
						continue
					}
					if math.IsNaN(mn) || x < mn {
						mn = x
					}
					if math.IsNaN(mx) || x > mx {
						mx = x
					}
				}
				w.Vals[v+"_min"] = mn
				w.Vals[v+"_max"] = mx
			}
			w.Vals[v+"_slope"] = slope(series)
			pser := make([]float64, len(prev))
			for i, r := range prev {
				pser[i] = r.Float(v)
			}
			pm, pok := meanOf(pser)
			if pok && mok {
				w.Vals[v+"_delta"] = m - pm
			} else {
				w.Vals[v+"_delta"] = 0
			}
		}
		// neutral fill for current/mean/min/max, zero for the rest
		for _, c := range FeatureCols {
			if f, ok := w.Vals[c]; !ok || math.IsNaN(f) {
				if hasSuffix(c, "_current") || hasSuffix(c, "_mean") || hasSuffix(c, "_min") || hasSuffix(c, "_max") {
					w.Vals[c] = neutralFor(c)
				} else {
					w.Vals[c] = 0
				}
			}
		}
		out = append(out, w)
	}
	return out
}

func hasSuffix(s, suf string) bool {
	return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}

// Vector returns the feature vector in FeatureCols order.
func (w Window) Vector() []float64 {
	v := make([]float64, len(FeatureCols))
	for i, c := range FeatureCols {
		v[i] = w.Vals[c]
	}
	return v
}
