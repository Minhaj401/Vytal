// Command vytal-server serves the FastAPI-compatible HTTP API for the
// Next.js dashboard: replay-backed live vitals, history and analysis.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/Minhaj401/Vytal/internal/config"
	"github.com/Minhaj401/Vytal/internal/replay"
)

var (
	mu     sync.Mutex
	eng    *replay.Replay
	engErr error
)

func repoFile(cand string) string {
	if _, err := os.Stat(cand); err == nil {
		return cand
	}
	if wd, err := os.Getwd(); err == nil {
		if p := filepath.Join(wd, cand); exists(p) {
			return p
		}
	}
	return cand
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func getReplay() (*replay.Replay, error) {
	mu.Lock()
	defer mu.Unlock()
	if eng != nil {
		return eng, nil
	}
	if engErr != nil {
		// retry once per call is wasteful; but CSV won't appear mid-run — keep error
		return nil, engErr
	}
	config.Load("")
	cand := os.Getenv("VYTALS_DATA")
	if cand == "" {
		cand = "data/vitals.csv"
	}
	p := repoFile(cand)
	if !exists(p) {
		if ms, _ := filepath.Glob("data/*.csv"); len(ms) > 0 {
			p = ms[0]
		}
	}
	if !exists(p) {
		engErr = fmt.Errorf("no vitals CSV found (tried %s). Generate one: go run ./cmd/vytal-makedata", cand)
		return nil, engErr
	}
	model := os.Getenv("VYTALS_MODEL")
	if model == "" {
		model = "models/risk_gbm.json"
	}
	mp := repoFile(model)
	if !exists(mp) {
		// back-compat: old python artifact name
		if exists(repoFile("models/xgb_risk.json")) && model == "models/risk_gbm.json" {
			mp = repoFile("models/xgb_risk.json")
		}
	}
	r, err := replay.New(p, mp, 30)
	if err != nil {
		engErr = err
		return nil, err
	}
	eng = r
	return eng, nil
}

func needReplay(w http.ResponseWriter) *replay.Replay {
	r, err := getReplay()
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": err.Error()})
		return nil
	}
	return r
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type patientRow struct {
	PatientID string   `json:"patient_id"`
	RiskScore float64  `json:"risk_score"`
	RiskLevel string   `json:"risk_level"`
	HR        *float64 `json:"hr"`
	SpO2      *float64 `json:"spo2"`
	EventTime string   `json:"event_time,omitempty"`
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		_, err := getReplay()
		writeJSON(w, http.StatusOK, map[string]any{"ok": err == nil, "error": errStr(err)})
	})

	mux.HandleFunc("/api/patients", func(w http.ResponseWriter, r *http.Request) {
		rp := needReplay(w)
		if rp == nil {
			return
		}
		out := []patientRow{}
		for _, pid := range rp.Patients() {
			live, ok := rp.Latest[pid]
			if !ok {
				out = append(out, patientRow{PatientID: pid, RiskLevel: "LOW"})
				continue
			}
			out = append(out, patientRow{PatientID: pid, RiskScore: live.RiskScore, RiskLevel: live.RiskLevel, HR: live.HR, SpO2: live.SpO2, EventTime: live.EventTime})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].RiskScore > out[j].RiskScore })
		writeJSON(w, http.StatusOK, map[string]any{"patients": out})
	})

	mux.HandleFunc("/api/patients/", func(w http.ResponseWriter, r *http.Request) {
		rest := r.URL.Path[len("/api/patients/"):]
		// {pid}/live or {pid}/history
		var pid, tail string
		if i := indexSlash(rest); i >= 0 {
			pid, tail = rest[:i], rest[i+1:]
		} else {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown route"})
			return
		}
		rp := needReplay(w)
		if rp == nil {
			return
		}
		switch tail {
		case "live":
			d, ok := rp.Latest[pid]
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown patient"})
				return
			}
			writeJSON(w, http.StatusOK, d)
		case "history":
			limit := 120
			if s := r.URL.Query().Get("limit"); s != "" {
				if n, err := strconv.Atoi(s); err == nil {
					limit = n
				}
			}
			if limit < 1 {
				limit = 1
			}
			if limit > 400 {
				limit = 400
			}
			h := rp.History[pid]
			if len(h) > limit {
				h = h[len(h)-limit:]
			}
			if h == nil {
				h = []replay.LivePoint{}
			}
			writeJSON(w, http.StatusOK, map[string]any{"patient_id": pid, "points": h})
		default:
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown route"})
		}
	})

	mux.HandleFunc("/api/analysis", func(w http.ResponseWriter, r *http.Request) {
		rp := needReplay(w)
		if rp == nil {
			return
		}
		if len(rp.Latest) == 0 {
			writeJSON(w, http.StatusOK, map[string]any{"counts": map[string]int{}, "avg_risk": 0, "critical": []any{}})
			return
		}
		counts := map[string]int{}
		var sum, mx float64
		type crit struct {
			PatientID string   `json:"patient_id"`
			RiskScore float64  `json:"risk_score"`
			RiskLevel string   `json:"risk_level"`
			HR        *float64 `json:"hr"`
			SpO2      *float64 `json:"spo2"`
		}
		var all []replay.LivePoint
		for _, v := range rp.Latest {
			all = append(all, v)
		}
		for _, v := range all {
			counts[v.RiskLevel]++
			sum += v.RiskScore
			if v.RiskScore > mx {
				mx = v.RiskScore
			}
		}
		sort.Slice(all, func(i, j int) bool { return all[i].RiskScore > all[j].RiskScore })
		top := []crit{}
		for i := 0; i < len(all) && i < 5; i++ {
			top = append(top, crit{all[i].PatientID, all[i].RiskScore, all[i].RiskLevel, all[i].HR, all[i].SpO2})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"counts": counts, "avg_risk": round1(sum / float64(len(all))), "max_risk": mx,
			"n_patients": len(all), "critical": top, "dist": counts,
		})
	})

	mux.HandleFunc("/api/advance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "POST only"})
			return
		}
		rp := needReplay(w)
		if rp == nil {
			return
		}
		steps := 10
		if s := r.URL.Query().Get("steps"); s != "" {
			if n, err := strconv.Atoi(s); err == nil {
				steps = n
			}
		}
		if steps < 1 {
			steps = 1
		}
		if steps > 200 {
			steps = 200
		}
		out := rp.Step(steps)
		var last any
		if len(out) > 0 {
			last = out[len(out)-1]
		}
		writeJSON(w, http.StatusOK, map[string]any{"advanced": len(out), "latest": last})
	})

	cfg := config.Load("")
	addr := fmt.Sprintf("%s:%d", cfg.API.Host, cfg.API.Port)
	log.Printf("vytals api on http://%s", addr)
	log.Fatal(http.ListenAndServe(addr, cors(mux)))
}

func errStr(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}

func indexSlash(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

func round1(f float64) float64 {
	if f < 0 {
		return 0
	}
	return float64(int(f*10+0.5)) / 10
}
