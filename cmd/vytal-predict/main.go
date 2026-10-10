// Command vytal-predict runs batch inference over the latest window per patient.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/Minhaj401/Vytal/internal/features"
	"github.com/Minhaj401/Vytal/internal/risk"
	"github.com/Minhaj401/Vytal/internal/vitals"
)

func main() {
	input := flag.String("input", "", "input CSV/JSON/JSONL")
	model := flag.String("model", "models/risk_gbm.json", "model JSON")
	limit := flag.Int("limit", 20, "rows to show")
	flag.Parse()
	if *input == "" {
		fmt.Fprintln(os.Stderr, "--input required")
		os.Exit(2)
	}
	recs, err := vitals.LoadAny(*input)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	m, err := risk.LoadModel(*model)
	if err != nil {
		m = &risk.Model{}
	}
	byPid := map[string][]vitals.Record{}
	for _, r := range recs {
		byPid[r.PatientID] = append(byPid[r.PatientID], r)
	}
	var all []risk.Scored
	for pid, g := range byPid {
		sort.Slice(g, func(i, j int) bool { return g[i].EventTime.Before(g[j].EventTime) })
		if len(g) > 30 {
			g = g[len(g)-30:]
		}
		_ = pid
		ws := features.BuildFeatures(g)
		all = append(all, m.Predict(ws)...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].RiskScore > all[j].RiskScore })
	fmt.Printf("%-10s %-7s %-6s %-10s %s\n", "patient_id", "p_risk", "score", "level", "window_end")
	for i := 0; i < len(all) && i < *limit; i++ {
		s := all[i]
		fmt.Printf("%-10s %-7.4f %-6.1f %-10s %s\n", s.PatientID, s.PRisk, s.RiskScore, s.RiskLevel, s.WindowEnd)
	}
}
