// Command vytal-makedata generates synthetic demo vitals CSV.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"
)

func main() {
	patients := flag.Int("patients", 12, "number of patients")
	minutes := flag.Int("minutes", 180, "minutes of history")
	out := flag.String("out", "data/vitals.csv", "output CSV")
	flag.Parse()

	rng := rand.New(rand.NewPCG(42, 0))
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	type prof struct {
		id   string
		arch string
	}
	profs := make([]prof, *patients)
	for i := range profs {
		pid := fmt.Sprintf("P%03d", i+1)
		rv := rng.Float64()
		arch := "stable"
		if rv >= 0.9 {
			arch = "critical"
		} else if rv >= 0.7 {
			arch = "deteriorate"
		}
		profs[i] = prof{pid, arch}
	}
	const freqS = 30
	nSteps := *minutes * 60 / freqS
	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"patient_id", "event_time", "seq", "hr", "spo2", "rr", "temp_c", "sbp", "dbp"})
	seq := 0
	rows := 0
	norm := func(mean, sd float64) float64 { return rng.NormFloat64()*sd + mean }
	for t := 0; t < nSteps; t++ {
		et := base.Add(time.Duration(t*freqS) * time.Second).Format("2006-01-02T15:04:05Z")
		drift := float64(t) / float64(nSteps)
		for _, p := range profs {
			seq++
			var hr, spo2, rr, tmp, sbp, dbp float64
			switch p.arch {
			case "stable":
				hr, spo2, rr, tmp, sbp, dbp = norm(76, 6), norm(97.2, 0.9), norm(16, 2), norm(36.9, 0.3), norm(122, 9), norm(78, 6)
			case "deteriorate":
				hr, spo2, rr, tmp, sbp, dbp = norm(76+35*drift, 7), norm(97.2-6*drift, 1.2), norm(16+10*drift, 2.5), norm(36.9+1.2*drift, 0.35), norm(122+30*drift, 11), norm(78+12*drift, 7)
			default:
				hr, spo2, rr, tmp, sbp, dbp = norm(118, 12), norm(88.5, 2.2), norm(27, 3.5), norm(38.6, 0.5), norm(165, 14), norm(102, 9)
			}
			vals := []*float64{&hr, &spo2, &rr, &tmp, &sbp, &dbp}
			strs := make([]string, 6)
			for j, vp := range vals {
				if rng.Float64() < 0.03 {
					strs[j] = ""
				} else {
					strs[j] = fmt.Sprintf("%.4f", *vp)
				}
			}
			_ = w.Write(append([]string{p.id, et, fmt.Sprintf("%d", seq)}, strs...))
			rows++
		}
	}
	fmt.Printf("wrote %s: %d rows, %d patients\n", *out, rows, *patients)
}
