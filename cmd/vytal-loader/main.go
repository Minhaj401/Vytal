// Command vytal-loader prints canonical loader stats for a CSV/JSON/JSONL input.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"

	"github.com/Minhaj401/Vytal/internal/vitals"
)

func main() {
	input := flag.String("input", "", "input CSV/JSON/JSONL file")
	out := flag.String("out", "", "optional canonical CSV out")
	limit := flag.Int("limit", 5, "rows to preview")
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
	patients := map[string]bool{}
	for _, r := range recs {
		patients[r.PatientID] = true
	}
	fmt.Printf("rows=%d patients=%d cols=%v\n", len(recs), len(patients), vitals.CanonicalCols)
	for i := 0; i < len(recs) && i < *limit; i++ {
		r := recs[i]
		fmt.Printf("%s %s seq=%d hr=%v spo2=%v rr=%v temp_c=%v sbp=%v dbp=%v\n",
			r.PatientID, vitals.EventTimeStr(r.EventTime), r.Seq,
			num(r.Float("hr")), num(r.Float("spo2")), num(r.Float("rr")),
			num(r.Float("temp_c")), num(r.Float("sbp")), num(r.Float("dbp")))
	}
	miss := map[string]int{}
	for _, r := range recs {
		for _, v := range vitals.Vitals {
			if math.IsNaN(r.Float(v)) {
				miss[v]++
			}
		}
	}
	fmt.Print("missing%: {")
	for i, v := range vitals.Vitals {
		if i > 0 {
			fmt.Print(" ")
		}
		pct := 0.0
		if len(recs) > 0 {
			pct = float64(miss[v]) * 100 / float64(len(recs))
		}
		fmt.Printf("%s:%.1f", v, pct)
	}
	fmt.Println("}")
	if *out != "" {
		if err := writeCSV(*out, recs); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println("wrote", *out)
	}
}

func num(f float64) any {
	if math.IsNaN(f) {
		return "NA"
	}
	return fmt.Sprintf("%.2f", f)
}
