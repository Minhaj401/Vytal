package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/Minhaj401/Vytal/internal/vitals"
)

func writeCSV(path string, recs []vitals.Record) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write(vitals.CanonicalCols); err != nil {
		return err
	}
	for _, r := range recs {
		row := []string{r.PatientID, vitals.EventTimeStr(r.EventTime), fmt.Sprintf("%d", r.Seq)}
		for _, v := range vitals.Vitals {
			fv := r.Float(v)
			if isNaN(fv) {
				row = append(row, "")
			} else {
				row = append(row, fmt.Sprintf("%.4f", fv))
			}
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

func isNaN(f float64) bool { return f != f }
