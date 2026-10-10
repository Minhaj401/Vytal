// Package vitals implements the canonical vital-record schema and the
// tolerant CSV/JSON/JSONL loader. Missing values are preserved as NaN and
// never fabricated — same contract as the original Python loader.
package vitals

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// CanonicalCols is the canonical schema.
var CanonicalCols = []string{"patient_id", "event_time", "seq", "hr", "spo2", "rr", "temp_c", "sbp", "dbp"}

// Vitals lists the six model input signals.
var Vitals = []string{"hr", "spo2", "rr", "temp_c", "sbp", "dbp"}

// Aliases maps canonical columns to accepted source names.
var Aliases = map[string][]string{
	"patient_id": {"patient_id", "subject_id", "patient", "id", "hadm_id"},
	"event_time": {"event_time", "timestamp", "time", "datetime", "charttime"},
	"seq":        {"seq", "seqno", "row", "index"},
	"hr":         {"hr", "heart_rate", "heartrate", "HR"},
	"spo2":       {"spo2", "SpO2", "spo_2", "o2sat", "oxygen"},
	"rr":         {"rr", "resp_rate", "respiratory_rate", "RR", "resp"},
	"temp_c":     {"temp_c", "temp", "temperature", "Temp", "temp_f"},
	"sbp":        {"sbp", "sys", "systolic", "SBP", "nibp_s", "abp_s"},
	"dbp":        {"dbp", "dias", "diastolic", "DBP", "nibp_d", "abp_d"},
}

// Ranges holds physiological plausibility bounds; out-of-range becomes missing.
var Ranges = map[string][2]float64{
	"hr":     {20, 250},
	"spo2":   {50, 100},
	"rr":     {4, 60},
	"temp_c": {30, 43},
	"sbp":    {50, 280},
	"dbp":    {20, 180},
}

// Record is one canonical vital record. Floats use NaN for missing.
type Record struct {
	PatientID string
	EventTime time.Time
	Seq       int64
	HasSeq    bool
	Vals      map[string]float64 // hr, spo2, rr, temp_c, sbp, dbp (NaN = missing)
}

// Float returns the vital value or NaN when absent.
func (r Record) Float(v string) float64 {
	if f, ok := r.Vals[v]; ok {
		return f
	}
	return math.NaN()
}

func pick(cols []string, canon string) string {
	for _, a := range Aliases[canon] {
		for _, c := range cols {
			if c == a {
				return c
			}
		}
	}
	lower := map[string]string{}
	for _, c := range cols {
		lower[strings.ToLower(c)] = c
	}
	for _, a := range Aliases[canon] {
		if c, ok := lower[strings.ToLower(a)]; ok {
			return c
		}
	}
	return ""
}

var timeFormats = []string{
	time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z",
	"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02",
	"01/02/2006 15:04:05", "01/02/2006",
}

func parseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	for _, f := range timeFormats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func parseFloat(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "nan") || strings.EqualFold(s, "none") || strings.EqualFold(s, "null") {
		return math.NaN()
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return math.NaN()
	}
	return f
}

// ToCanonical maps raw string rows (column name -> value) into canonical records.
func ToCanonical(cols []string, rows []map[string]string) ([]Record, error) {
	if pick(cols, "patient_id") == "" {
		return nil, fmt.Errorf("input needs a patient id column (patient_id/subject_id/id)")
	}
	if pick(cols, "event_time") == "" {
		return nil, fmt.Errorf("input needs a timestamp column (event_time/timestamp/time)")
	}
	srcOf := map[string]string{}
	for _, canon := range CanonicalCols {
		srcOf[canon] = pick(cols, canon)
	}
	hasTempF := false
	for _, c := range cols {
		if c == "temp_f" {
			hasTempF = true
		}
	}
	out := make([]Record, 0, len(rows))
	for _, row := range rows {
		pid := ""
		if s := srcOf["patient_id"]; s != "" {
			pid = strings.TrimSpace(row[s])
		}
		if pid == "" || strings.EqualFold(pid, "none") || strings.EqualFold(pid, "null") {
			continue
		}
		ets := ""
		if s := srcOf["event_time"]; s != "" {
			ets = row[s]
		}
		et, ok := parseTime(ets)
		if !ok {
			continue
		}
		rec := Record{PatientID: pid, EventTime: et, Vals: map[string]float64{}}
		for _, v := range Vitals {
			f := math.NaN()
			if s := srcOf[v]; s != "" {
				f = parseFloat(row[s])
			}
			if lo, hi := Ranges[v][0], Ranges[v][1]; !math.IsNaN(f) && (f < lo || f > hi) {
				f = math.NaN()
			}
			rec.Vals[v] = f
		}
		// temp_f -> temp_c when temp_c entirely absent for this row
		if hasTempF && math.IsNaN(rec.Vals["temp_c"]) {
			if tf := parseFloat(row["temp_f"]); !math.IsNaN(tf) {
				rec.Vals["temp_c"] = (tf - 32) * 5.0 / 9.0
				if lo, hi := Ranges["temp_c"][0], Ranges["temp_c"][1]; rec.Vals["temp_c"] < lo || rec.Vals["temp_c"] > hi {
					rec.Vals["temp_c"] = math.NaN()
				}
			}
		}
		if s := srcOf["seq"]; s != "" {
			if sv := strings.TrimSpace(row[s]); sv != "" {
				if n, err := strconv.ParseInt(sv, 10, 64); err == nil {
					rec.Seq, rec.HasSeq = n, true
				} else if f, err := strconv.ParseFloat(sv, 64); err == nil && !math.IsNaN(f) {
					rec.Seq, rec.HasSeq = int64(f), true
				}
			}
		}
		out = append(out, rec)
	}
	// sort by patient, then time (matches Python loader)
	sort.Slice(out, func(i, j int) bool {
		if out[i].PatientID != out[j].PatientID {
			return out[i].PatientID < out[j].PatientID
		}
		return out[i].EventTime.Before(out[j].EventTime)
	})
	// fill seq per patient when entirely absent
	anySeq := false
	for _, r := range out {
		if r.HasSeq {
			anySeq = true
			break
		}
	}
	if !anySeq {
		counts := map[string]int64{}
		for i := range out {
			counts[out[i].PatientID]++
			out[i].Seq = counts[out[i].PatientID]
			out[i].HasSeq = true
		}
	}
	return out, nil
}

// LoadAny reads CSV or JSON/JSONL into canonical records.
func LoadAny(path string) ([]Record, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".csv":
		return loadCSV(path)
	case ".json", ".jsonl", ".ndjson":
		return loadJSON(path)
	default:
		return nil, fmt.Errorf("unsupported format: %s (use csv/json/jsonl)", ext)
	}
}

func loadCSV(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	for i := range header {
		header[i] = strings.TrimSpace(header[i])
	}
	var rows []map[string]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		m := map[string]string{}
		for i, h := range header {
			if i < len(rec) {
				m[h] = rec[i]
			}
		}
		rows = append(rows, m)
	}
	return ToCanonical(header, rows)
}

func loadJSON(path string) ([]Record, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(string(b))
	var raws []map[string]any
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal(b, &raws); err != nil {
			return nil, err
		}
	} else {
		for _, line := range strings.Split(trimmed, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				return nil, err
			}
			raws = append(raws, m)
		}
	}
	if len(raws) == 0 {
		return []Record{}, nil
	}
	cols := []string{}
	seen := map[string]bool{}
	for _, m := range raws {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				cols = append(cols, k)
			}
		}
	}
	rows := make([]map[string]string, 0, len(raws))
	for _, m := range raws {
		r := map[string]string{}
		for k, v := range m {
			switch t := v.(type) {
			case nil:
				r[k] = ""
			case string:
				r[k] = t
			case float64:
				r[k] = strconv.FormatFloat(t, 'f', -1, 64)
			case bool:
				r[k] = strconv.FormatBool(t)
			default:
				bb, _ := json.Marshal(t)
				r[k] = string(bb)
			}
		}
		rows = append(rows, r)
	}
	return ToCanonical(cols, rows)
}

// EventTimeStr formats in the canonical Zulu layout.
func EventTimeStr(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}
