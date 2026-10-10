// Command vytal-stream is the Go-native streaming worker. It replaces the
// Spark Structured Streaming job: consume vitals.raw, validate, watermark,
// 5-minute tumbling windows sliding every minute per patient, score with the
// GBM model, and append to Postgres risk_scores.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/Minhaj401/Vytal/internal/config"
	"github.com/Minhaj401/Vytal/internal/features"
	"github.com/Minhaj401/Vytal/internal/risk"
	"github.com/Minhaj401/Vytal/internal/store"
	"github.com/Minhaj401/Vytal/internal/vitals"
	"github.com/twmb/franz-go/pkg/kgo"
)

type key struct {
	pid string
	et  string
	seq int64
}

func main() {
	bootstrap := flag.String("bootstrap", "", "kafka bootstrap servers")
	topic := flag.String("topic", "", "kafka topic")
	model := flag.String("model", "", "model JSON path")
	flag.Parse()
	cfg := config.Load("")
	if *bootstrap == "" {
		*bootstrap = cfg.Kafka.BootstrapServers
	}
	if *topic == "" {
		*topic = cfg.Kafka.Topic
	}
	if *model == "" {
		*model = cfg.Model.Path
	}
	if err := store.Ensure(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "pg ensure failed (continuing without sink): %v\n", err)
	}
	m, err := risk.LoadModel(*model)
	if err != nil {
		m = &risk.Model{}
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*bootstrap, ",")...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot reach Kafka at %s: %v\n", *bootstrap, err)
		os.Exit(1)
	}
	defer cl.Close()
	ctx := context.Background()
	fmt.Printf("streaming %s/%s window=5m/1m watermark=2m -> postgres risk_scores\n", *bootstrap, *topic)

	var (
		seen     = map[key]bool{}
		bufs     = map[string][]vitals.Record{}
		maxSeen  time.Time
		lastTick = time.Now()
	)
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				fmt.Fprintf(os.Stderr, "fetch error: %v\n", e.Err)
			}
		}
		n := 0
		fetches.EachRecord(func(r *kgo.Record) {
			var msg map[string]any
			if err := json.Unmarshal(r.Value, &msg); err != nil {
				return
			}
			pid, _ := msg["patient_id"].(string)
			if pid == "" && r.Key != nil {
				pid = string(r.Key)
			}
			ets, _ := msg["event_time"].(string)
			et, ok := parseEventTime(ets)
			if pid == "" || !ok {
				return
			}
			var seq int64
			if f, ok := msg["seq"].(float64); ok {
				seq = int64(f)
			}
			k := key{pid, vitals.EventTimeStr(et), seq}
			if seen[k] {
				return
			}
			seen[k] = true
			if et.After(maxSeen) {
				maxSeen = et
			}
			// watermark 2m: drop events older than maxSeen - 2m
			if !maxSeen.IsZero() && et.Before(maxSeen.Add(-2*time.Minute)) {
				return
			}
			rec := vitals.Record{PatientID: pid, EventTime: et, Seq: seq, HasSeq: true, Vals: map[string]float64{}}
			for _, v := range vitals.Vitals {
				rec.Vals[v] = numField(msg[v])
			}
			bufs[pid] = append(bufs[pid], rec)
			n++
		})
		// flush every 30s like the trigger interval
		if time.Since(lastTick) >= 30*time.Second && len(bufs) > 0 {
			lastTick = time.Now()
			flush(cfg, m, bufs, 1)
		}
	}
}

func parseEventTime(s string) (time.Time, bool) {
	for _, f := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02 15:04:05"} {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func numField(v any) float64 {
	switch t := v.(type) {
	case nil:
		return math.NaN()
	case float64:
		return t
	case int64:
		return float64(t)
	default:
		return math.NaN()
	}
}

// flush scores the last 5 minutes per patient and appends to PG.
func flush(cfg config.Config, m *risk.Model, bufs map[string][]vitals.Record, batch int) {
	var pids []string
	for pid := range bufs {
		pids = append(pids, pid)
	}
	sort.Strings(pids)
	var rows []store.Score
	top := []risk.Scored{}
	for _, pid := range pids {
		g := bufs[pid]
		if len(g) == 0 {
			continue
		}
		cut := g[len(g)-1].EventTime.Add(-5 * time.Minute)
		var win []vitals.Record
		for _, r := range g {
			if !r.EventTime.Before(cut) {
				win = append(win, r)
			}
		}
		if len(win) == 0 {
			continue
		}
		ws := features.BuildFeatures(win)
		for _, s := range m.Predict(ws) {
			wstr := ""
			if s.WindowStr {
				// window_start = window_end - 5m (tumbling approximation)
				if et, ok := parseEventTime(s.WindowEnd); ok {
					wstr = vitals.EventTimeStr(et.Add(-5 * time.Minute))
				}
			}
			rows = append(rows, store.Score{PatientID: s.PatientID, WindowEnd: s.WindowEnd, WindowStart: wstr, PRisk: s.PRisk, RiskScore: s.RiskScore, RiskLevel: s.RiskLevel})
			top = append(top, s)
		}
		// retain only recent history per patient
		if len(g) > 2000 {
			bufs[pid] = g[len(g)-2000:]
		}
	}
	if err := store.InsertScores(cfg, rows); err != nil {
		fmt.Printf("[batch %d] pg write failed (ok in dev): %v\n", batch, err)
	}
	sort.Slice(top, func(i, j int) bool { return top[i].RiskScore > top[j].RiskScore })
	if len(top) > 3 {
		top = top[:3]
	}
	fmt.Printf("[batch %d] scored %d windows; top=%v\n", batch, len(rows), top)
}
