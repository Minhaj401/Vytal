// Command vytal-producer replays CSV/JSONL into Kafka topic vitals.raw.
// Key = patient_id, acks = all, original event_time preserved.
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
	"github.com/Minhaj401/Vytal/internal/vitals"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	input := flag.String("input", "", "input CSV/JSON/JSONL")
	topic := flag.String("topic", "", "kafka topic (default from config)")
	speed := flag.Float64("speed", 60, "1=realtime, 60=60x, 0=max throughput")
	bootstrap := flag.String("bootstrap", "", "kafka bootstrap servers")
	loop := flag.Bool("loop", false, "loop forever")
	flag.Parse()
	if *input == "" {
		fmt.Fprintln(os.Stderr, "--input required")
		os.Exit(2)
	}
	cfg := config.Load("")
	if *topic == "" {
		*topic = cfg.Kafka.Topic
	}
	if *bootstrap == "" {
		*bootstrap = cfg.Kafka.BootstrapServers
	}
	recs, err := vitals.LoadAny(*input)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].EventTime.Before(recs[j].EventTime) })
	fmt.Printf("replaying %d records -> %s/%s speed=%v\n", len(recs), *bootstrap, *topic, *speed)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*bootstrap, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(5),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.RequestTimeoutOverhead(8*time.Second),
	)
	if err != nil {
		fatalKafka(*bootstrap, err)
	}
	defer cl.Close()
	ctx := context.Background()
	// fail fast when broker unreachable
	if err := cl.Ping(ctx); err != nil {
		fatalKafka(*bootstrap, err)
	}
	sent := 0
	t0 := time.Now()
	var prev time.Time
	for {
		for _, r := range recs {
			msg := map[string]any{
				"patient_id": r.PatientID,
				"event_time": vitals.EventTimeStr(r.EventTime),
			}
			if r.HasSeq {
				msg["seq"] = r.Seq
			} else {
				msg["seq"] = nil
			}
			for _, v := range vitals.Vitals {
				f := r.Float(v)
				if math.IsNaN(f) {
					msg[v] = nil
				} else {
					msg[v] = f
				}
			}
			val, _ := json.Marshal(msg)
			if *speed > 0 && !prev.IsZero() {
				dt := r.EventTime.Sub(prev).Seconds() / *speed
				if dt > 0 && dt < 5 {
					time.Sleep(time.Duration(dt * float64(time.Second)))
				}
			}
			prev = r.EventTime
			rec := &kgo.Record{Topic: *topic, Key: []byte(r.PatientID), Value: val}
			if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
				fatalKafka(*bootstrap, err)
			}
			sent++
			if sent%500 == 0 {
				el := time.Since(t0).Seconds()
				fmt.Printf("sent %d/%d (%.1f msg/s)\n", sent, len(recs), float64(sent)/el)
			}
		}
		el := time.Since(t0).Seconds()
		fmt.Printf("DONE sent=%d elapsed=%.1fs throughput=%.1f msg/s\n", sent, el, float64(sent)/maxf(el, 1e-6))
		if !*loop {
			break
		}
		fmt.Println("looping...")
	}
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func fatalKafka(bootstrap string, err error) {
	fmt.Fprintf(os.Stderr, "cannot reach Kafka at %s. Start infra first (make infra). detail: %v\n", bootstrap, err)
	os.Exit(1)
}
