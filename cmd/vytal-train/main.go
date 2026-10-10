// Command vytal-train trains the GBM risk regressor on rule-generated labels.
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"

	"github.com/Minhaj401/Vytal/internal/features"
	"github.com/Minhaj401/Vytal/internal/gbm"
	"github.com/Minhaj401/Vytal/internal/risk"
	"github.com/Minhaj401/Vytal/internal/vitals"
)

// windowize slices each patient's history into window-minute buckets and
// builds one feature row per non-empty bucket.
func windowize(recs []vitals.Record, windowMin int) []features.Window {
	byPid := map[string][]vitals.Record{}
	var order []string
	for _, r := range recs {
		if _, ok := byPid[r.PatientID]; !ok {
			order = append(order, r.PatientID)
		}
		byPid[r.PatientID] = append(byPid[r.PatientID], r)
	}
	sort.Strings(order)
	var out []features.Window
	for _, pid := range order {
		g := byPid[pid]
		sort.Slice(g, func(i, j int) bool { return g[i].EventTime.Before(g[j].EventTime) })
		if len(g) == 0 {
			continue
		}
		buckets := map[int64][]vitals.Record{}
		var keys []int64
		for _, r := range g {
			k := r.EventTime.Unix() / int64(windowMin*60)
			if _, ok := buckets[k]; !ok {
				keys = append(keys, k)
			}
			buckets[k] = append(buckets[k], r)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, k := range keys {
			ws := features.BuildFeatures(buckets[k])
			out = append(out, ws...)
		}
	}
	return out
}

func main() {
	input := flag.String("input", "", "input CSV/JSON/JSONL")
	out := flag.String("out", "models/risk_gbm.json", "output model JSON")
	window := flag.Int("window", 5, "window minutes")
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
	pids := map[string]bool{}
	for _, r := range recs {
		pids[r.PatientID] = true
	}
	fmt.Printf("loaded %d rows, %d patients\n", len(recs), len(pids))
	F := windowize(recs, *window)
	if len(F) == 0 {
		fmt.Fprintln(os.Stderr, "no training windows — check input")
		os.Exit(1)
	}
	fmt.Printf("windows: %d features: %d\n", len(F), len(features.FeatureCols))
	y := make([]float64, len(F))
	rng := rand.New(rand.NewPCG(7, 0))
	for i, w := range F {
		y[i] = risk.RuleScore(w) + rng.NormFloat64()*2
		if y[i] < 0 {
			y[i] = 0
		}
		if y[i] > 100 {
			y[i] = 100
		}
	}
	var mean, sd, mn, mx float64
	mn, mx = y[0], y[0]
	for _, v := range y {
		mean += v
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}
	mean /= float64(len(y))
	for _, v := range y {
		sd += (v - mean) * (v - mean)
	}
	sd = math.Sqrt(sd / float64(len(y)))
	fmt.Printf("label mean=%.1f std=%.1f min=%.1f max=%.1f\n", mean, sd, mn, mx)

	X := make([][]float64, len(F))
	groups := make([]string, len(F))
	for i, w := range F {
		X[i] = w.Vector()
		groups[i] = w.PatientID
	}
	trIdx, teIdx := groupSplit(groups, 0.2, 42)
	fmt.Printf("train patients=%d test patients=%d\n", uniq(groups, trIdx), uniq(groups, teIdx))
	Xtr, ytr := pick(X, y, trIdx)
	Xte, yte := pick(X, y, teIdx)
	opt := gbm.DefaultOptions()
	best := math.MaxFloat64
	patience, bad := 30, 0
	model := gbm.Train(Xtr, ytr, Xte, yte, opt, func(round int, trMAE, teMAE float64) bool {
		if round%50 == 0 || round == 0 {
			fmt.Printf("[%d] train-mae:%.3f test-mae:%.3f\n", round, trMAE, teMAE)
		}
		if teMAE < best {
			best = teMAE
			bad = 0
		} else {
			bad++
		}
		return bad < patience
	})
	pred := make([]float64, len(yte))
	for i := range Xte {
		pred[i] = model.Predict(Xte[i])
	}
	fmt.Printf("MAE=%.2f R2=%.3f\n", gbm.MAE(pred, yte), gbm.R2(pred, yte))
	type kv struct {
		k string
		v float64
	}
	var imps []kv
	for i, g := range model.Gains {
		imps = append(imps, kv{features.FeatureCols[i], g})
	}
	sort.Slice(imps, func(a, b int) bool { return imps[a].v > imps[b].v })
	fmt.Print("top features: [")
	for i := 0; i < 10 && i < len(imps); i++ {
		if i > 0 {
			fmt.Print(" ")
		}
		fmt.Printf("(%s %.1f)", imps[i].k, imps[i].v)
	}
	fmt.Println("]")
	if err := os.MkdirAll(filepath.Dir(*out), 0755); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if err := model.Save(*out); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println("saved ->", *out)
}

// groupSplit shuffles patient groups with seed and holds out ~testFrac.
func groupSplit(groups []string, testFrac float64, seed int64) ([]int, []int) {
	uniqP := []string{}
	seen := map[string]bool{}
	for _, g := range groups {
		if !seen[g] {
			seen[g] = true
			uniqP = append(uniqP, g)
		}
	}
	rng := rand.New(rand.NewPCG(uint64(seed), 0))
	rng.Shuffle(len(uniqP), func(i, j int) { uniqP[i], uniqP[j] = uniqP[j], uniqP[i] })
	nTest := int(float64(len(uniqP)) * testFrac)
	if nTest < 1 && len(uniqP) > 1 {
		nTest = 1
	}
	test := map[string]bool{}
	for _, p := range uniqP[:nTest] {
		test[p] = true
	}
	var tr, te []int
	for i, g := range groups {
		if test[g] {
			te = append(te, i)
		} else {
			tr = append(tr, i)
		}
	}
	return tr, te
}

func uniq(groups []string, idx []int) int {
	s := map[string]bool{}
	for _, i := range idx {
		s[groups[i]] = true
	}
	return len(s)
}

func pick(X [][]float64, y []float64, idx []int) ([][]float64, []float64) {
	xo := make([][]float64, len(idx))
	yo := make([]float64, len(idx))
	for k, i := range idx {
		xo[k], yo[k] = X[i], y[i]
	}
	return xo, yo
}
