// Package gbm implements a small gradient-boosted regression-tree ensemble
// (squared error, exact greedy splits, depth-limited trees) in pure Go.
// It trains the Vytals risk regressor on rule-generated labels — the same
// training setup as the Python XGBoost pipeline, without the dependency.
package gbm

import (
	"encoding/json"
	"os"
	"sort"
)

// Node is one regression-tree node. Leaf nodes carry Value.
type Node struct {
	Feature   int     `json:"feature"`
	Threshold float64 `json:"threshold"`
	Left      *Node   `json:"left,omitempty"`
	Right     *Node   `json:"right,omitempty"`
	Value     float64 `json:"value"`
	Leaf      bool    `json:"leaf"`
	Gain      float64 `json:"gain,omitempty"`
}

// Ensemble is a boosted set of trees with a base score and learning rate.
type Ensemble struct {
	Base  float64   `json:"base"`
	LR    float64   `json:"lr"`
	Trees []*Node   `json:"trees"`
	Gains []float64 `json:"gains"`
}

// Predict returns the raw regression output.
func (e *Ensemble) Predict(x []float64) float64 {
	s := e.Base
	for _, t := range e.Trees {
		s += e.LR * predictNode(t, x)
	}
	return s
}

func predictNode(n *Node, x []float64) float64 {
	for !n.Leaf {
		if n.Feature < len(x) && x[n.Feature] <= n.Threshold {
			n = n.Left
		} else {
			n = n.Right
		}
	}
	return n.Value
}

// Save writes the ensemble as JSON.
func (e *Ensemble) Save(path string) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

// Load reads an ensemble JSON file. Missing/corrupt files return an error
// so callers can fall back to the rules engine.
func Load(path string) (*Ensemble, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var e Ensemble
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	if len(e.Trees) == 0 {
		return nil, errNoTrees
	}
	return &e, nil
}

var errNoTrees = errString("gbm: no trees in model file")

type errString string

func (e errString) Error() string { return string(e) }

// Options tunes training.
type Options struct {
	Rounds   int
	MaxDepth int
	LR       float64
	MinChild int
	Seed     int64
}

// DefaultOptions mirrors the Python pipeline shape (300 rounds, depth 5).
func DefaultOptions() Options {
	return Options{Rounds: 300, MaxDepth: 5, LR: 0.08, MinChild: 4, Seed: 42}
}

// Train fits residuals with least-squares trees. evalFn, when non-nil, is
// called each round with (round, trainMAE, testMAE); returning false stops.
func Train(X [][]float64, y []float64, Xt [][]float64, yt []float64, opt Options, evalFn func(round int, trainMAE, testMAE float64) bool) *Ensemble {
	base := mean(y)
	e := &Ensemble{Base: base, LR: opt.LR, Gains: make([]float64, len(X[0]))}
	resid := make([]float64, len(y))
	for i := range y {
		resid[i] = y[i] - base
	}
	// current full predictions for eval
	full := make([]float64, len(y))
	for i := range full {
		full[i] = base
	}
	var fullT []float64
	if len(Xt) > 0 {
		fullT = make([]float64, len(yt))
		for i := range fullT {
			fullT[i] = base
		}
	}
	for r := 0; r < opt.Rounds; r++ {
		tree := fitTree(X, resid, opt.MaxDepth, opt.MinChild, e.Gains)
		e.Trees = append(e.Trees, tree)
		for i := range X {
			d := predictNode(tree, X[i])
			resid[i] -= opt.LR * d
			full[i] += opt.LR * d
		}
		if len(Xt) > 0 {
			for i := range Xt {
				fullT[i] += opt.LR * predictNode(tree, Xt[i])
			}
		}
		if evalFn != nil {
			tr := mae(full, y)
			te := 0.0
			if len(Xt) > 0 {
				te = mae(fullT, yt)
			}
			if !evalFn(r, tr, te) {
				break
			}
		}
	}
	return e
}

func mean(v []float64) float64 {
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

// MAE computes mean absolute error.
func MAE(pred, y []float64) float64 { return mae(pred, y) }

func mae(pred, y []float64) float64 {
	var s float64
	for i := range y {
		d := pred[i] - y[i]
		if d < 0 {
			d = -d
		}
		s += d
	}
	return s / float64(len(y))
}

// R2 computes the coefficient of determination.
func R2(pred, y []float64) float64 {
	m := mean(y)
	var ss, st float64
	for i := range y {
		d := pred[i] - y[i]
		ss += d * d
		t := y[i] - m
		st += t * t
	}
	if st == 0 {
		return 0
	}
	return 1 - ss/st
}

type idxVal struct {
	i int
	v float64
}

func fitTree(X [][]float64, resid []float64, maxDepth, minChild int, gains []float64) *Node {
	idx := make([]int, len(X))
	for i := range idx {
		idx[i] = i
	}
	return buildNode(X, resid, idx, maxDepth, minChild, gains)
}

func buildNode(X [][]float64, resid []float64, idx []int, depth, minChild int, gains []float64) *Node {
	var sum float64
	for _, i := range idx {
		sum += resid[i]
	}
	value := sum / float64(len(idx))
	node := &Node{Value: value, Leaf: true}
	if depth <= 0 || len(idx) < 2*minChild {
		return node
	}
	nFeat := len(X[0])
	bestGain := 0.0
	bestF, bestT := -1, 0.0
	var bestL, bestR []int
	for f := 0; f < nFeat; f++ {
		ivs := make([]idxVal, len(idx))
		for k, i := range idx {
			ivs[k] = idxVal{i, X[i][f]}
		}
		sort.Slice(ivs, func(a, b int) bool { return ivs[a].v < ivs[b].v })
		var tot float64
		for _, iv := range ivs {
			tot += resid[iv.i]
		}
		var lSum float64
		for k := 0; k < len(ivs)-1; k++ {
			iv := ivs[k]
			lSum += resid[iv.i]
			lN := float64(k + 1)
			rN := float64(len(ivs) - k - 1)
			if lN < float64(minChild) || rN < float64(minChild) {
				continue
			}
			if ivs[k].v == ivs[k+1].v {
				continue
			}
			rSum := tot - lSum
			// squared-error gain: SS(parent) - SS(left) - SS(right)
			gain := (lSum*lSum/lN + rSum*rSum/rN) - (tot * tot / float64(len(ivs)))
			if gain > bestGain {
				bestGain = gain
				bestF = f
				bestT = (ivs[k].v + ivs[k+1].v) / 2
				bestL = bestL[:0]
				bestR = bestR[:0]
				for _, jv := range ivs {
					if jv.v <= bestT {
						bestL = append(bestL, jv.i)
					} else {
						bestR = append(bestR, jv.i)
					}
				}
			}
		}
	}
	if bestF < 0 {
		return node
	}
	node.Leaf = false
	node.Feature = bestF
	node.Threshold = bestT
	node.Gain = bestGain
	if bestF < len(gains) {
		gains[bestF] += bestGain
	}
	node.Left = buildNode(X, resid, append([]int(nil), bestL...), depth-1, minChild, gains)
	node.Right = buildNode(X, resid, append([]int(nil), bestR...), depth-1, minChild, gains)
	return node
}
