// Package risk loads trained GBM ensembles. The loader lives here (next to
// the scorer) so callers share one import.
package risk

import (
	"github.com/Minhaj401/Vytal/internal/gbm"
)

// LoadModel reads a GBM JSON model file. A missing or corrupt file yields a
// rules-fallback model and no error — the API must never crash on this.
func LoadModel(path string) (*Model, error) {
	b, err := gbm.Load(path)
	if err != nil {
		return &Model{}, nil
	}
	return &Model{Booster: b}, nil
}
