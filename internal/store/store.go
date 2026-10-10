// Package store handles PostgreSQL persistence of risk scores.
package store

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/Minhaj401/Vytal/internal/config"
	_ "github.com/lib/pq"
)

const ddl = `
CREATE TABLE IF NOT EXISTS risk_scores (
  patient_id TEXT NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  window_start TIMESTAMPTZ,
  p_risk DOUBLE PRECISION,
  risk_score DOUBLE PRECISION,
  risk_level TEXT,
  PRIMARY KEY (patient_id, window_end)
);
CREATE TABLE IF NOT EXISTS vitals_raw (
  patient_id TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  seq BIGINT,
  hr DOUBLE PRECISION, spo2 DOUBLE PRECISION, rr DOUBLE PRECISION,
  temp_c DOUBLE PRECISION, sbp DOUBLE PRECISION, dbp DOUBLE PRECISION
);
`

// DSN builds a postgres connection string from config.
func DSN(cfg config.Config) string {
	p := cfg.Postgres
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable connect_timeout=5",
		p.Host, p.Port, p.DB, p.User, p.Password)
}

// Ensure creates tables if missing.
func Ensure(cfg config.Config) error {
	db, err := sql.Open("postgres", DSN(cfg))
	if err != nil {
		return err
	}
	defer db.Close()
	// lib/pq can run multi-statement strings, but split defensively
	for _, stmt := range strings.Split(ddl, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// Score is one row for risk_scores.
type Score struct {
	PatientID   string
	WindowEnd   string // RFC3339
	WindowStart string // RFC3339, may be empty
	PRisk       float64
	RiskScore   float64
	RiskLevel   string
}

// InsertScores appends rows, ignoring primary-key duplicates from replays.
func InsertScores(cfg config.Config, rows []Score) error {
	if len(rows) == 0 {
		return nil
	}
	db, err := sql.Open("postgres", DSN(cfg))
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO risk_scores
		(patient_id, window_end, window_start, p_risk, risk_score, risk_level)
		VALUES ($1, $2::timestamptz, NULLIF($3,'')::timestamptz, $4, $5, $6)
		ON CONFLICT (patient_id, window_end) DO NOTHING`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, r := range rows {
		if _, err := stmt.Exec(r.PatientID, r.WindowEnd, r.WindowStart, r.PRisk, r.RiskScore, r.RiskLevel); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}
