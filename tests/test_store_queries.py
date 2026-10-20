import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from vytals.store import queries  # noqa: E402

ROW = {
    "patient_id": "P012",
    "window_start": datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
    "window_end": datetime(2026, 1, 1, 10, 5, tzinfo=timezone.utc),
    "p_risk": 0.9712,
    "risk_score": 97.1,
    "risk_level": "CRITICAL",
    "hr": 120.0, "spo2": 88.0, "rr": 28.0,
    "temp_c": 38.6, "sbp": 150.0, "dbp": 95.0,
}


class QueriesTest(unittest.TestCase):
    def test_point_matches_live_contract(self):
        p = queries.point(ROW)
        self.assertEqual(p["patient_id"], "P012")
        self.assertEqual(p["event_time"], "2026-01-01T10:05:00+00:00")
        self.assertIsNone(p["seq"])
        self.assertEqual(p["risk_level"], "CRITICAL")
        self.assertEqual(p["hr"], 120.0)
        self.assertEqual(p["dbp"], 95.0)
        self.assertTrue(set(queries.VITALS) <= set(p))

    def test_point_missing_vitals_stay_none(self):
        row = {**ROW, "hr": None, "spo2": None, "temp_c": None}
        p = queries.point(row)
        self.assertIsNone(p["hr"])
        self.assertIsNone(p["spo2"])
        self.assertIsNone(p["temp_c"])

    def test_patient_contract_for_ranking(self):
        self.assertEqual(queries.patient(ROW), {
            "patient_id": "P012", "risk_score": 97.1, "risk_level": "CRITICAL",
            "hr": 120.0, "spo2": 88.0, "event_time": "2026-01-01T10:05:00+00:00",
        })


if __name__ == "__main__":
    unittest.main()
