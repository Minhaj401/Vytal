import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from vytals.data.clinicalscore import news2, qsofa  # noqa: E402


class News2Test(unittest.TestCase):
    def test_all_normal(self):
        self.assertEqual(news2(70, 98, 16, 36.8, 120), 0)

    def test_critical(self):
        self.assertEqual(news2(140, 88, 30, 39.5, 85), 3 + 3 + 3 + 2 + 3)

    def test_boundaries(self):
        self.assertEqual(news2(70, 98, 8, 36.8, 120), 3)      # rr <= 8
        self.assertEqual(news2(70, 93, 16, 36.8, 120), 2)     # spo2 92-93
        self.assertEqual(news2(70, 98, 16, 36.8, 220), 3)     # sbp >= 220
        self.assertEqual(news2(41, 98, 16, 36.8, 120), 1)     # hr 41-50
        self.assertEqual(news2(70, 98, 16, 38.5, 120), 1)     # temp 38.1-39.0

    def test_missing_scores_zero(self):
        self.assertEqual(news2(), 0)
        self.assertEqual(news2(hr=float("nan")), 0)


class QsofaTest(unittest.TestCase):
    def test_both_components(self):
        self.assertEqual(qsofa(22, 100), 2)

    def test_normal(self):
        self.assertEqual(qsofa(16, 120), 0)

    def test_missing(self):
        self.assertEqual(qsofa(), 0)


if __name__ == "__main__":
    unittest.main()
