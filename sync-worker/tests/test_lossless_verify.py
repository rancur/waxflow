"""Tests for the lossless-verification gate (fake-FLAC / transcode detection).

Uses ffmpeg to synthesize fixtures at run time:
  * a FULL-bandwidth signal encoded as FLAC  -> a genuine lossless file (should PASS)
  * the same signal hard low-passed at 15 kHz -> simulates an MP3/AAC transcode
    re-wrapped as FLAC (a fake) that MUST be caught by the spectral gate (FAIL)
  * an MP3 encode of the signal                -> not a lossless container (FAIL)

Skipped automatically if ffmpeg or numpy is unavailable.
"""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

SYNC_WORKER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SYNC_WORKER_DIR not in sys.path:
    sys.path.insert(0, SYNC_WORKER_DIR)

from tasks.lossless_verify import verify_lossless, spectral_cutoff, ffprobe_audio  # noqa: E402


def _have(cmd):
    return shutil.which(cmd) is not None


def _have_numpy():
    try:
        import numpy  # noqa: F401
        return True
    except Exception:
        return False


@unittest.skipUnless(_have("ffmpeg") and _have("ffprobe"), "ffmpeg/ffprobe required")
class TestLosslessVerify(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="llv_test_")
        cls.genuine = os.path.join(cls.dir, "genuine.flac")
        cls.fake = os.path.join(cls.dir, "fake_lowpass.flac")
        cls.mp3 = os.path.join(cls.dir, "lossy.mp3")
        # 6s of full-bandwidth pink-ish noise, 44.1k/16-bit stereo
        base = [
            "ffmpeg", "-v", "error", "-y",
            "-f", "lavfi", "-i", "anoisesrc=d=6:c=pink:r=44100:a=0.5",
            "-ac", "2", "-ar", "44100", "-sample_fmt", "s16",
        ]
        subprocess.run(base + [cls.genuine], check=True)
        # fake: brickwall low-pass at 15 kHz then store as FLAC (looks lossless, isn't)
        subprocess.run(
            ["ffmpeg", "-v", "error", "-y", "-i", cls.genuine,
             "-af", ("lowpass=f=15000:poles=2,lowpass=f=15000:poles=2,"
                     "lowpass=f=15000:poles=2,lowpass=f=15000:poles=2,lowpass=f=15000:poles=2"),
             "-ar", "44100", "-sample_fmt", "s16", cls.fake],
            check=True,
        )
        subprocess.run(
            ["ffmpeg", "-v", "error", "-y", "-i", cls.genuine, "-c:a", "libmp3lame",
             "-b:a", "128k", cls.mp3],
            check=True,
        )

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_probe_reports_flac(self):
        p = ffprobe_audio(self.genuine)
        self.assertEqual(p["codec"], "flac")
        self.assertGreaterEqual(p["sample_rate"], 44100)

    @unittest.skipUnless(_have_numpy(), "numpy required for spectral analysis")
    def test_spectral_distinguishes_cutoff(self):
        g = spectral_cutoff(self.genuine, 44100)
        f = spectral_cutoff(self.fake, 44100)
        self.assertIsNotNone(g)
        self.assertIsNotNone(f)
        # genuine keeps energy near Nyquist; fake cuts hard at ~15 kHz
        self.assertGreater(g["cutoff_hz"], 19000)
        self.assertLess(f["cutoff_hz"], 17000)

    @unittest.skipUnless(_have_numpy(), "numpy required for spectral analysis")
    def test_genuine_flac_passes(self):
        r = verify_lossless(self.genuine, expected_duration_ms=6000)
        self.assertTrue(r["passed"], msg=f"reasons={r['reasons']}")
        self.assertEqual(r["checks"]["spectral_verdict"], "pass")

    @unittest.skipUnless(_have_numpy(), "numpy required for spectral analysis")
    def test_fake_lowpass_flac_rejected(self):
        r = verify_lossless(self.fake, expected_duration_ms=6000)
        self.assertFalse(r["passed"])
        self.assertEqual(r["checks"]["spectral_verdict"], "fail")
        self.assertTrue(any("transcode" in x or "fake" in x for x in r["reasons"]))

    def test_mp3_container_rejected(self):
        # An MP3 is not a lossless container — must fail regardless of numpy.
        r = verify_lossless(self.mp3, expected_duration_ms=6000)
        self.assertFalse(r["passed"])
        self.assertTrue(any("lossless codec" in x for x in r["reasons"]))

    def test_duration_mismatch_rejected(self):
        # Correct lossless file but wrong expected duration => wrong-track guard.
        r = verify_lossless(self.genuine, expected_duration_ms=180000)
        self.assertFalse(r["passed"])
        self.assertTrue(any("duration mismatch" in x for x in r["reasons"]))

    def test_missing_file_rejected(self):
        r = verify_lossless("/nonexistent/nope.flac", expected_duration_ms=6000)
        self.assertFalse(r["passed"])


if __name__ == "__main__":
    unittest.main()
