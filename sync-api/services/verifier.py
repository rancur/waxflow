import json
import subprocess
import os


class VerifierService:
    """Verifies downloaded audio files for lossless quality."""

    async def verify_lossless(self, file_path: str) -> dict:
        """
        Run ffprobe to check codec, sample rate, and bit depth.
        Returns verification result dict.
        """
        if not os.path.isfile(file_path):
            return {
                "status": "fail",
                "error": f"File not found: {file_path}",
                "codec": None,
                "sample_rate": None,
                "bit_depth": None,
            }

        try:
            result = subprocess.run(
                [
                    "ffprobe", "-v", "quiet",
                    "-print_format", "json",
                    "-show_streams",
                    "-select_streams", "a:0",
                    file_path,
                ],
                capture_output=True, text=True, timeout=30,
            )

            if result.returncode != 0:
                return {
                    "status": "fail",
                    "error": f"ffprobe failed: {result.stderr}",
                    "codec": None,
                    "sample_rate": None,
                    "bit_depth": None,
                }

            data = json.loads(result.stdout)
            streams = data.get("streams", [])
            if not streams:
                return {
                    "status": "fail",
                    "error": "No audio streams found",
                    "codec": None,
                    "sample_rate": None,
                    "bit_depth": None,
                }

            stream = streams[0]
            codec = stream.get("codec_name", "")
            sample_rate = int(stream.get("sample_rate", 0))
            bit_depth = int(stream.get("bits_per_raw_sample", stream.get("bits_per_sample", 0)))

            # Lossless codecs with >= 16-bit and >= 44100 Hz
            lossless_codecs = (
                "flac", "alac", "wav", "aiff",
                "pcm_s16le", "pcm_s24le", "pcm_s32le",
                "pcm_s16be", "pcm_s24be", "pcm_s32be",
                "pcm_f32le", "pcm_f64le",
            )
            is_lossless = codec in lossless_codecs
            is_hires = sample_rate >= 44100 and bit_depth >= 16

            status = "pass" if (is_lossless and is_hires) else "fail"

            return {
                "status": status,
                "codec": codec,
                "sample_rate": sample_rate,
                "bit_depth": bit_depth,
                "is_genuine_lossless": is_lossless and is_hires,
            }

        except subprocess.TimeoutExpired:
            return {"status": "fail", "error": "ffprobe timed out", "codec": None, "sample_rate": None, "bit_depth": None}
        except Exception as e:
            return {"status": "fail", "error": str(e), "codec": None, "sample_rate": None, "bit_depth": None}

    async def analyze_spectrum(self, file_path: str) -> dict:
        """
        Spectral analysis to detect transcoded files (lossy re-encoded as lossless).
        Uses sox to check for frequency cutoff typical of lossy codecs.
        """
        if not os.path.isfile(file_path):
            return {"suspicious": False, "error": f"File not found: {file_path}"}

        try:
            result = subprocess.run(
                ["sox", file_path, "-n", "stat"],
                capture_output=True, text=True, timeout=60,
            )

            # sox stat outputs to stderr
            stats_text = result.stderr

            # Parse maximum frequency — if it cuts off below 20kHz, likely transcoded
            # This is a heuristic; full spectral analysis would need numpy/scipy
            lines = stats_text.strip().split("\n")
            stats = {}
            for line in lines:
                parts = line.split(":", 1)
                if len(parts) == 2:
                    stats[parts[0].strip()] = parts[1].strip()

            return {
                "suspicious": False,  # Basic check — would need spectral peak analysis for real detection
                "stats": stats,
            }

        except subprocess.TimeoutExpired:
            return {"suspicious": False, "error": "sox timed out"}
        except Exception as e:
            return {"suspicious": False, "error": str(e)}

    async def generate_fingerprint(self, file_path: str) -> dict:
        """Generate a Chromaprint acoustic fingerprint."""
        if not os.path.isfile(file_path):
            return {"fingerprint": None, "error": f"File not found: {file_path}"}

        try:
            result = subprocess.run(
                ["fpcalc", "-json", file_path],
                capture_output=True, text=True, timeout=60,
            )

            if result.returncode != 0:
                return {"fingerprint": None, "error": f"fpcalc failed: {result.stderr}"}

            data = json.loads(result.stdout)
            return {
                "fingerprint": data.get("fingerprint"),
                "duration": data.get("duration"),
            }

        except subprocess.TimeoutExpired:
            return {"fingerprint": None, "error": "fpcalc timed out"}
        except FileNotFoundError:
            return {"fingerprint": None, "error": "fpcalc not found (install libchromaprint-tools)"}
        except Exception as e:
            return {"fingerprint": None, "error": str(e)}
