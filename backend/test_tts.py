"""Standalone test for the Gemini TTS integration, isolated from the rest of
the alert pipeline so you can find out in seconds whether TTS itself works,
rather than debugging it through a live alert.

Usage:
    python -m backend.test_tts
    python -m backend.test_tts --play    # also attempts to play the audio locally (macOS/Linux)
"""
import argparse
import base64
import sys

from backend.tts import synthesize_alert_audio


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--play", action="store_true", help="attempt to play the generated audio locally")
    args = parser.parse_args()

    print("→ Requesting TTS audio for a test alert message...")
    test_message = "West region ticket sales are thirty percent below the cross-region average."
    audio_b64 = synthesize_alert_audio(test_message)

    if audio_b64 is None:
        print("✗ TTS generation failed — check GOOGLE_API_KEY and GEMINI_TTS_MODEL in .env")
        print("  (this is expected to fail gracefully in the app; alerts still work without audio)")
        sys.exit(1)

    wav_bytes = base64.b64decode(audio_b64)
    out_path = "test_alert_audio.wav"
    with open(out_path, "wb") as f:
        f.write(wav_bytes)

    print(f"✓ Generated {len(wav_bytes)} bytes of audio, saved to {out_path}")

    if args.play:
        import subprocess
        import platform
        try:
            if platform.system() == "Darwin":
                subprocess.run(["afplay", out_path], check=True)
            else:
                subprocess.run(["aplay", out_path], check=True)
            print("✓ Played audio locally")
        except Exception as e:
            print(f"  (couldn't auto-play: {e} — open {out_path} manually to listen)")


if __name__ == "__main__":
    main()
