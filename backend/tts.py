"""Turns critical alert messages into spoken audio using Gemini's TTS
capability.

This is deliberately isolated in its own module and wrapped in try/except
at the call site (backend/alerts.py) so that if TTS fails for any reason
(quota, auth, model name drift) it degrades gracefully — the alert still
fires and displays, it just won't have a voice that one time.

NOTE ON MODEL NAME: Gemini's TTS-capable model names have shifted over
time (gemini-2.5-flash-preview-tts, gemini-2.5-pro-preview-tts, and newer
variants). Set GEMINI_TTS_MODEL in your .env to whatever the current
documentation specifies if the default here doesn't work — the
request/response shape (response_modalities=["AUDIO"] + inline PCM audio
data) should stay stable even if the model string moves.
"""
import base64
import io
import os
import wave

from google import genai
from google.genai import types

TTS_MODEL = os.environ.get("GEMINI_TTS_MODEL", "gemini-2.5-flash-preview-tts")
VOICE_NAME = os.environ.get("GEMINI_TTS_VOICE", "Kore")

# Gemini TTS returns raw PCM audio at this format; we wrap it in a WAV
# container so it plays directly in a browser <audio> element with no
# extra client-side decoding.
SAMPLE_RATE = 24000
SAMPLE_WIDTH = 2  # 16-bit
CHANNELS = 1

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = genai.Client()
    return _client


def _pcm_to_wav_bytes(pcm_bytes: bytes) -> bytes:
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(SAMPLE_WIDTH)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm_bytes)
    return buffer.getvalue()


def synthesize_alert_audio(text: str) -> str | None:
    """Generates a short spoken announcement for an alert message.

    Returns a base64-encoded WAV string ready to embed as a data URI on the
    frontend (`data:audio/wav;base64,<this>`), or None if generation fails
    for any reason — callers should treat this as best-effort.
    """
    try:
        client = _get_client()
        announcement = f"Alert. {text}"

        response = client.models.generate_content(
            model=TTS_MODEL,
            contents=announcement,
            config=types.GenerateContentConfig(
                response_modalities=["AUDIO"],
                speech_config=types.SpeechConfig(
                    voice_config=types.VoiceConfig(
                        prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=VOICE_NAME)
                    )
                ),
            ),
        )

        pcm_data = response.candidates[0].content.parts[0].inline_data.data
        wav_bytes = _pcm_to_wav_bytes(pcm_data)
        return base64.b64encode(wav_bytes).decode("ascii")

    except Exception as e:  # noqa: BLE001 — deliberately broad, TTS is best-effort
        print(f"[tts] audio generation failed, continuing without voice: {e}")
        return None
