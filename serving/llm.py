"""Thin wrapper over Gemma. Local via Ollama (default) or hosted via Google AI Studio."""
import subprocess
from collections.abc import Iterator

import config


def _nvidia_stats() -> dict:
    """First NVIDIA GPU's name and memory use, via nvidia-smi (absent on non-NVIDIA machines)."""
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.used,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3, check=True).stdout.splitlines()[0]
        name, used, total = (v.strip() for v in out.split(","))
        return {"nvidia_name": name, "nvidia_used_gb": float(used) / 1024, "nvidia_total_gb": float(total) / 1024}
    except (OSError, subprocess.SubprocessError, ValueError, IndexError):
        return {}


class GemmaClient:
    def __init__(self, backend: str = config.LLM_BACKEND):
        self.backend = backend
        if backend == "ollama":
            import ollama
            self._client = ollama.Client(host=config.OLLAMA_HOST)
            self.model = config.OLLAMA_MODEL
        elif backend == "google":
            from google import genai  # pip install google-genai
            self._client = genai.Client(api_key=config.GOOGLE_API_KEY)
            self.model = config.GOOGLE_MODEL
        else:
            raise ValueError(f"Unknown LLM_BACKEND: {backend}")

    def chat(self, messages: list[dict], temperature: float = 0.0) -> str:
        return "".join(self.stream(messages, temperature))

    def stream(self, messages: list[dict], temperature: float = 0.0) -> Iterator[str]:
        if self.backend == "ollama":
            for chunk in self._client.chat(model=self.model, messages=messages, stream=True,
                                           options={"temperature": temperature, "num_ctx": 8192}):
                yield chunk["message"]["content"]
        else:
            # Gemma on the Gemini API has no system role: fold it into the first user turn.
            system = "\n\n".join(m["content"] for m in messages if m["role"] == "system")
            contents, first_user = [], True
            for m in messages:
                if m["role"] == "system":
                    continue
                text = m["content"]
                if m["role"] == "user" and first_user and system:
                    text, first_user = f"{system}\n\n{text}", False
                contents.append({"role": "model" if m["role"] == "assistant" else "user",
                                 "parts": [{"text": text}]})
            for chunk in self._client.models.generate_content_stream(
                    model=self.model, contents=contents, config={"temperature": temperature}):
                if chunk.text:
                    yield chunk.text

    def model_info(self) -> dict:
        """Model and hardware facts for the UI. Every field may be None if unavailable."""
        info = {"backend": self.backend, "model": self.model, "family": None, "parameters": None,
                "quantization": None, "disk_gb": None, "loaded_gb": None, "gpu_share": None,
                "context": None, "nvidia_name": None, "nvidia_used_gb": None, "nvidia_total_gb": None}
        if self.backend == "ollama":
            try:
                d = self._client.show(self.model).details
                info.update(family=d.family, parameters=d.parameter_size, quantization=d.quantization_level)
                for m in self._client.list().models:
                    if m.model == self.model:
                        info["disk_gb"] = m.size / 1e9
                for m in self._client.ps().models:
                    if m.model == self.model and m.size:
                        info.update(loaded_gb=m.size / 1e9, gpu_share=m.size_vram / m.size,
                                    context=getattr(m, "context_length", None))
            except Exception:  # noqa: BLE001 - a missing panel field must never break the chat
                pass
        info.update(_nvidia_stats())
        return info

    def health(self) -> tuple[bool, str]:
        try:
            if self.backend == "ollama":
                names = [m.model for m in self._client.list().models]
                if not any(n == self.model or n.split(":")[0] == self.model for n in names):
                    return False, f"Model {self.model} not pulled. Run: ollama pull {self.model}"
            return True, f"{self.backend}: {self.model}"
        except Exception as e:  # noqa: BLE001 - surface any connection problem to the UI
            return False, f"{self.backend} unreachable: {e}"
