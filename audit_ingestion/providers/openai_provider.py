"""
audit_ingestion_v03/audit_ingestion/providers/openai_provider.py
"""
from __future__ import annotations
import json
import base64
import logging
import subprocess
import tempfile
import os
from typing import Any, Optional
from .base import AIProvider

logger = logging.getLogger(__name__)

try:
    import openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False


class OpenAIProvider(AIProvider):
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4o-mini"):
        if not HAS_OPENAI:
            raise ImportError("openai not installed")
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model

    def _call(self, system: str, user: str, max_tokens: int = 3000) -> str:
        resp = self.client.chat.completions.create(
            model=self.model,
            max_tokens=max_tokens,
            temperature=0.0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
        )
        return resp.choices[0].message.content or ""

    def extract_text_from_pdf_vision(self, pdf_bytes: bytes, max_pages: int = 6) -> str:
        """Convert PDF pages to images and extract text via GPT-4o vision."""
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        img_dir = tempfile.mkdtemp()
        image_contents = []

        try:
            subprocess.run(
                ["pdftoppm", "-jpeg", "-r", "150", "-l", str(max_pages),
                 tmp_path, f"{img_dir}/page"],
                capture_output=True, timeout=60
            )

            for img_file in sorted(os.listdir(img_dir)):
                if img_file.endswith(".jpg"):
                    img_path = os.path.join(img_dir, img_file)
                    with open(img_path, "rb") as f:
                        img_b64 = base64.b64encode(f.read()).decode()
                    image_contents.append({
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{img_b64}", "detail": "high"}
                    })
                    os.unlink(img_path)
                    if len(image_contents) >= max_pages:
                        break
        except Exception as e:
            logger.warning(f"pdftoppm failed: {e}")
        finally:
            try:
                os.unlink(tmp_path)
                os.rmdir(img_dir)
            except Exception:
                pass

        if not image_contents:
            return ""

        image_contents.append({
            "type": "text",
            "text": "Extract ALL text from these document pages. Return complete text preserving all numbers, dates, names, amounts, and terms exactly as they appear."
        })

        try:
            resp = self.client.chat.completions.create(
                model="gpt-4o",
                max_tokens=4000,
                messages=[{"role": "user", "content": image_contents}],
            )
            return resp.choices[0].message.content or ""
        except Exception as e:
            logger.error(f"Vision extraction failed: {e}")
            return ""
