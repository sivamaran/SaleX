from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger


@dataclass
class CacheConfig:
	cache_filename: str = "classification_cache.json"
	cache_dirname: str = ".cache"
	ttl_seconds: int = 7 * 24 * 60 * 60
	confidence_decay_per_day: float = 0.02  # 2% per day


class ClassificationCache:
	def __init__(self, base_dir: Optional[Path] = None, config: Optional[CacheConfig] = None):
		self.config = config or CacheConfig()
		if base_dir is None:
			base_dir = Path(__file__).resolve().parent
		self.cache_dir = base_dir / self.config.cache_dirname
		self.cache_dir.mkdir(parents=True, exist_ok=True)
		self.cache_path = self.cache_dir / self.config.cache_filename
		self._data: Dict[str, Any] = {}
		self._load()

	def _load(self) -> None:
		if self.cache_path.exists():
			try:
				self._data = json.loads(self.cache_path.read_text(encoding="utf-8"))
			except Exception as e:
				logger.warning(f"Failed to read cache; starting fresh: {e}")
				self._data = {}
		else:
			self._data = {}

	def _save(self) -> None:
		try:
			# Ensure everything is JSON-serializable (e.g., Pydantic HttpUrl already cast by callers)
			self.cache_path.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
		except Exception as e:
			logger.warning(f"Failed to persist cache: {e}")

	def get(self, url: str) -> Optional[Dict[str, Any]]:
		rec = self._data.get(url)
		if not rec:
			return None
		now = time.time()
		age = now - rec.get("ts", 0)
		if age > self.config.ttl_seconds:
			return None
		# Apply confidence decay in whole days only to avoid tiny drifts immediately after writes
		seconds_per_day = 24 * 60 * 60
		whole_days = int(age // seconds_per_day)
		conf = float(rec.get("result", {}).get("confidence", 0.0))
		if whole_days > 0:
			decayed = max(0.0, conf * (1.0 - self.config.confidence_decay_per_day) ** whole_days)
			# Round for stable representation
			rec["result"]["confidence"] = round(decayed, 6)
		else:
			rec["result"]["confidence"] = conf
		return rec

	def set(self, url: str, result: Dict[str, Any]) -> None:
		self._data[url] = {"ts": time.time(), "result": result}
		self._save()

	def override(self, url: str, classification: str, confidence: float) -> None:
		rec = self._data.get(url) or {"ts": time.time(), "result": {}}
		rec["result"]["classification"] = classification
		rec["result"]["confidence"] = confidence
		# Make it explicit in reasons to avoid confusion with stale analysis
		rec_result = rec.setdefault("result", {})
		rec_result["reasons"] = [f"Manual override to {classification} ({confidence:.2f})"]
		rec["ts"] = time.time()
		self._data[url] = rec
		self._save()

	def get_similar(self, url: str) -> Optional[Dict[str, Any]]:
		"""Return a recent classification from the same domain with a matching path prefix,
		with slightly reduced confidence. This is a best-effort hint for similar URL structures."""
		try:
			from urllib.parse import urlparse
			parsed = urlparse(url)
			candidates = []
			for key, rec in self._data.items():
				if not isinstance(rec, dict) or "result" not in rec:
					continue
				kp = urlparse(key)
				if kp.scheme != parsed.scheme or kp.netloc != parsed.netloc:
					continue
				if not kp.path:
					continue
				if parsed.path.startswith(kp.path.rstrip("/")):
					candidates.append(rec)
			if candidates:
				best = sorted(candidates, key=lambda r: r.get("ts", 0), reverse=True)[0]
				best = json.loads(json.dumps(best))
				best["result"]["confidence"] = max(0.0, float(best["result"].get("confidence", 0.0)) * 0.9)
				return best
		except Exception:
			return None
		return None
