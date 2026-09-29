"""Synthetic storefront clickstream with realistic funnel ratios and user skew."""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from itertools import accumulate

CATEGORIES = ["electronics", "books", "fashion", "home", "beauty", "sports", "toys", "grocery"]
EVENT_WEIGHTS = {"view": 0.70, "add_to_cart": 0.20, "purchase": 0.08, "refund": 0.02}
DEVICES = ["web", "ios", "android"]


@dataclass(frozen=True)
class Product:
    product_id: str
    category: str
    price: float


class EventGenerator:
    """Generates events that match the latest `UserEvent` schema.

    A Zipf-like user distribution means a minority of users generate most
    traffic, which exercises hot keys in the online store the way real
    traffic does.
    """

    def __init__(self, n_users: int = 10_000, n_products: int = 2_000, seed: int | None = None):
        self.rng = random.Random(seed)
        self.users = [f"u{i:06d}" for i in range(n_users)]
        self._user_cum_weights = list(accumulate(1.0 / (i + 1) ** 0.8 for i in range(n_users)))
        self.products = [
            Product(
                product_id=f"p{i:05d}",
                category=self.rng.choice(CATEGORIES),
                price=round(min(self.rng.lognormvariate(3.5, 0.9), 5000.0), 2),
            )
            for i in range(n_products)
        ]
        self.sessions: dict[str, str] = {}
        self._types = list(EVENT_WEIGHTS)
        self._type_cum_weights = list(accumulate(EVENT_WEIGHTS.values()))

    def _session_for(self, user_id: str) -> str:
        # ~5% chance a user starts a new session on any event.
        if user_id not in self.sessions or self.rng.random() < 0.05:
            self.sessions[user_id] = uuid.UUID(int=self.rng.getrandbits(128)).hex[:16]
        return self.sessions[user_id]

    def event(self, now_ms: int | None = None) -> dict:
        user_id = self.rng.choices(self.users, cum_weights=self._user_cum_weights, k=1)[0]
        product = self.rng.choice(self.products)
        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "user_id": user_id,
            "event_type": self.rng.choices(self._types, cum_weights=self._type_cum_weights, k=1)[0],
            "product_id": product.product_id,
            "category": product.category,
            "amount": product.price,
            "currency": "USD",
            "event_ts": now_ms if now_ms is not None else int(time.time() * 1000),
            "session_id": self._session_for(user_id),
            "device": self.rng.choice(DEVICES),
        }

    def batch(self, n: int) -> list[dict]:
        now_ms = int(time.time() * 1000)
        return [self.event(now_ms) for _ in range(n)]

    def invalid_event(self) -> dict:
        """An event that violates the contract (used to exercise the DLQ path)."""
        event = self.event()
        breakage = self.rng.choice(["drop_user", "bad_amount", "bad_ts"])
        if breakage == "drop_user":
            event.pop("user_id")
        elif breakage == "bad_amount":
            event["amount"] = "12.99 USD"
        else:
            event["event_ts"] = "yesterday"
        return event
