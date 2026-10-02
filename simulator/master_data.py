"""
master_data.py — Seed data for restaurants, riders, customers, cities.

Generates realistic but fictional Indian food-delivery master data.
Used by the simulator to create consistent foreign-key relationships.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import ClassVar

from faker import Faker

fake = Faker("en_IN")
random.seed(42)

# ── City profiles (affects load weighting) ───────────────────────────────────

CITY_WEIGHTS: dict[str, float] = {
    "Bengaluru": 0.25,
    "Mumbai": 0.22,
    "Delhi": 0.20,
    "Hyderabad": 0.12,
    "Chennai": 0.10,
    "Pune": 0.06,
    "Kolkata": 0.03,
    "Ahmedabad": 0.02,
}

CITIES = list(CITY_WEIGHTS.keys())
CITY_POPULATION = list(CITY_WEIGHTS.values())

CUISINES = [
    "North Indian", "South Indian", "Chinese", "Pizza", "Burgers",
    "Biryani", "Rolls & Wraps", "Desserts", "Healthy", "Continental",
    "Thai", "Italian", "Mexican", "Sushi", "Sandwiches",
]

COMMISSION_TIERS = ["standard", "premium", "enterprise"]
COMMISSION_WEIGHTS = [0.65, 0.25, 0.10]

PAYMENT_METHODS = ["upi", "card", "wallet", "cod", "netbanking"]
PAYMENT_WEIGHTS = [0.45, 0.25, 0.15, 0.10, 0.05]


# ── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class Restaurant:
    restaurant_id: str
    name: str
    city: str
    cuisine: str
    commission_tier: str
    is_active: bool = True
    avg_prep_minutes: int = 20
    rejection_rate: float = 0.05  # probability of rejecting an order
    cancellation_rate: float = 0.03


@dataclass
class Rider:
    rider_id: str
    name: str
    city: str
    avg_delivery_minutes: int = 25
    idle_probability: float = 0.15  # probability they're busy / unavailable


@dataclass
class Customer:
    customer_id: str
    name: str
    city: str
    is_repeat: bool = False


# ── Generator functions ───────────────────────────────────────────────────────

def _make_id(prefix: str, n: int) -> str:
    return f"{prefix}-{n:06d}"


def generate_restaurants(count: int = 500) -> list[Restaurant]:
    restaurants = []
    for i in range(1, count + 1):
        city = random.choices(CITIES, weights=CITY_POPULATION)[0]
        cuisine = random.choice(CUISINES)
        tier = random.choices(COMMISSION_TIERS, weights=COMMISSION_WEIGHTS)[0]
        prep = random.randint(10, 45)
        name_adj = random.choice(["Royal", "Golden", "Spicy", "Fresh", "Quick", "Classic", "Urban"])
        name_noun = random.choice(["Kitchen", "Bites", "Eats", "Bowl", "Grill", "House", "Corner"])
        restaurants.append(Restaurant(
            restaurant_id=_make_id("RST", i),
            name=f"{name_adj} {cuisine} {name_noun}",
            city=city,
            cuisine=cuisine,
            commission_tier=tier,
            avg_prep_minutes=prep,
            rejection_rate=round(random.uniform(0.01, 0.12), 3),
            cancellation_rate=round(random.uniform(0.01, 0.08), 3),
        ))
    return restaurants


def generate_riders(count: int = 300) -> list[Rider]:
    riders = []
    for i in range(1, count + 1):
        city = random.choices(CITIES, weights=CITY_POPULATION)[0]
        riders.append(Rider(
            rider_id=_make_id("RID", i),
            name=fake.name(),
            city=city,
            avg_delivery_minutes=random.randint(15, 45),
            idle_probability=round(random.uniform(0.05, 0.30), 2),
        ))
    return riders


def generate_customers(count: int = 5000) -> list[Customer]:
    customers = []
    for i in range(1, count + 1):
        city = random.choices(CITIES, weights=CITY_POPULATION)[0]
        customers.append(Customer(
            customer_id=_make_id("CUS", i),
            name=fake.name(),
            city=city,
            is_repeat=random.random() < 0.35,
        ))
    return customers


# ── Singleton master data ─────────────────────────────────────────────────────

class MasterData:
    """
    Singleton holder for all master entities.
    Indexed by city for fast lookup during event generation.
    """
    _instance: ClassVar[MasterData | None] = None

    def __init__(self) -> None:
        self.restaurants = generate_restaurants(500)
        self.riders = generate_riders(300)
        self.customers = generate_customers(5000)

        # Index by city
        self.restaurants_by_city: dict[str, list[Restaurant]] = {}
        self.riders_by_city: dict[str, list[Rider]] = {}
        self.customers_by_city: dict[str, list[Customer]] = {}

        for r in self.restaurants:
            self.restaurants_by_city.setdefault(r.city, []).append(r)
        for r in self.riders:
            self.riders_by_city.setdefault(r.city, []).append(r)
        for c in self.customers:
            self.customers_by_city.setdefault(c.city, []).append(c)

    @classmethod
    def get(cls) -> MasterData:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def random_restaurant(self, city: str) -> Restaurant:
        return random.choice(self.restaurants_by_city.get(city, self.restaurants))

    def random_rider(self, city: str) -> Rider:
        available = [
            r for r in self.riders_by_city.get(city, self.riders)
            if random.random() > r.idle_probability
        ]
        return random.choice(available) if available else random.choice(self.riders)

    def random_customer(self, city: str) -> Customer:
        return random.choice(self.customers_by_city.get(city, self.customers))

    def random_city(self) -> str:
        return random.choices(CITIES, weights=CITY_POPULATION)[0]

    def random_payment_method(self) -> str:
        return random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
