
import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CITIES = {
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Marrakech": (31.6295, -7.9811),
    "Tanger": (35.7595, -5.8340)
}

CATEGORIES = [
    "pizza", "tacos", "sushi",
    "marocain", "burger", "poulet"
]


def generate_data(n):

    restaurants = []

    # Generer 200 restaurants
    for i in range(1, 201):

        city = list(CITIES.keys())[(i - 1) % 4]
        lat, lon = CITIES[city]

        restaurant = {
            "restaurant_id": f"R-{i:03d}",
            "name": f"Restaurant {i:03d}",
            "city": city,
            "category": random.choice(CATEGORIES),
            "latitude": round(lat + random.uniform(-0.03, 0.03), 6),
            "longitude": round(lon + random.uniform(-0.03, 0.03), 6)
        }

        restaurants.append(restaurant)

    # Enregistrer les restaurants
    with open(DATA_DIR / "restaurants.json", "w", encoding="utf-8") as f:
        for restaurant in restaurants:
            f.write(json.dumps(restaurant, ensure_ascii=False) + "\n")

    events = []

    start_time = datetime(
        2026, 11, 15, 12, 0, tzinfo=timezone.utc
    )

    # Generer les evenements de commandes
    for i in range(n):

        # Environ 2% de doublons
        if events and random.random() < 0.02:
            events.append(random.choice(events).copy())
            continue

        restaurant = random.choice(restaurants)
        city = restaurant["city"]

        status = random.choices(
            ["CREATED", "PICKED_UP", "DELIVERED", "CANCELLED"],
            weights=[20, 15, 58, 7]
        )[0]

        # Environ 5% d'evenements retardes
        delay = (
            random.uniform(10, 15)
            if random.random() < 0.05
            else random.uniform(0, 2)
        )

        event_time = (
            start_time
            + timedelta(seconds=i)
            - timedelta(minutes=delay)
        )

        event = {
            "order_id": f"ORD-{i+1:07d}",
            "restaurant_id": restaurant["restaurant_id"],
            "customer_id": f"C-{random.randint(1, 10000):05d}",
            "city": city,
            "status": status,
            "amount_mad": round(random.uniform(40, 300), 2),
            "items_count": random.randint(1, 6),
            "distance_km": round(random.uniform(0.3, 12), 2),
            "courier_id": f"L-{city[:3].upper()}-{random.randint(1, 40):02d}",
            "event_time": event_time.isoformat().replace("+00:00", "Z")
        }

        events.append(event)

    # Enregistrer les commandes
    with open(
        DATA_DIR / "orders_history.jsonl",
        "w",
        encoding="utf-8"
    ) as f:

        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    print("Generation terminee !")
    print("Restaurants :", len(restaurants))
    print("Evenements :", len(events))
    print("Dossier :", DATA_DIR)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        choices=["file"],
        required=True
    )

    parser.add_argument(
        "--n",
        type=int,
        default=50000
    )

    args = parser.parse_args()

    if args.n <= 0:
        parser.error("--n doit etre positif")

    generate_data(args.n)
