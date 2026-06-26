"""
01_generate_dataset.py
Generador sintético de 100,000 registros para el dataset de videojuegos.
Produce un CSV compatible con el esquema de vg_market_analytics.csv.
"""

import random
import csv
import os
from datetime import datetime, timedelta

PLATFORMS = ["PC", "PlayStation 5", "Xbox Series X", "Nintendo Switch", "PlayStation 4", "Xbox One", "Mobile"]
GENRES = ["Action", "RPG", "Strategy", "Sports", "Shooter", "Adventure", "Simulation", "Horror", "Puzzle", "Racing"]

OUTPUT_PATH = "/home/vboxuser/videogames-market-analytics/data/videogames_synthetic.csv"
NUM_RECORDS = 100_000

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

def random_date(start_year=2022, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def generate_record():
    is_on_sale = random.choice([0, 1])
    discount = round(random.uniform(10, 75), 2) if is_on_sale else 0.0
    base_price = round(random.uniform(4.99, 69.99), 2)
    current_price = round(base_price * (1 - discount / 100), 2) if is_on_sale else base_price
    concurrent_players = random.randint(50, 250_000)
    hype_score = round(random.uniform(0.1, 10.0), 2)
    is_early_access = random.choice([0, 1])
    holiday_sale = random.choice([0, 1])
    estimated_revenue = round(concurrent_players * current_price * random.uniform(0.05, 0.3), 2)
    if estimated_revenue <= 0:
        estimated_revenue = round(random.uniform(100, 5000), 2)

    return {
        "obs_date": random_date(),
        "platform": random.choice(PLATFORMS),
        "genre": random.choice(GENRES),
        "current_price_usd": current_price,
        "discount_pc": discount,
        "is_on_sale": is_on_sale,
        "concurrent_players": concurrent_players,
        "hype_score": hype_score,
        "estimated_revenue_usd": estimated_revenue,
        "is_early_access": is_early_access,
        "holiday_sale": holiday_sale,
    }

def main():
    print(f"Generando {NUM_RECORDS:,} registros en {OUTPUT_PATH} ...")
    fieldnames = [
        "obs_date", "platform", "genre", "current_price_usd", "discount_pc",
        "is_on_sale", "concurrent_players", "hype_score", "estimated_revenue_usd",
        "is_early_access", "holiday_sale"
    ]
    try:
        with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i in range(NUM_RECORDS):
                writer.writerow(generate_record())
                if (i + 1) % 10_000 == 0:
                    print(f"  {i+1:,} registros escritos...")
        print(f"Dataset generado exitosamente: {OUTPUT_PATH}")
    except Exception as e:
        print(f"Error generando dataset: {e}")
        raise

if __name__ == "__main__":
    main()
