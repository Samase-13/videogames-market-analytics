"""
03_productor_kafka.py
Simulador de eventos de CCU (Concurrent Users) en tiempo real via Kafka.
Genera eventos JSON cada 0.5-2 segundos al tópico 'eventos_juego'.
"""

import json
import random
import time
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, NoBrokersAvailable
except ImportError:
    raise ImportError("Instala kafka-python: pip install kafka-python")

KAFKA_SERVERS = ["10.242.175.212:9092"]
TOPIC        = "eventos_juego"

PLATFORMS = ["PC", "PlayStation 5", "Xbox Series X", "Nintendo Switch", "PlayStation 4", "Mobile"]
GENRES    = ["Action", "RPG", "Strategy", "Sports", "Shooter", "Adventure", "Simulation", "Horror"]

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=10000,
        )
        print(f"Productor Kafka conectado a {KAFKA_SERVERS}")
        return producer
    except NoBrokersAvailable as e:
        raise ConnectionError(f"No se puede conectar a Kafka en {KAFKA_SERVERS}: {e}")

def generate_event():
    concurrent_players = random.randint(100, 500_000)
    hype_spike = 1 if concurrent_players > 300_000 or random.random() < 0.05 else 0
    return {
        "timestamp":          datetime.utcnow().isoformat(),
        "platform":           random.choice(PLATFORMS),
        "genre":              random.choice(GENRES),
        "concurrent_players": concurrent_players,
        "hype_score":         round(random.uniform(0.1, 10.0), 2),
        "current_price_usd":  round(random.uniform(0.99, 69.99), 2),
        "is_on_sale":         random.choice([0, 1]),
        "hype_spike":         hype_spike,
    }

def on_success(metadata):
    print(f"  [OK] Evento enviado -> partición {metadata.partition}, offset {metadata.offset}")

def on_error(exc):
    print(f"  [ERROR] Fallo al enviar: {exc}")

def main():
    producer = None
    try:
        producer = create_producer()
        print(f"Produciendo eventos en tópico '{TOPIC}'. Ctrl+C para detener.\n")
        eventos_enviados = 0

        while True:
            event = generate_event()
            future = producer.send(TOPIC, value=event)
            future.add_callback(on_success)
            future.add_errback(on_error)

            eventos_enviados += 1
            spike_flag = "⚡ HYPE SPIKE" if event["hype_spike"] else ""
            print(f"[{eventos_enviados:05d}] {event['timestamp']} | {event['platform']:20s} | "
                  f"CCU: {event['concurrent_players']:>7,} | {spike_flag}")

            delay = random.uniform(0.5, 2.0)
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\nProductor detenido. Eventos enviados: {eventos_enviados:,}")
    except ConnectionError as e:
        print(f"Error de conexión: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")
        raise
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("Productor Kafka cerrado.")

if __name__ == "__main__":
    main()
