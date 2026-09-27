#!/usr/bin/env python3
"""
Synthetic e-commerce clickstream event generator.

Produces realistic, correlated user sessions (page_view -> add_to_cart ->
purchase funnels) to a Kafka-compatible topic (Redpanda). Event times carry
a small random skew plus occasional late arrivals (~3% arrive 6-10 minutes
late) so the streaming job's watermarking has something real to chew on.

Configuration via environment:
    KAFKA_BOOTSTRAP  Kafka bootstrap servers (default localhost:9092)
    TOPIC            Topic to produce to (default clickstream-events)
    EVENT_RATE       Target events per second (default 40)
    TOPIC_PARTITIONS Partitions for auto-created topic (default 6)
"""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("generator")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "clickstream-events")
EVENT_RATE = float(os.environ.get("EVENT_RATE", "40"))  # events per second
PARTITIONS = int(os.environ.get("TOPIC_PARTITIONS", "6"))

PRODUCTS = [
    # (product_id, name, category, price)
    ("p001", "Aurora Wireless Headphones", "Electronics", 129.99),
    ("p002", "Volt 65W GaN Charger", "Electronics", 49.99),
    ("p003", "Nimbus Mechanical Keyboard", "Electronics", 89.99),
    ("p004", "Orbit 4K Webcam", "Electronics", 79.99),
    ("p005", "Pulse Fitness Tracker", "Electronics", 59.99),
    ("p006", "EchoBuds Pro", "Electronics", 99.99),
    ("p007", "Terra Hiking Backpack 40L", "Sports", 119.99),
    ("p008", "Apex Trail Running Shoes", "Sports", 139.99),
    ("p009", "Core Yoga Mat Pro", "Sports", 39.99),
    ("p010", "HydroSteel Bottle 1L", "Sports", 29.99),
    ("p011", "Summit Insulated Jacket", "Apparel", 159.99),
    ("p012", "Drift Denim Jeans", "Apparel", 69.99),
    ("p013", "Linen Oxford Shirt", "Apparel", 54.99),
    ("p014", "Cloudstep Sneakers", "Apparel", 89.99),
    ("p015", "Merino Crew Socks 3-Pack", "Apparel", 24.99),
    ("p016", "Atlas Wool Coat", "Apparel", 199.99),
    ("p017", "Ember Ceramic Pour-Over Set", "Home", 64.99),
    ("p018", "Lumen Desk Lamp", "Home", 44.99),
    ("p019", "Haven Memory Foam Pillow", "Home", 34.99),
    ("p020", "Kiln Stoneware Dinner Set", "Home", 129.99),
    ("p021", "Fern Glass Terrarium", "Home", 27.99),
    ("p022", "Oakfield Throw Blanket", "Home", 49.99),
    ("p023", "Glow Vitamin C Serum", "Beauty", 32.99),
    ("p024", "Silk Repair Hair Mask", "Beauty", 28.99),
    ("p025", "Mineral SPF 50 Sunscreen", "Beauty", 24.99),
    ("p026", "Botanic Clay Cleanser", "Beauty", 22.99),
    ("p027", "Velvet Matte Lipstick Set", "Beauty", 36.99),
    ("p028", "Dewdrop Hyaluronic Toner", "Beauty", 26.99),
]

PRODUCT_PAGES = [f"/product/{pid}" for pid, _, _, _ in PRODUCTS]
BROWSE_PAGES = [
    "/", "/search", "/deals", "/new-arrivals",
    "/category/electronics", "/category/apparel", "/category/home",
    "/category/sports", "/category/beauty",
]

DEVICES = ["desktop", "mobile", "tablet"]
DEVICE_WEIGHTS = [0.45, 0.45, 0.10]

# Funnel transition probabilities: current stage -> [(next stage, probability)]
FUNNEL = {
    "page_view": [("page_view", 0.55), ("add_to_cart", 0.28), ("end", 0.17)],
    "add_to_cart": [("page_view", 0.35), ("add_to_cart", 0.22), ("purchase", 0.26), ("end", 0.17)],
    "purchase": [("page_view", 0.60), ("end", 0.40)],
}


def _next_stage(stage: str) -> str:
    r = random.random()
    cumulative = 0.0
    for nxt, p in FUNNEL[stage]:
        cumulative += p
        if r < cumulative:
            return nxt
    return "end"


def ensure_topic() -> None:
    """Create the topic if it does not exist (idempotent)."""
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    topic = NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)
    futures = admin.create_topics([topic])
    for name, fut in futures.items():
        try:
            fut.result(timeout=15)
            log.info("Created topic %s (%d partitions)", name, PARTITIONS)
        except Exception as e:  # TopicExistsError etc. -> already usable
            log.info("Topic %s ready (%s)", name, e)


def make_session_events() -> list:
    """Generate one correlated user session as a list of event dicts."""
    user_id = f"u{random.randint(1, 20000):05d}"
    session_id = uuid.uuid4().hex[:12]
    device = random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0]
    session_products = random.sample(PRODUCTS, k=random.randint(1, 3))

    events = []
    now = datetime.now(timezone.utc)
    # Session started a little while ago; walk forward in time.
    t = now - timedelta(seconds=random.uniform(5, 240))
    stage = "page_view"
    while True:
        product = random.choice(session_products)
        if stage == "page_view":
            page = random.choice(PRODUCT_PAGES + BROWSE_PAGES)
            ev_product = product if page.startswith("/product/") else None
        elif stage == "add_to_cart":
            page, ev_product = "/cart", product
        else:  # purchase
            page, ev_product = "/checkout/success", product

        # Event-time skew: mostly near-on-time, occasionally very late.
        skew = random.expovariate(1 / 15.0)
        if random.random() < 0.03:
            skew += random.uniform(360, 600)  # 6-10 min late -> dropped by watermark
        event_time = t - timedelta(seconds=skew)

        events.append({
            "event_id": uuid.uuid4().hex,
            "user_id": user_id,
            "session_id": session_id,
            "event_type": stage,
            "product_id": ev_product[0] if ev_product else None,
            "product_name": ev_product[1] if ev_product else None,
            "category": ev_product[2] if ev_product else None,
            "price": ev_product[3] if ev_product and stage in ("add_to_cart", "purchase") else None,
            "page_url": page,
            "device": device,
            "event_time": event_time.isoformat(),
        })
        t += timedelta(seconds=random.uniform(2, 25))
        if t > now:
            break
        stage = _next_stage(stage)
        if stage == "end" or len(events) >= 12:
            break
    return events


def main() -> None:
    ensure_topic()
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 50,
        "compression.type": "snappy",
        "acks": "all",
    })
    delivered = 0

    def _acked(err, _msg):
        nonlocal delivered
        if err:
            log.warning("Delivery failed: %s", err)
        else:
            delivered += 1

    log.info("Producing ~%.0f events/sec to topic '%s' (%s)", EVENT_RATE, TOPIC, BOOTSTRAP)
    interval = 1.0 / EVENT_RATE
    try:
        while True:
            events = make_session_events()
            batch_start = time.monotonic()
            for event in events:
                producer.produce(
                    TOPIC,
                    json.dumps(event).encode("utf-8"),
                    key=event["session_id"],  # session affinity -> per-session ordering
                    callback=_acked,
                )
                producer.poll(0)
            producer.flush(10)
            # Pace the loop to hold the target rate.
            sleep_for = len(events) * interval - (time.monotonic() - batch_start)
            if sleep_for > 0:
                time.sleep(sleep_for)
            if delivered >= 5000 and delivered % 5000 < len(events):
                log.info("delivered=%d events", delivered)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(15)
        log.info("Shutdown. Total delivered: %d", delivered)


if __name__ == "__main__":
    main()
