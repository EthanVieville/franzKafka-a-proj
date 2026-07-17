# Verite terrain independante pour CART-3 : calcule directement sur
# data/raw/cartflow.orders.jsonl (donc sans Kafka) le CA total par
# categorie, toutes fenetres confondues. Reimplemente les regles CART-1
# (Validators.java) - source = commandes brutes valides, comme dans
# CartflowTopology (choix assume, voir le commentaire au-dessus du bloc
# CART-3).
#
# Les fenetres tumbling de 1 min ne se chevauchent jamais et ne laissent
# aucun trou : la somme de toutes les fenetres d'une categorie doit donc
# etre egale au total calcule ici, sans avoir a rejouer le decoupage en
# fenetres cote Python.
#
# Usage : python scripts/cart3_ground_truth.py   (depuis la racine du repo)

import json
from datetime import datetime
from collections import defaultdict

CATEGORIES = {"HIGH-TECH", "MAISON", "MODE", "SPORT", "BEAUTE", "JARDIN", "JOUETS", "EPICERIE"}


def parsable_ts(value):
    if not value:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def is_blank(v):
    return v is None or (isinstance(v, str) and v.strip() == "")


def validate_order(o):
    if not isinstance(o, dict):
        return False
    if is_blank(o.get("order_id")) or is_blank(o.get("user_id")) or is_blank(o.get("card_id")):
        return False
    if is_blank(o.get("currency")) or is_blank(o.get("shipping_city")):
        return False
    total = o.get("total")
    if total is None or not isinstance(total, (int, float)) or total <= 0:
        return False
    if not parsable_ts(o.get("timestamp")):
        return False
    items = o.get("items")
    if not items:
        return False
    for it in items:
        if is_blank(it.get("sku")) or is_blank(it.get("product")) or is_blank(it.get("category")):
            return False
        if it.get("category") not in CATEGORIES:
            return False
        qty = it.get("qty")
        if qty is None or not isinstance(qty, (int, float)) or qty <= 0:
            return False
        unit_price = it.get("unit_price")
        if unit_price is None or not isinstance(unit_price, (int, float)) or unit_price <= 0:
            return False
    return True


def load_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
            except Exception:
                continue
            try:
                payload = json.loads(envelope.get("value"))
            except Exception:
                yield None
                continue
            yield payload


def main():
    revenue_by_category = defaultdict(float)
    total_orders = valid_orders = 0

    for payload in load_lines("data/raw/cartflow.orders.jsonl"):
        total_orders += 1
        if payload is None or not validate_order(payload):
            continue
        valid_orders += 1
        for item in payload["items"]:
            revenue_by_category[item["category"]] += item["qty"] * item["unit_price"]

    print(f"Orders : {total_orders} total, {valid_orders} valides (CART-1)")
    print()
    print("CA total par categorie (toutes fenetres confondues) :")
    for category in sorted(revenue_by_category):
        print(f"  {category:12s} {revenue_by_category[category]:14.2f}")
    print(f"  {'TOTAL':12s} {sum(revenue_by_category.values()):14.2f}")


if __name__ == "__main__":
    main()
