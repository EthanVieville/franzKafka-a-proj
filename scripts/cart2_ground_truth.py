# Verite terrain independante pour CART-2 : calcule directement sur
# data/raw/*.jsonl (donc sans Kafka) le nombre de commandes qui DEVRAIENT
# ressortir sur <grp>.cartflow.orders.confirmed : reimplemente les regles
# CART-1 (Validators.java) puis la fenetre de jointure CART-2 (+/-10 min,
# JoinWindows.ofTimeDifferenceWithNoGrace).
#
# Usage : python scripts/cart2_ground_truth.py   (depuis la racine du repo)
#
# Sert de reference pour verifier qu'un rejeu (make load / make stream) sur
# le cluster local produit bien un total de commandes confirmees proche de
# cette cible - voir recap/02-cart2-commandes-confirmees.md section 4.

import json
from datetime import datetime

CATEGORIES = {"HIGH-TECH", "MAISON", "MODE", "SPORT", "BEAUTE", "JARDIN", "JOUETS", "EPICERIE"}
STATUSES = {"AUTHORIZED", "DECLINED"}
WINDOW_MS = 10 * 60 * 1000


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


def validate_payment(p):
    if not isinstance(p, dict):
        return False
    if is_blank(p.get("payment_id")) or is_blank(p.get("order_id")) or is_blank(p.get("card_id")):
        return False
    amount = p.get("amount")
    if amount is None or not isinstance(amount, (int, float)) or amount <= 0:
        return False
    if p.get("status") not in STATUSES:
        return False
    if not parsable_ts(p.get("timestamp")):
        return False
    return True


def load_lines(path):
    """Chaque ligne est {"key", "value": "<json encode en string>", "ts_ms"} (format DataLoader)."""
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
                yield None, envelope.get("ts_ms")
                continue
            yield payload, envelope.get("ts_ms")


def main():
    orders_by_id = {}
    total_orders = valid_orders = 0
    for payload, ts_ms in load_lines("data/raw/cartflow.orders.jsonl"):
        total_orders += 1
        if payload is not None and validate_order(payload):
            valid_orders += 1
            orders_by_id.setdefault(payload["order_id"], []).append(ts_ms)

    payments_by_order = {}
    total_payments = valid_payments = authorized_valid_payments = 0
    for payload, ts_ms in load_lines("data/raw/cartflow.payments.jsonl"):
        total_payments += 1
        if payload is not None and validate_payment(payload):
            valid_payments += 1
            if payload.get("status") == "AUTHORIZED":
                authorized_valid_payments += 1
                payments_by_order.setdefault(payload["order_id"], []).append(ts_ms)

    matched_order_ids = set()
    matched_occurrences = 0
    for order_id, order_ts_list in orders_by_id.items():
        pay_ts_list = payments_by_order.get(order_id)
        if not pay_ts_list:
            continue
        for order_ts in order_ts_list:
            if any(abs(pay_ts - order_ts) <= WINDOW_MS for pay_ts in pay_ts_list):
                matched_order_ids.add(order_id)
                matched_occurrences += 1

    print(f"Orders   : {total_orders} total, {valid_orders} valides (CART-1)")
    print(f"Payments : {total_payments} total, {valid_payments} valides (CART-1), "
          f"{authorized_valid_payments} valides+AUTHORIZED")
    print(f"order_id avec un paiement valide+AUTHORIZED (sans condition de fenetre) : {len(payments_by_order)}")
    print(f"order_id valides matches dans la fenetre +/-10min (CART-2)              : {len(matched_order_ids)}")
    print(f"Occurrences matchees dans la fenetre (si doublons dans le dataset)      : {matched_occurrences}")


if __name__ == "__main__":
    main()
