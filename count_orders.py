import json

with open("response.json") as f:
    data = json.load(f)

orders = data["data"]
total = sum(len(order.get("products")) for order in orders)
print(f"Total product instances: {total}")
