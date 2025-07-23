import json
import requests
import os
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from dateutil import parser
from requests.adapters import HTTPAdapter, Retry
from typing import Optional, List, Dict, Any


@dataclass
class DevXClient:
    base_url: str
    token: str
    timeout: int = 15

    def __post_init__(self):
        # Prepare session with retry logic
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get_orders(self, params: Dict[str, Any]) -> List[Dict]:
        url = f"{self.base_url}/orders"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        try:
            resp = self.session.get(url, headers=headers, params=params, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json().get("data", [])
        except requests.RequestException as e:
            print(f"❌ Request failed: {e}")
            return []

    def flatten_buyer_address(self, order: Dict, product: Dict) -> Dict:
        buyer_address = order.pop("address", {})
        for key, value in buyer_address.items():
            if key == "pin":
                product["pincode"] = value
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    product[f"{key}_{sub_key}"] = sub_value
        return product

    def flatten_product_attributes(self, product: Dict) -> Dict:
        attrs = product.pop("attributes", [])
        for attribute in attrs:
            for key, value in attribute.items():
                product[key] = value
        return product

    def flatten_product_misc(self, product: Dict) -> Dict:
        misc = product.pop("misc_data", {})
        for key, value in misc.items():
            product[key] = value
        return product

    def add_order_data(self, product: Dict, order: Dict) -> Dict:
        order_copy = order.copy()
        product = self.flatten_buyer_address(order_copy, product)
        keys = [
            "display_order_id", "created_at", "uuid", "modified_at",
            "coupon_discount", "delivery_cost", "store_lead_id", "is_new",
            "customer_credits_used", "customer_credits_earned", "credit_label",
            "is_first_order_for_customer", "coupon_code", "payment_mode",
        ]
        for key in keys:
            if key == "created_at":
                dt = parser.isoparse(order[key])
                product[f"order_{key}"] = dt.date().isoformat()
            else:
                product[f"order_{key}"] = order.get(key)
        return product

    def add_line_item_group(self, product: Dict) -> Dict:
        lig = product.pop("line_item_group", {}) or {}
        keys = [
            "total_product_bundle_selling_price",
            "total_product_bundle_original_price",
            "name", "base_qty", "product_bundle_type", "uuid"
        ]
        for key in keys:
            product[f"bundle_{key}"] = lig.get(key)
        return product

    def extract_products(self, orders: List[Dict]) -> pd.DataFrame:
        rows = []
        for order in orders:
            for prod in order.pop("products", []):
                p = prod.copy()
                p = self.flatten_product_attributes(p)
                p = self.flatten_product_misc(p)
                p = self.add_order_data(p, order)
                p = self.add_line_item_group(p)
                # drop unused fields
                for f in ["image","add_on_data","timer","tp_data","gift_wrap_message",
                          "default_staff_id","default_staff_name"]:
                    p.pop(f, None)
                rows.append(p)
        df = pd.DataFrame(rows)
        # cleanup
        if not df.empty:
            df["pincode"] = df["pincode"].astype(str)
            df.rename(columns={"order_payment_mode": "payment_method"}, inplace=True)
            df["payment_method"] = df["payment_method"].astype(str)
            df.drop_duplicates(subset=["line_item_uuid"], keep="first", inplace=True)
        return df


    def save_to_csv(self, df: pd.DataFrame, path: str = "./temp/flattened_orders.csv"):

        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            df.to_csv(path, index=False)
            print(f"✅ Saved flattened CSV to: {path}")
        except Exception as e:
            print(f"❌ Failed to save CSV: {e}")



if __name__ == "__main__":
    client = DevXClient(
        base_url="http://13.202.156.212:9000/gc",
        token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY3Rvcl9pZCI6InVzZXJfMDFLMEJUNkRCUjY2MFk0UTJGRTg3NFE5WU0iLCJhY3Rvcl90eXBlIjoidXNlciIsImF1dGhfaWRlbnRpdHlfaWQiOiJhdXRoaWRfMDFLMEJUNkRHUDNFQ0REUzg1N0hXOERDME4iLCJhcHBfbWV0YWRhdGEiOnsidXNlcl9pZCI6InVzZXJfMDFLMEJUNkRCUjY2MFk0UTJGRTg3NFE5WU0ifSwiaWF0IjoxNzUzMTgwNTgyLCJleHAiOjQ5MDg5NDA1ODJ9.En6Dx42QB4wF7RPvnNKMO4G7G3xxYZjV_S7Eerc0tu0"
    )
    params = {
        "created_at[$gte]": "2025-07-15T09:56:33.151Z",
        "created_at[$lte]": "2025-07-22T10:18:15.115Z"
    }
    orders = client.get_orders(params)
    print(f"Fetched {len(orders)} orders")
    df = client.extract_products(orders)
    client.save_to_csv(df)
    print(df.head())
