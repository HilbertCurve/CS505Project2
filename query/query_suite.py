"""Hard-coded benchmark queries for the assignment experiment matrix."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QuerySpec:
    """One benchmark query plus the structures worth comparing."""

    name: str
    select_columns: list[str]
    predicate: list[str]
    structures: list[tuple[str, str]]
    notes: str


@dataclass(frozen=True)
class DatasetSpec:
    """Dataset metadata used to build and benchmark a table."""

    name: str
    table_name: str
    path: Path
    schema: dict[str, tuple[str, int | None]]
    queries: list[QuerySpec]


def build_dataset_specs(root_dir: Path) -> list[DatasetSpec]:
    datasets_dir = root_dir / "test" / "datasets"

    return [
        DatasetSpec(
            name="subscription_accounts",
            table_name="subscription_accounts",
            path=datasets_dir / "subscription_accounts.csv",
            schema={
                "account_id": ("INTEGER", None),
                "is_active": ("INTEGER", None),
                "status_code": ("INTEGER", None),
                "region_code": ("INTEGER", None),
                "region_name": ("VARCHAR", 20),
                "plan_tier": ("INTEGER", None),
                "plan_name": ("VARCHAR", 20),
                "account_balance_cents": ("INTEGER", None),
            },
            queries=[
                QuerySpec("eq_status", ["account_id"], ["status_code", "=", "0"], [("bitmap", "status_code")], "equality"),
                QuerySpec("eq_plan", ["account_id"], ["plan_tier", "=", "3"], [("bitmap", "plan_tier")], "equality"),
                QuerySpec("range_balance_selective", ["account_id"], ["account_balance_cents", ">", "3000"], [], "selective range"),
                QuerySpec("range_balance_nonselective", ["account_id"], ["account_balance_cents", ">=", "100"], [], "non-selective range"),
                QuerySpec("and_status_balance", ["account_id"], ["status_code", "=", "2", "AND", "account_balance_cents", ">", "3000"], [("bitmap", "status_code")], "AND"),
                QuerySpec("agg_active_count", ["account_id"], ["is_active", "=", "1"], [("bitmap", "is_active")], "aggregation-friendly count"),
            ],
        ),
        DatasetSpec(
            name="warehouse_inventory",
            table_name="warehouse_inventory",
            path=datasets_dir / "warehouse_inventory.csv",
            schema={
                "inventory_id": ("INTEGER", None),
                "warehouse_id": ("INTEGER", None),
                "warehouse_zone": ("VARCHAR", 20),
                "aisle_id": ("INTEGER", None),
                "shelf_id": ("INTEGER", None),
                "product_id": ("INTEGER", None),
                "units_on_hand": ("INTEGER", None),
                "restock_ts": ("INTEGER", None),
            },
            queries=[
                QuerySpec("eq_warehouse", ["inventory_id"], ["warehouse_id", "=", "100"], [("rle", "warehouse_id")], "equality"),
                QuerySpec("eq_zone", ["inventory_id"], ["warehouse_zone", "=", "'bulk'"], [("bitmap", "warehouse_zone")], "equality"),
                QuerySpec("range_product_selective", ["inventory_id"], ["product_id", ">=", "1390200"], [("zone_map", "product_id")], "selective range"),
                QuerySpec("range_restock_nonselective", ["inventory_id"], ["restock_ts", ">=", "1704200000"], [("zone_map", "restock_ts")], "non-selective range"),
                QuerySpec("and_zone_warehouse", ["inventory_id"], ["warehouse_zone", "=", "'bulk'", "AND", "warehouse_id", "=", "100"], [("bitmap", "warehouse_zone"), ("rle", "warehouse_id")], "AND"),
                QuerySpec("agg_warehouse_count", ["inventory_id"], ["warehouse_id", "=", "120"], [("rle", "warehouse_id")], "aggregation-friendly count"),
            ],
        ),
        DatasetSpec(
            name="payment_authorizations",
            table_name="payment_authorizations",
            path=datasets_dir / "payment_authorizations.csv",
            schema={
                "auth_id": ("INTEGER", None),
                "merchant_id": ("INTEGER", None),
                "merchant_category": ("VARCHAR", 20),
                "card_hash": ("INTEGER", None),
                "amount_cents": ("INTEGER", None),
                "terminal_id": ("INTEGER", None),
            },
            queries=[
                QuerySpec("eq_terminal", ["auth_id"], ["terminal_id", "=", "5"], [], "equality"),
                QuerySpec("eq_category", ["auth_id"], ["merchant_category", "=", "'travel'"], [("bitmap", "merchant_category")], "equality"),
                QuerySpec("range_amount_selective", ["auth_id"], ["amount_cents", ">=", "245000"], [], "selective range"),
                QuerySpec("range_amount_nonselective", ["auth_id"], ["amount_cents", ">=", "1000"], [], "non-selective range"),
                QuerySpec("and_category_terminal", ["auth_id"], ["merchant_category", "=", "'grocery'", "AND", "terminal_id", "=", "5"], [("bitmap", "merchant_category")], "AND"),
                QuerySpec("agg_terminal_count", ["auth_id"], ["terminal_id", "=", "1"], [], "aggregation-friendly count"),
            ],
        ),
        DatasetSpec(
            name="shipment_tracking",
            table_name="shipment_tracking",
            path=datasets_dir / "shipment_tracking.csv",
            schema={
                "shipment_event_id": ("INTEGER", None),
                "event_time": ("INTEGER", None),
                "hub_id": ("INTEGER", None),
                "package_id": ("INTEGER", None),
                "status_code": ("INTEGER", None),
                "scan_delay_sec": ("INTEGER", None),
                "package_weight_g": ("INTEGER", None),
            },
            queries=[
                QuerySpec("eq_status", ["shipment_event_id"], ["status_code", "=", "3"], [("bitmap", "status_code")], "equality"),
                QuerySpec("eq_package", ["shipment_event_id"], ["package_id", "=", "500100"], [("rle", "package_id")], "equality"),
                QuerySpec("range_time_selective", ["shipment_event_id"], ["event_time", ">=", "1736130000"], [("zone_map", "event_time")], "selective range"),
                QuerySpec("range_time_nonselective", ["shipment_event_id"], ["event_time", ">=", "1735693200"], [("zone_map", "event_time")], "non-selective range"),
                QuerySpec("and_status_weight", ["shipment_event_id"], ["status_code", "=", "3", "AND", "package_weight_g", ">", "3000"], [("bitmap", "status_code")], "AND"),
                QuerySpec("agg_status_count", ["shipment_event_id"], ["status_code", "=", "0"], [("bitmap", "status_code")], "aggregation-friendly count"),
            ],
        ),
    ]
