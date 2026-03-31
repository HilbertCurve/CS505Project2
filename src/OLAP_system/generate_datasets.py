"""
Generate four CSV datasets that each exhibit different data characteristics
relevant to compression and indexing strategies.
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


# Generate 10,000 rows per dataset so the files are large enough to expose
# compression and indexing behavior without being too annoying to inspect.
DEFAULT_ROW_COUNT = 10_000

# Store generated CSV files alongside the other project test data.
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "test" / "datasets"

# Fixed seed so the generated datasets are reproducible.
RNG = random.Random(505)


def write_csv(path: Path, headers: list[str], rows: list[list[object]]) -> None:
    # Ensure the output folder exists before writing the file.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)
        writer.writerows(rows)


def build_customer_accounts(rows: int) -> list[list[object]]:
    # Dataset 1: customer subscription accounts
    # - account_id is the primary identifier
    # - is_active and status_code describe the account state
    # - region_code and plan_tier describe where the customer belongs and what
    #   service plan they pay for
    # - account_balance_cents is usually small but occasionally very large
    #
    # Requirements covered:
    # - low-cardinality boolean/status
    # - skew
    # - a column with a few large outliers for mostly encoding
    records = []

    for account_id in range(1, rows + 1):
        # Low-cardinality for is_active flag field
        is_active = 1 if RNG.random() < 0.5 else 0

        # Status values represent: 0 = good standing, 1 = past due, 2 = suspended
        status_code = RNG.choices([0, 1, 2], weights=[84, 11, 5], k=1)[0]

        # Region distribution is also skewed toward a primary market
        region_code = RNG.choices([10, 20, 30, 40], weights=[48, 27, 15, 10], k=1)[0]
        region_name = {10: "north", 20: "south", 30: "east", 40: "west"}[region_code]

        # plan_tier represents: 1 = basic, 2 = standard, 3 = premium
        plan_tier = RNG.choices([1, 2, 3], weights=[52, 33, 15], k=1)[0]
        plan_name = {1: "basic", 2: "standard", 3: "premium"}[plan_tier]

        # Account balances for active, past due, and suspended accounts
        if status_code == 0:
            account_balance_cents = (account_id % 6) * 100
        elif status_code == 1:
            account_balance_cents = 800 + (account_id % 8) * 250
        else:
            account_balance_cents = 2500 + (account_id % 10) * 400

        # Rare major delinquencies for a few large outliers
        if account_id % 500 == 0:
            account_balance_cents = 500_000 + RNG.randint(-250_000, 250_000)

        records.append(
            [
                account_id,
                is_active,
                status_code,
                region_code,
                region_name,
                plan_tier,
                plan_name,
                account_balance_cents,
            ]
        )

    return records


def build_warehouse_inventory(rows: int) -> list[list[object]]:
    # Dataset 2: inventory records stored by warehouse location
    # - inventory_id is the row identifier
    # - warehouse_id groups nearby rows into physical clusters
    # - aisle_id and shelf_id describe storage location
    # - product_id is sorted within each warehouse block
    # - units_on_hand are in a uniform random range
    # - restock_ts moves forward in time as rows progress
    #
    # Requirements covered:
    # - sorted or clustered integers
    # - uniform random integers
    # - monotonic timestamps/IDs
    records = []
    start_time = datetime(2024, 1, 1, 8, 0, 0)

    for row_index in range(rows):
        # Each 250-row block belongs to one warehouse, creating clear clustering
        warehouse_id = 100 + (row_index // 250)

        # Physical location fields are derived from the warehouse block
        warehouse_zone = ["bulk", "cold_storage", "general", "returns"][warehouse_id % 4]
        aisle_id = ((row_index % 250) // 25) + 1
        shelf_id = ((row_index % 250) // 5) + 1

        # Within a warehouse, product ids are sorted in ascending order
        product_id = warehouse_id * 10_000 + (row_index % 250)

        # Quantities are uniformly random between 1 and 100
        units_on_hand = RNG.randint(1, 100)

        # Restock timestamps increase monotonically
        restock_ts = int((start_time + timedelta(minutes=row_index)).timestamp())

        records.append(
            [
                row_index + 1,
                warehouse_id,
                warehouse_zone,
                aisle_id,
                shelf_id,
                product_id,
                units_on_hand,
                restock_ts,
            ]
        )

    return records


def build_payment_authorizations(rows: int) -> list[list[object]]:
    # Dataset 3: payment authorization attempts
    # - auth_id identifies the authorization request
    # - merchant_id, card_hash, amount_cents, and response_ms are sampled from
    #   large uniform domains
    # - terminal_id is a smaller location/device bucket
    #
    # Requirements covered:
    # - uniform random integers
    records = []

    for auth_id in range(1, rows + 1):
        # Columns from large uniform ranges
        merchant_id = RNG.randint(100_000, 999_999)
        card_hash = RNG.randint(1, 999_999)
        amount_cents = RNG.randint(100, 250_000)
        merchant_category = ["grocery", "fuel", "travel", "retail", "restaurant"][merchant_id % 5]

        # A smaller uniformly random terminal id acts like a store/device bucket
        terminal_id = RNG.randint(1, 100)

        records.append(
            [
                auth_id,
                merchant_id,
                merchant_category,
                card_hash,
                amount_cents,
                terminal_id,
            ]
        )

    return records


def build_shipment_tracking(rows: int) -> list[list[object]]:
    # Dataset 4: package shipment tracking events
    # - shipment_event_id and event_time increase steadily
    # - hub_id identifies the facility that scanned the package
    # - package_id identifies the package being moved
    # - status_code represents the current shipment state
    # - scan_delay_sec is a small operational metric
    # - package_weight_g is usually modest but has a few very heavy outliers
    #
    # Requirements covered:
    # - monotonic timestamps/IDs
    # - a column with a few large outliers for mostly encoding
    records = []
    start_time = datetime(2025, 1, 1, 0, 0, 0)

    for shipment_event_id in range(1, rows + 1):
        event_time = int((start_time + timedelta(seconds=shipment_event_id * 45)).timestamp())
        hub_id = 200 + (shipment_event_id % 24)

        # A package stays "active" for a short run of scan events before the next package id appears
        package_id = 500_000 + ((shipment_event_id - 1) // 4)

        # Status values represent:
        # 0 = label created, 1 = arrived at hub, 2 = departed hub, 3 = delivered
        status_code = (shipment_event_id - 1) % 4

        # Scan delays stay small enough to look like operational telemetry
        scan_delay_sec = 15 + (shipment_event_id % 120)

        # Most packages are light parcels, so this column is mostly small
        package_weight_g = 250 + ((package_id * 7 + status_code * 13) % 4_500)

        # Rare oversized shipments for large outliers
        if shipment_event_id % 500 == 0:
            package_weight_g = 250_000 + RNG.randint(0, 100_000)

        records.append(
            [
                shipment_event_id,
                event_time,
                hub_id,
                package_id,
                status_code,
                scan_delay_sec,
                package_weight_g,
            ]
        )

    return records


def generate_all_datasets(rows: int = DEFAULT_ROW_COUNT, output_dir: Path = OUTPUT_DIR) -> list[Path]:
    # Register the four datasets and their schemas
    datasets = [
        (
            "dataset_status_skew.csv",
            [
                "account_id",
                "is_active",
                "status_code",
                "region_code",
                "region_name",
                "plan_tier",
                "plan_name",
                "account_balance_cents",
            ],
            build_customer_accounts(rows),
        ),
        (
            "dataset_clustered_sorted.csv",
            [
                "inventory_id",
                "warehouse_id",
                "warehouse_zone",
                "aisle_id",
                "shelf_id",
                "product_id",
                "units_on_hand",
                "restock_ts",
            ],
            build_warehouse_inventory(rows),
        ),
        (
            "dataset_uniform_random.csv",
            [
                "auth_id",
                "merchant_id",
                "merchant_category",
                "card_hash",
                "amount_cents",
                "terminal_id",
            ],
            build_payment_authorizations(rows),
        ),
        (
            "dataset_time_series.csv",
            [
                "shipment_event_id",
                "event_time",
                "hub_id",
                "package_id",
                "status_code",
                "scan_delay_sec",
                "package_weight_g",
            ],
            build_shipment_tracking(rows),
        ),
    ]

    # Write each dataset to a CSV file in the output directory
    for filename, headers, records in datasets:
        path = output_dir / filename
        write_csv(path, headers, records)

    return 0


if __name__ == "__main__":
    # Running this file directly generates all four CSV files
    generate_all_datasets()
