"""Hard-coded evaluation runner for bitmap, zone maps, and RLE."""

from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(ROOT_DIR / "src"))
sys.path.insert(0, str(ROOT_DIR / "test" / "datasets"))

import OLAP_system.database as database
from generate_datasets import generate_all_datasets
from query.query_suite import build_dataset_specs, DatasetSpec, QuerySpec
import OLAP_system.timer as timer


REPORT_PATH = ROOT_DIR / "test" / "hardCodedQueries_report.txt"
TABLE_HEADER = (
    f"{'Mode':<24}"
    f"{'Rows':>8}  "
    f"{'Q Time':>10}  "
    f"{'Build':>10}  "
    f"{'Scanned':>8}  "
    f"{'Skipped':>10}  "
    f"{'Bytes':>8}  "
    f"{'Used':<20}"
)


def load_dataset(dataset_spec: DatasetSpec) -> None:
    database.reset_database()
    timer.reset_metrics()
    database.create_table(dataset_spec.table_name, dataset_spec.schema)
    database.load_csv(dataset_spec.table_name, str(dataset_spec.path))


def result_ids(frame, id_column: str) -> list[int]:
    return frame[id_column].tolist()


def append_report(lines: list[str]) -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("a", encoding="utf-8") as report_file:
        for line in lines:
            report_file.write(line + "\n")


def format_result_row(
    mode: str,
    rows,
    q_time,
    build,
    scanned,
    skipped,
    bytes_used,
    used,
) -> str:
    build_str = "-" if build is None else f"{build:.6f}"
    bytes_str = "-" if bytes_used is None else str(bytes_used)
    return (
        f"{mode:<24}"
        f"{rows:>8}  "
        f"{q_time:>10.6f}  "
        f"{build_str:>10}  "
        f"{scanned:>8}  "
        f"{skipped:>10}  "
        f"{bytes_str:>8}  "
        f"{used:<20}"
    )


def run_baseline(dataset_spec: DatasetSpec, query_spec: QuerySpec):
    load_dataset(dataset_spec)
    result = database.handle_select(dataset_spec.table_name, query_spec.select_columns, query_spec.predicate)
    metric = timer.last_query_time
    assert metric is not None
    return result, metric


def run_indexed(dataset_spec: DatasetSpec, query_spec: QuerySpec, indexer: str, column: str):
    load_dataset(dataset_spec)
    build_metric = database.handle_index(dataset_spec.table_name, column, indexer)
    result = database.handle_select(dataset_spec.table_name, query_spec.select_columns, query_spec.predicate)
    query_metric = timer.last_query_time
    assert query_metric is not None
    return result, build_metric, query_metric


def evaluate_query(dataset_spec: DatasetSpec, query_spec: QuerySpec) -> None:
    id_column = next(iter(dataset_spec.schema.keys()))

    baseline_result, baseline_metric = run_baseline(dataset_spec, query_spec)
    baseline_ids = result_ids(baseline_result, id_column)

    append_report(
        [
            f"  Query: {query_spec.name} [{query_spec.notes}]",
            f"  Predicate: {' '.join(query_spec.predicate)}",
            f"  {'-' * len(TABLE_HEADER)}",
            f"  {TABLE_HEADER}",
            f"  {'-' * len(TABLE_HEADER)}",
            "  "
            + format_result_row(
                "baseline",
                baseline_metric.rows_matched,
                baseline_metric.total_time,
                None,
                baseline_metric.rows_scanned,
                f"{baseline_metric.segments_skipped}/{baseline_metric.segments_total}",
                None,
                baseline_metric.index_used,
            ),
        ]
    )

    for indexer, column in query_spec.structures:
        indexed_result, build_metric, query_metric = run_indexed(dataset_spec, query_spec, indexer, column)
        indexed_ids = result_ids(indexed_result, id_column)

        assert indexed_ids == baseline_ids, (
            f"Mismatch for dataset={dataset_spec.name}, query={query_spec.name}, "
            f"index={indexer}({column})"
        )
        assert build_metric.index_type == indexer
        assert build_metric.bytes_used > 0

        append_report(
            [
                "  "
                + format_result_row(
                    f"{indexer}({column})",
                    query_metric.rows_matched,
                    query_metric.total_time,
                    build_metric.time,
                    query_metric.rows_scanned,
                    f"{query_metric.segments_skipped}/{query_metric.segments_total}",
                    build_metric.bytes_used,
                    query_metric.index_used,
                )
            ]
        )

    append_report([""])


def run_suite() -> None:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("", encoding="utf-8")
    append_report(["Generating datasets...", ""])
    generate_all_datasets()

    append_report(["Running hard-coded query evaluation...", ""])
    dataset_specs = build_dataset_specs(ROOT_DIR)
    assert len(dataset_specs) >= 4

    for dataset_spec in dataset_specs:
        append_report([f"Dataset: {dataset_spec.name}", "=" * (9 + len(dataset_spec.name)), ""])
        assert dataset_spec.path.exists(), f"Missing dataset: {dataset_spec.path}"
        assert len(dataset_spec.queries) >= 6

        for query_spec in dataset_spec.queries:
            evaluate_query(dataset_spec, query_spec)

    append_report(["All hard-coded evaluations passed."])
    print(f"Saved report to {REPORT_PATH}")


if __name__ == "__main__":
    run_suite()
