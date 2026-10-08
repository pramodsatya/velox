#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Summarizes the output directory of jit-fusion-benchmark.sh as Markdown."""

import argparse
import csv
import os
import sqlite3
import statistics
from collections import Counter, defaultdict

ARMS = ["A", "B", "C"]


def read_csv(path, record):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [row for row in csv.DictReader(f) if row["record"] == record]


def ms(value):
    return f"{value:.3f}" if value < 10 else f"{value:.1f}"


def ratio(numerator, denominator):
    return f"{numerator / denominator:.2f}x" if denominator else "-"


def mib(value):
    return f"{value / 2**20:.1f}"


def rows_label(rows):
    rows = int(rows)
    for unit, size in (("M", 10**6), ("K", 10**3)):
        if rows >= size and rows % size == 0:
            return f"{rows // size}{unit}"
    return str(rows)


def hot_times(rows):
    """Median and range of ms per evaluation by (case, rows, null pct, arm)."""
    times = defaultdict(list)
    for row in rows:
        key = (int(row["case"]), int(row["rows"]), int(row["null_pct"]), row["arm"])
        times[key].append(float(row["ms_per_eval"]))
    return {
        key: (statistics.median(values), min(values), max(values), len(values))
        for key, values in times.items()
    }


def within(items, start, end):
    return [x for x in items if start <= x[0] and x[1] <= end]


def nsys_counts(path, iterations):
    """Kernels, copies and sets per evaluation by NVTX range, for ranges named
    <case>/<rows>/<null pct>/<arm>. An evaluation synchronizes its stream, so
    its GPU work lies within the range."""
    if not os.path.exists(path):
        return {}, {}
    con = sqlite3.connect(path)
    tables = {name for (name,) in con.execute("SELECT name FROM sqlite_master")}
    strings = dict(con.execute("SELECT id, value FROM StringIds"))
    ranges = []
    for start, end, text, text_id in con.execute(
        "SELECT start, end, text, textId FROM NVTX_EVENTS WHERE end IS NOT NULL"
    ):
        name = text if text is not None else strings.get(text_id, "")
        parts = name.split("/")
        if len(parts) == 4 and parts[3] in ARMS:
            key = (int(parts[0]), int(parts[1]), int(parts[2]), parts[3])
            ranges.append((start, end, key))

    def events(table, name_column=None):
        if table not in tables:
            return []
        column = f", {name_column}" if name_column else ", NULL"
        return list(con.execute(f"SELECT start, end{column} FROM {table}"))

    kernels = events("CUPTI_ACTIVITY_KIND_KERNEL", "shortName")
    copies = events("CUPTI_ACTIVITY_KIND_MEMCPY")
    sets = events("CUPTI_ACTIVITY_KIND_MEMSET")
    counts = {}
    names = {}
    for start, end, key in ranges:
        k = within(kernels, start, end)
        counts[key] = (
            len(k) / iterations,
            len(within(copies, start, end)) / iterations,
            len(within(sets, start, end)) / iterations,
            sum(e - s for s, e, _ in k) / iterations / 1e6,
        )
        names[key] = Counter(strings.get(n, str(n)) for _, _, n in k)
    return counts, names


def per_arm(values, cell):
    return " / ".join("-" if x is None else cell(x) for x in values)


def median_of(rows, field):
    return statistics.median(float(r[field]) for r in rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dir")
    parser.add_argument("--profile-iterations", type=int, default=20)
    parser.add_argument("--rows", type=int, default=10_000_000)
    args = parser.parse_args()

    cases = {}
    with open(os.path.join(args.dir, "cases.csv")) as f:
        for row in csv.DictReader(f):
            cases[int(row["case"])] = row["sql"]

    times = hot_times(read_csv(os.path.join(args.dir, "time.csv"), "time"))
    a0 = hot_times(read_csv(os.path.join(args.dir, "time-a0.csv"), "time"))
    allocations = {
        (int(r["case"]), int(r["rows"]), int(r["null_pct"]), r["arm"]): r
        for r in read_csv(os.path.join(args.dir, "time.csv"), "alloc")
    }
    counts, names = nsys_counts(
        os.path.join(args.dir, "profile.sqlite"), args.profile_iterations
    )
    configs = sorted({key[:3] for key in times})
    rounds = max((t[3] for t in times.values()), default=0)

    print(f"## Hot time per evaluation at {rows_label(args.rows)} rows\n")
    print(
        f"Milliseconds per evaluation and stream synchronization, median of "
        f"{rounds} interleaved rounds (min-max). B vs A is the gain from one "
        f"kernel per function, C vs B the gain from fusion alone.\n"
    )
    print("| # | Expression | Nulls | A | B | C | B vs A | C vs B | C vs A |")
    print("|---|---|---|---|---|---|---|---|---|")
    for case, rows, nulls in configs:
        if rows != args.rows:
            continue
        t = [times.get((case, rows, nulls, arm)) for arm in ARMS]
        if None in t:
            continue
        cells = [f"{ms(x[0])} ({ms(x[1])}-{ms(x[2])})" for x in t]
        print(
            f"| {case} | `{cases[case]}` | {nulls}% | {' | '.join(cells)} | "
            f"{ratio(t[0][0], t[1][0])} | {ratio(t[1][0], t[2][0])} | "
            f"{ratio(t[0][0], t[2][0])} |"
        )

    print("\n## Scaling\n")
    print("Median milliseconds per evaluation; speedups are of C over A.\n")
    print("| # | Rows | Nulls | A | B | C | C vs A |")
    print("|---|---|---|---|---|---|---|")
    for case, rows, nulls in configs:
        t = [times.get((case, rows, nulls, arm)) for arm in ARMS]
        if None in t:
            continue
        print(
            f"| {case} | {rows_label(rows)} | {nulls}% | "
            f"{' | '.join(ms(x[0]) for x in t)} | {ratio(t[0][0], t[2][0])} |"
        )

    if counts or allocations:
        print(f"\n## Work per evaluation at {rows_label(args.rows)} rows\n")
        print(
            "Kernels, device copies and memsets per evaluation and the GPU time "
            "of its kernels in ms (nsys), and MiB allocated per evaluation "
            "besides the result (the intermediate columns and buffers), by arm "
            "A / B / C.\n"
        )
        print(
            "| # | Nulls | Kernels | Copies | Memsets | Kernel ms | "
            "Intermediate MiB | Result MiB |"
        )
        print("|---|---|---|---|---|---|---|---|")
        for case, rows, nulls in configs:
            if rows != args.rows:
                continue
            c = [counts.get((case, rows, nulls, arm)) for arm in ARMS]
            a = [allocations.get((case, rows, nulls, arm)) for arm in ARMS]
            inter = " / ".join(
                "-"
                if x is None
                else mib(int(x["alloc_bytes"]) - int(x["result_bytes"]))
                for x in a
            )
            result = mib(int(a[0]["result_bytes"])) if a[0] else "-"
            print(
                f"| {case} | {nulls}% | {per_arm(c, lambda x: f'{x[0]:g}')} | "
                f"{per_arm(c, lambda x: f'{x[1]:g}')} | "
                f"{per_arm(c, lambda x: f'{x[2]:g}')} | "
                f"{per_arm(c, lambda x: f'{x[3]:.3f}')} | {inter} | {result} |"
            )

    cold = read_csv(os.path.join(args.dir, "cold.csv"), "cold")
    if cold:
        runs = defaultdict(list)
        for row in cold:
            runs[(int(row["case"]), row["arm"])].append(row)
        print("\n## Cold start at 1K rows\n")
        print(
            "Milliseconds in fresh processes with empty kernel caches, median "
            f"of {max(len(r) for r in runs.values())} runs. Each process first "
            "compiles an unrelated JIT kernel, then an unrelated JIT kernel "
            "with a custom op, which NVRTC compiles to LTO-IR with other "
            "options; these pay what a process pays once for each kind of "
            "kernel. First eval is the case's first evaluation, which compiles "
            "its kernels; extra = first eval - hot eval. Break-even "
            "is when C's hot saving over A repays its extra cold cost, in "
            f"evaluations of {rows_label(args.rows)} rows and in rows.\n"
        )
        print(
            "| # | First JIT kernel | First custom-op kernel | "
            "First eval A / B / C | Extra A / B / C | "
            "Break-even evaluations | Break-even rows |"
        )
        print("|---|---|---|---|---|---|---|")
        for case in sorted({c for c, _ in runs}):
            arm_runs = [runs.get((case, arm), []) for arm in ARMS]
            if not all(arm_runs):
                continue
            process = median_of(sum(arm_runs, []), "process_first_jit_ms")
            process_custom_op = median_of(
                sum(arm_runs, []), "process_first_custom_op_jit_ms"
            )
            first = [median_of(r, "first_eval_ms") for r in arm_runs]
            extra = [
                median_of(r, "first_eval_ms") - median_of(r, "hot_eval_ms")
                for r in arm_runs
            ]
            hot = [times.get((case, args.rows, 0, arm)) for arm in ("A", "C")]
            evals = rows = "-"
            if extra[2] <= extra[0]:
                evals = rows = "0 (C is not slower cold)"
            elif None not in hot and hot[0][0] > hot[1][0]:
                n = (extra[2] - extra[0]) / (hot[0][0] - hot[1][0])
                evals = f"{n:,.0f}"
                rows = f"{n * args.rows:,.0f}"
            elif None not in hot:
                evals = rows = "never (C is not faster hot)"
            print(
                f"| {case} | {ms(process)} | {ms(process_custom_op)} | "
                f"{' / '.join(ms(x) for x in first)} | "
                f"{' / '.join(ms(x) for x in extra)} | {evals} | {rows} |"
            )

    if a0:
        print("\n## Arm A with and without the cuDF patch\n")
        print(
            "Median milliseconds per evaluation of arm A on this build and on "
            "a build of the same Velox without the cuDF JIT call-node patch "
            "(A0).\n"
        )
        print("| # | Rows | Nulls | A0 | A | A vs A0 |")
        print("|---|---|---|---|---|---|")
        for case, rows, nulls in configs:
            x = times.get((case, rows, nulls, "A"))
            y = a0.get((case, rows, nulls, "A"))
            if x and y:
                print(
                    f"| {case} | {rows_label(rows)} | {nulls}% | {ms(y[0])} | "
                    f"{ms(x[0])} | {ratio(y[0], x[0])} |"
                )

    if names:
        with open(os.path.join(args.dir, "kernels.md"), "w") as f:
            f.write(f"# Kernels per evaluation at {rows_label(args.rows)} rows\n")
            for key in sorted(names):
                f.write(f"\n## Case {key[0]}, {key[2]}% nulls, arm {key[3]}\n\n")
                for name, n in names[key].most_common():
                    f.write(f"- {n / args.profile_iterations:g} x `{name[:120]}`\n")


if __name__ == "__main__":
    main()
