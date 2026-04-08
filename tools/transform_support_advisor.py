#!/usr/bin/env python3
import argparse


def contains(expr: str, token: str) -> bool:
    return token.lower() in (expr or "").lower()


def main() -> int:
    parser = argparse.ArgumentParser(description="Heuristic transform support advisor")
    parser.add_argument("--source-type", default="mysql")
    parser.add_argument("--sink-type", default="doris")
    parser.add_argument("--projection", default="")
    parser.add_argument("--filter", default="")
    args = parser.parse_args()

    source = (args.source_type or "").lower()
    sink = (args.sink_type or "").lower()
    projection = args.projection or ""
    filter_expr = args.filter or ""

    lines = []
    lines.append(f"source={source}, sink={sink}")

    risky = []
    if contains(projection, "cast(c1 as int)") or contains(projection, "cast(c3 as int)"):
        risky.append("string-to-int CAST may fail on non-numeric values and can stall or drop records")
    if contains(filter_expr, "c0 <= 20000") or contains(filter_expr, "between -50000 and 50000"):
        risky.append("strict filter may legitimately produce near-zero sink rows")

    if sink == "doris" and projection.strip():
        lines.append("doris note: transform projection changes sink schema; verify projected columns match expectations")

    if risky:
        lines.append("risk-level=high")
        for r in risky:
            lines.append(f"risk: {r}")
    else:
        lines.append("risk-level=low")
        lines.append("no obvious transform compatibility risks detected by heuristic rules")

    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
