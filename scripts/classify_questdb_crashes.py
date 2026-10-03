#!/usr/bin/env python3
"""Classify QuestDB/JVM crash logs without changing their source directory."""

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from statistics import median
from typing import Iterable, Optional


_SIGNAL_RE = re.compile(r"^#\s+(SIG[A-Z0-9]+)\s+\(", re.MULTILINE)
_SI_CODE_RE = re.compile(
    r"^siginfo:.*?si_code:\s*-?\d+\s*\(([^)]+)\)",
    re.MULTILINE,
)
_TIME_RE = re.compile(
    r"^Time:\s+(.+?)\s+elapsed time:\s*([0-9.]+) seconds",
    re.MULTILINE,
)
_PROBLEM_FRAME_RE = re.compile(
    r"^#\s+[Jj]\s+.*?\b(io\.questdb[^\s(]+)",
    re.MULTILINE,
)
_JAVA_FRAME_RE = re.compile(r"^\s*[Jj]\s+.*?\b(io\.questdb[^\s(]+)")


@dataclass(frozen=True)
class CrashRecord:
    filename: str
    crash_time: Optional[datetime]
    signal: Optional[str]
    si_code: Optional[str]
    uptime_seconds: Optional[float]
    first_questdb_frame: Optional[str]


def _parse_time(value: str) -> datetime:
    normalized = " ".join(value.split())
    parsed = datetime.strptime(normalized, "%a %b %d %H:%M:%S %Y UTC")
    return parsed.replace(tzinfo=timezone.utc)


def _first_questdb_frame(text: str) -> Optional[str]:
    problematic = _PROBLEM_FRAME_RE.search(text)
    if problematic:
        return problematic.group(1)

    marker = text.find("Java frames:")
    if marker < 0:
        return None

    java_stack = text[marker:].split("---------------  P R O C E S S", 1)[0]
    for line in java_stack.splitlines()[1:]:
        frame = _JAVA_FRAME_RE.search(line)
        if frame:
            return frame.group(1)
    return None


def classify_file(path: Path) -> CrashRecord:
    """Parse one JVM error file, tolerating incomplete logs."""
    text = path.read_text(encoding="utf-8", errors="replace")
    signal_match = _SIGNAL_RE.search(text)
    code_match = _SI_CODE_RE.search(text)
    time_match = _TIME_RE.search(text)

    crash_time = None
    uptime_seconds = None
    if time_match:
        crash_time = _parse_time(time_match.group(1))
        uptime_seconds = float(time_match.group(2))

    return CrashRecord(
        filename=path.name,
        crash_time=crash_time,
        signal=signal_match.group(1) if signal_match else None,
        si_code=code_match.group(1) if code_match else None,
        uptime_seconds=uptime_seconds,
        first_questdb_frame=_first_questdb_frame(text),
    )


def _sort_key(path: Path) -> tuple[int, int | str]:
    crash_number = re.fullmatch(r"crash\+(\d+)\.log", path.name)
    if crash_number:
        return (0, int(crash_number.group(1)))
    return (1, path.name)


def classify_directory(directory: Path) -> list[CrashRecord]:
    """Classify crash+*.log and hs_err*.log files in deterministic order."""
    paths = set(directory.glob("crash+*.log")) | set(directory.glob("hs_err*.log"))
    return [classify_file(path) for path in sorted(paths, key=_sort_key)]


def _label(value: Optional[str], missing: str) -> str:
    return value if value is not None else missing


def _print_counts(title: str, counts: Counter[str]) -> None:
    print(title)
    for value, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        print(f"{count}\t{value}")


def print_report(records: Iterable[CrashRecord]) -> None:
    """Print per-file classifications followed by deterministic aggregates."""
    records = list(records)
    print("FILES")
    print("filename\tdate_utc\tsignal\tsi_code\tuptime_seconds\tfirst_questdb_frame")
    for record in records:
        crash_time = (
            record.crash_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            if record.crash_time is not None
            else "UNAVAILABLE"
        )
        uptime = (
            f"{record.uptime_seconds:.6f}"
            if record.uptime_seconds is not None
            else "UNAVAILABLE"
        )
        print(
            f"{record.filename}\t{crash_time}\t"
            f"{_label(record.signal, 'NO_SIGNAL_LINE')}\t"
            f"{_label(record.si_code, 'NO_SI_CODE')}\t{uptime}\t"
            f"{_label(record.first_questdb_frame, 'NO_QUESTDB_JAVA_FRAME')}"
        )

    print(f"SUMMARY\tfiles={len(records)}")
    _print_counts(
        "SIGNALS",
        Counter(_label(record.signal, "NO_SIGNAL_LINE") for record in records),
    )
    _print_counts(
        "SI_CODES",
        Counter(_label(record.si_code, "NO_SI_CODE") for record in records),
    )
    _print_counts(
        "FIRST_QUESTDB_FRAMES",
        Counter(
            _label(record.first_questdb_frame, "NO_QUESTDB_JAVA_FRAME")
            for record in records
        ),
    )
    _print_counts(
        "DATES_UTC",
        Counter(
            record.crash_time.date().isoformat()
            if record.crash_time is not None
            else "UNAVAILABLE"
            for record in records
        ),
    )

    uptimes = [
        record.uptime_seconds
        for record in records
        if record.uptime_seconds is not None
    ]
    if uptimes:
        print(
            "UPTIME_SECONDS\t"
            f"count={len(uptimes)}\tmedian={median(uptimes):.6f}\tmax={max(uptimes):.6f}"
        )
    else:
        print("UPTIME_SECONDS\tcount=0\tmedian=UNAVAILABLE\tmax=UNAVAILABLE")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="Directory containing QuestDB crash logs")
    args = parser.parse_args()

    if not args.directory.is_dir():
        parser.error(f"not a directory: {args.directory}")

    print_report(classify_directory(args.directory))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
