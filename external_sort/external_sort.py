from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import sys
import tempfile
from pathlib import Path
from typing import Optional, Union


WRITE_BATCH_SIZE = 8192


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_sorted{input_path.suffix}")


def unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def write_numbers(output, values: list[int]) -> None:
    lines: list[str] = []
    for value in values:
        lines.append(f"{value}\n")
        if len(lines) >= WRITE_BATCH_SIZE:
            output.write("".join(lines).encode("ascii"))
            lines.clear()

    if lines:
        output.write("".join(lines).encode("ascii"))


def scan_ranges(input_path: Path, numbers_per_chunk: int) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []

    with open(input_path, "rb") as source:
        start = source.tell()
        count = 0

        while True:
            line = source.readline()
            if not line:
                end = source.tell()
                if count > 0:
                    ranges.append((start, end))
                break

            if line.strip():
                count += 1

            if count >= numbers_per_chunk:
                end = source.tell()
                ranges.append((start, end))
                start = end
                count = 0

    return ranges


def sort_range(task: tuple[str, int, int, str]) -> str:
    input_path, start, end, run_path = task
    values: list[int] = []

    with open(input_path, "rb") as source:
        source.seek(start)

        while source.tell() < end:
            line = source.readline()
            if not line:
                break

            stripped = line.strip()
            if stripped:
                values.append(int(stripped))

    values.sort()

    with open(run_path, "wb") as output:
        write_numbers(output, values)

    return run_path


class NumberReader:
    def __init__(self, path: str):
        self.file = open(path, "rb")

    def close(self) -> None:
        self.file.close()

    def __enter__(self) -> "NumberReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def take(self) -> Optional[int]:
        while True:
            line = self.file.readline()
            if not line:
                return None

            stripped = line.strip()
            if stripped:
                return int(stripped)


def flush_output(output, buffer: list[int]) -> None:
    if not buffer:
        return

    write_numbers(output, buffer)
    buffer.clear()


def merge_pair(task: tuple[str, str, str, int]) -> str:
    left_path, right_path, output_path, output_buffer_size = task
    output_buffer: list[int] = []

    with NumberReader(left_path) as left:
        with NumberReader(right_path) as right:
            with open(output_path, "wb") as output:
                left_value = left.take()
                right_value = right.take()

                while left_value is not None and right_value is not None:
                    if left_value <= right_value:
                        output_buffer.append(left_value)
                        left_value = left.take()
                    else:
                        output_buffer.append(right_value)
                        right_value = right.take()

                    if len(output_buffer) >= output_buffer_size:
                        flush_output(output, output_buffer)

                while left_value is not None:
                    output_buffer.append(left_value)
                    if len(output_buffer) >= output_buffer_size:
                        flush_output(output, output_buffer)
                    left_value = left.take()

                while right_value is not None:
                    output_buffer.append(right_value)
                    if len(output_buffer) >= output_buffer_size:
                        flush_output(output, output_buffer)
                    right_value = right.take()

                flush_output(output, output_buffer)

    return output_path


def pool_map(function, tasks: list[tuple], worker_count: int) -> list[str]:
    if not tasks:
        return []

    if worker_count <= 1 or len(tasks) == 1:
        return [function(task) for task in tasks]

    with mp.Pool(processes=worker_count) as pool:
        return list(pool.imap_unordered(function, tasks, chunksize=1))


def create_sorted_runs(
    input_path: Path,
    memory_numbers: int,
    cpu_count: int,
    temp_dir: Path,
) -> list[str]:
    sort_workers = min(cpu_count, memory_numbers)
    numbers_per_chunk = max(1, memory_numbers // sort_workers)
    ranges = scan_ranges(input_path, numbers_per_chunk)

    tasks: list[tuple[str, int, int, str]] = []
    for index, (start, end) in enumerate(ranges):
        run_path = temp_dir / f"run_{index:08d}.txt"
        tasks.append((str(input_path), start, end, str(run_path)))

    print(
        f"sort phase: workers={sort_workers}, chunk_numbers={numbers_per_chunk}, runs={len(tasks)}",
        file=sys.stderr,
    )
    return pool_map(sort_range, tasks, sort_workers)


def merge_all_runs(
    runs: list[str],
    memory_numbers: int,
    cpu_count: int,
    temp_dir: Path,
) -> str:
    pass_index = 0

    while len(runs) > 1:
        merge_workers = min(cpu_count, max(1, memory_numbers // 3), len(runs) // 2)
        output_buffer_size = max(1, memory_numbers // max(1, merge_workers * 3))

        tasks: list[tuple[str, str, str, int]] = []
        carried_runs: list[str] = []

        for pair_index in range(0, len(runs), 2):
            if pair_index + 1 >= len(runs):
                carried_runs.append(runs[pair_index])
                continue

            output_path = temp_dir / f"merge_{pass_index:04d}_{pair_index // 2:08d}.txt"
            tasks.append((runs[pair_index], runs[pair_index + 1], str(output_path), output_buffer_size))

        print(
            f"merge pass {pass_index}: workers={merge_workers}, pairs={len(tasks)}",
            file=sys.stderr,
        )

        merged_runs = pool_map(merge_pair, tasks, merge_workers)

        for left_path, right_path, _, _ in tasks:
            unlink_if_exists(Path(left_path))
            unlink_if_exists(Path(right_path))

        runs = merged_runs + carried_runs
        pass_index += 1

    return runs[0]


def external_sort(
    input_path: Union[str, Path],
    memory_numbers: int,
    output_path: Optional[Union[str, Path]] = None,
) -> Path:
    source = Path(input_path).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)

    if memory_numbers < 1:
        raise ValueError("memory_numbers must be positive.")

    destination = Path(output_path).resolve() if output_path else default_output_path(source)
    if destination == source:
        raise ValueError("Output file must be different from input file.")

    cpu_count = os.cpu_count() or 1
    destination.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix=f".{source.stem}_runs_", dir=source.parent) as temp:
        temp_dir = Path(temp)
        runs = create_sorted_runs(source, memory_numbers, cpu_count, temp_dir)

        if not runs:
            destination.write_bytes(b"")
            return destination

        final_run = runs[0] if len(runs) == 1 else merge_all_runs(runs, memory_numbers, cpu_count, temp_dir)
        os.replace(final_run, destination)

    return destination


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="External parallel sort for a large file with one integer per line."
    )
    parser.add_argument(
        "input_file",
        help="Path to the input file.",
    )
    parser.add_argument(
        "memory_numbers",
        type=int,
        help="Maximum number of integers that may be loaded into memory at the same time.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output = external_sort(args.input_file, args.memory_numbers)
    print(f"Done. Sorted file: {output}")


if __name__ == "__main__":
    mp.freeze_support()
    main()
