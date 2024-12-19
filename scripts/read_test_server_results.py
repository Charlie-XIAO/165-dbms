#!/usr/bin/env python3

import argparse
import re

PATTERN = re.compile(r"(Test|Benchmark)\s(\d+).*?Runtime:\s(\d+)\sus")
DT_PATTERN = re.compile(
    r"up to milestone \d as of \b[A-Za-z]{3} (\b[A-Za-z]{3} \d{1,2}) (\d{2}:\d{2}:\d{2} [APM]{2}) EST \d{4}"
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", required=True, help="Output file")
    args = parser.parse_args()
    output_file = open(args.output, "w")

    print("Accepting input from stdin...")
    input_lines = []
    try:
        while True:
            line = input()
            input_lines.append(line)
    except EOFError:
        pass

    input_text = "\n".join(input_lines)
    dt_match = DT_PATTERN.search(input_text)
    matches = PATTERN.findall(input_text)
    mapping = {(type_, int(num)): int(runtime) for type_, num, runtime in matches}

    print(dt_match.group(1), file=output_file)  # Month and day
    print(dt_match.group(2), file=output_file)  # Time

    for num in range(1, 8):
        benchmark_perf = mapping.get(("Benchmark", num), "")
        print(benchmark_perf, file=output_file)

    for num in range(1, 66):
        test_perf = mapping.get(("Test", num), "")
        print(test_perf, file=output_file)

    output_file.close()


if __name__ == "__main__":
    main()
