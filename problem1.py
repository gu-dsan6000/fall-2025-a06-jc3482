import argparse
import os
import re
import random
import pandas as pd

def main(input_path: str, output_path: str):
    print(f"\n[INFO] Reading logs from: {input_path}")

    # 收集所有日志文件路径
    log_files = []
    for root, _, files in os.walk(input_path):
        for f in files:
            if f.endswith(".log") or "stderr" in f or "stdout" in f or "syslog" in f:
                log_files.append(os.path.join(root, f))

    if not log_files:
        print("[WARN] No log files found.")
        return

    log_pattern = re.compile(r"\b(INFO|WARN|ERROR|DEBUG)\b")

    total_lines = 0
    matched_lines = []
    level_counts = {}

    # 读取日志并解析
    for log_file in log_files:
        with open(log_file, "r", errors="ignore") as f:
            for line in f:
                total_lines += 1
                m = log_pattern.search(line)
                if m:
                    level = m.group(1)
                    level_counts[level] = level_counts.get(level, 0) + 1
                    matched_lines.append((level, line.strip()))

    # 转为 DataFrame
    df = pd.DataFrame(matched_lines, columns=["log_level", "log_entry"])

    # 输出目录
    os.makedirs(output_path, exist_ok=True)
    counts_path = os.path.join(output_path, "problem1_counts.csv")
    sample_path = os.path.join(output_path, "problem1_sample.csv")
    summary_path = os.path.join(output_path, "problem1_summary.txt")

    # 写出 counts
    counts_df = pd.DataFrame(list(level_counts.items()), columns=["log_level", "count"])
    counts_df.sort_values("count", ascending=False).to_csv(counts_path, index=False)

    # 写出 sample
    if not df.empty:
        df.sample(min(10, len(df))).to_csv(sample_path, index=False)

    # 写出 summary
    total_with_levels = sum(level_counts.values())
    with open(summary_path, "w") as f:
        f.write(f"Total log lines processed: {total_lines}\n")
        f.write(f"Total lines with log levels: {total_with_levels}\n")
        f.write(f"Unique log levels found: {len(level_counts)}\n\n")
        f.write("Log level distribution:\n")
        for lvl, cnt in sorted(level_counts.items(), key=lambda x: -x[1]):
            pct = (cnt / total_with_levels) * 100 if total_with_levels else 0
            f.write(f"  {lvl:6s}: {cnt:>8,d} ({pct:6.2f}%)\n")

    print("\n[INFO] Output written to:")
    print(f"  {counts_path}")
    print(f"  {sample_path}")
    print(f"  {summary_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution (local version)")
    parser.add_argument("--input", required=True, help="Input directory path (e.g., data/sample)")
    parser.add_argument("--output", required=True, help="Output directory path (e.g., data/output)")
    args = parser.parse_args()
    main(args.input, args.output)
