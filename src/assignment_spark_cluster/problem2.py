"""
Problem 2: Cluster Usage Analysis
Automatically extracts cluster/application IDs and start/end times
from Hadoop/YARN container logs.
"""

import os
import re
import pandas as pd
import matplotlib
matplotlib.use("Agg")  
import matplotlib.pyplot as plt
from datetime import datetime
from pyspark.sql import SparkSession

# Silence Spark INFO logs 
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.ui.showConsoleProgress=false pyspark-shell"
os.environ["PYSPARK_LOG_LEVEL"] = "ERROR"


def parse_timestamp(line):
    """Extract timestamp from a log line like '17/06/08 13:33:49 INFO ...'"""
    match = re.match(r"(\d{2}/\d{2}/\d{2}) (\d{2}:\d{2}:\d{2})", line)
    if match:
        date_str, time_str = match.groups()
        try:
            return datetime.strptime(date_str + " " + time_str, "%y/%m/%d %H:%M:%S")
        except Exception:
            return None
    return None


def main():
    # -----------------------------------
    # Initiate SparkSession
    # -----------------------------------
    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsage")
        .master("spark://172.31.90.126:7077")
        .getOrCreate()
    )

    base_dir = "data"
    input_dir = os.path.join(base_dir, "raw")
    output_dir = os.path.join(base_dir, "output")
    os.makedirs(output_dir, exist_ok=True)

    print(f"Scanning logs from: {input_dir}")

    # -----------------------------------
    # Read log data
    # -----------------------------------
    from glob import glob
    log_files = glob(os.path.join(input_dir, "*", "*.log"))
    print(f"Found {len(log_files)} log files.")

    records = []

    for file_path in log_files:
        cluster_match = re.search(r"application_(\d+)_\d+", file_path)
        app_match = re.search(r"(application_\d+_\d+)", file_path)

        if not cluster_match or not app_match:
            continue

        cluster_id = cluster_match.group(1)
        application_id = app_match.group(1)

        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                lines = f.readlines()
                if not lines:
                    continue
                start_time = parse_timestamp(lines[0])
                end_time = parse_timestamp(lines[-1])
        except Exception:
            continue

        if start_time and end_time:
            records.append((cluster_id, application_id, start_time, end_time))

    # make into pandas DataFrame
    df = pd.DataFrame(records, columns=["cluster_id", "application_id", "start_time", "end_time"])
    df["duration_sec"] = (df["end_time"] - df["start_time"]).dt.total_seconds()
    df.sort_values(["cluster_id", "start_time"], inplace=True)

    # -----------------------------------
    # output csv
    # -----------------------------------
    timeline_csv = os.path.join(output_dir, "problem2_timeline.csv")
    cluster_csv = os.path.join(output_dir, "problem2_cluster_summary.csv")
    stats_txt = os.path.join(output_dir, "problem2_stats.txt")

    df.to_csv(timeline_csv, index=False)

    # cluster summary
    cluster_summary = (
        df.groupby("cluster_id")
        .agg(
            num_applications=("application_id", "count"),
            cluster_first_app=("start_time", "min"),
            cluster_last_app=("end_time", "max")
        )
        .reset_index()
        .sort_values("num_applications", ascending=False)
    )
    cluster_summary.to_csv(cluster_csv, index=False)

    # stats file
    total_clusters = len(cluster_summary)
    total_apps = len(df)
    avg_apps_per_cluster = total_apps / total_clusters if total_clusters > 0 else 0

    with open(stats_txt, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for _, row in cluster_summary.head(5).iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

    print(f" Saved timeline: {timeline_csv}")
    print(f" Saved cluster summary: {cluster_csv}")
    print(f"Stats written: {stats_txt}")

    # -----------------------------------
    # Visulization
    # -----------------------------------
    if not cluster_summary.empty:
        plt.figure(figsize=(8, 4))
        plt.bar(cluster_summary["cluster_id"], cluster_summary["num_applications"], color="skyblue")
        plt.xticks(rotation=45, ha="right")
        plt.xlabel("Cluster ID")
        plt.ylabel("Number of Applications")
        plt.title("Applications per Cluster")
        for i, v in enumerate(cluster_summary["num_applications"]):
            plt.text(i, v + 0.5, str(v), ha="center", fontsize=8)
        bar_path = os.path.join(output_dir, "problem2_bar_chart.png")
        plt.tight_layout()
        plt.savefig(bar_path)
        plt.close()
        print(f"Saved bar chart: {bar_path}")

        # largest cluster duration density
        largest_cluster_id = cluster_summary.iloc[0]["cluster_id"]
        largest = df[df["cluster_id"] == largest_cluster_id]
        if not largest.empty:
            plt.figure(figsize=(8, 4))
            plt.hist(largest["duration_sec"], bins=40, alpha=0.7, color="salmon", edgecolor="black")
            plt.xscale("log")
            plt.xlabel("Job Duration (seconds, log scale)")
            plt.ylabel("Frequency")
            plt.title(f"Duration Distribution for Cluster {largest_cluster_id} (n={len(largest)})")
            density_path = os.path.join(output_dir, "problem2_density_plot.png")
            plt.tight_layout()
            plt.savefig(density_path)
            plt.close()
            print(f"Saved density plot: {density_path}")


    spark.stop()


if __name__ == "__main__":
    main()
