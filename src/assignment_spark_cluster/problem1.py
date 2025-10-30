from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
import os

def main():
    # -----------------------------------
    # Initiate SparkSession（on master）
    # -----------------------------------
    spark = (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")
        .master("local[*]")  
        .getOrCreate()
    )

    # -----------------------------------
    # config path
    # -----------------------------------
    base_dir = "/home/ubuntu/fall-2025-a06-jc3482/data"
    input_dir = f"file://{base_dir}/raw/*/*.log"
    output_dir = f"{base_dir}/output"
    os.makedirs(output_dir, exist_ok=True)

    print(f" Reading logs from: {input_dir}")

    # -----------------------------------
    # read logs from raw data path (not sample but all)
    # -----------------------------------
    logs = spark.read.text(input_dir)
    print(f" Total lines loaded: {logs.count():,}")

    # -----------------------------------
    # use regular expression to extract levels of logs
    # -----------------------------------
    logs = logs.withColumn(
        "log_level",
        regexp_extract(col("value"), r"(INFO|WARN|ERROR|DEBUG)", 1)
    ).filter(col("log_level") != "")

    # -----------------------------------
    # stats on  log level 
    # -----------------------------------
    counts = logs.groupBy("log_level").count().orderBy("count", ascending=False)

    counts.coalesce(1).write.csv(
        os.path.join(output_dir, "problem1_counts.csv"),
        header=True,
        mode="overwrite"
    )

    # -----------------------------------
    # sample 10 log rows
    # -----------------------------------
    sample_logs = logs.sample(False, 0.001).limit(10)
    sample_logs.coalesce(1).write.csv(
        os.path.join(output_dir, "problem1_sample.csv"),
        header=True,
        mode="overwrite"
    )

    # -----------------------------------
    # make summary documents
    # -----------------------------------
    total = logs.count()
    summary_path = os.path.join(output_dir, "problem1_summary.txt")

    with open(summary_path, "w") as f:
        f.write(f"Total log lines analyzed: {total}\n\n")
        f.write("Log level distribution:\n")
        for row in counts.collect():
            f.write(f"{row['log_level']}: {row['count']}\n")

    print("\nOutputs saved to:")
    print(f" - {output_dir}/problem1_counts.csv/")
    print(f" - {output_dir}/problem1_sample.csv/")
    print(f" - {summary_path}\n")

    spark.stop()

if __name__ == "__main__":
    main()
