## Analysis Report
### Problem 1: Log Pattern Extraction
For Problem 1, I used PySpark to scan all the raw log files in data/raw. The script used regular expressions to extract timestamps, log levels, and messages. I then converted them into a structured DataFrame and aggregated counts by log level.

In total, 27,410,336 log lines were analyzed. Most of them were informational messages. The log level distribution was:

INFO: 27,389,482

ERROR: 11,259

WARN: 9,595

This shows that the cluster was running stably, with very few errors or warnings. The output files include problem1_counts.csv and problem1_summary.txt, which summarize the results.

The Spark job took around 45–55 seconds to run with three executors. Caching the intermediate DataFrame reduced recomputation. I also turned off Spark’s INFO-level logs to make the output easier to read. Overall, the task ran smoothly and efficiently.

### Problem 2: Cluster Usage Analysis

For Problem 2, I analyzed application-level activity across different clusters. The script extracted each application’s cluster ID, start time, end time, and duration. Then I created two main outputs: problem2_timeline.csv for detailed events and problem2_cluster_summary.csv for aggregated results.

The dataset included 5 unique clusters and 3,442 total applications. On average, each cluster ran about 688 applications, but usage was very uneven. One cluster (1485248649253) handled 3,220 applications, which is about 93 percent of all jobs. The rest had small workloads, and one cluster only ran a single job. This suggests that one main production cluster handled almost everything while smaller ones were used for testing or short-term runs.

The Spark job finished in about 50 seconds. I noticed that reading many small log files slowed down the first stage, so I coalesced partitions to improve performance. Memory usage stayed under 500 MB per node.

Visualization Summary

The Applications per Cluster bar chart clearly shows how uneven the workload was. Cluster 1485248649253 dominated with 3,220 apps, while the others had only a few dozen.

The Duration Distribution plot focuses on that main cluster. It uses a log-scaled x-axis to show job lengths. Most jobs finished within 10 seconds, but a few took much longer—up to 10,000 seconds. This pattern suggests most tasks were short, with a few long or complex ones creating a heavy tail.