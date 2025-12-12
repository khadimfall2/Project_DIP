import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
df = pd.read_csv("benchmark_results.csv")

# Ensure numeric columns are floats
df["Write Throughput (ops/sec)"] = df["Write Throughput (ops/sec)"].astype(float)
df["Read Latency (ms)"] = df["Read Latency (ms)"].astype(float)
df["Compression (%)"] = df["Compression (%)"].astype(float)

# Set up the phases as x-axis categories
phases = df["Phase"].values
x = range(len(phases))

# Values for each metric
write_tp = df["Write Throughput (ops/sec)"].values
read_lat = df["Read Latency (ms)"].values
compression = df["Compression (%)"].values

# Create grouped bar chart
bar_width = 0.25

plt.figure(figsize=(10,6))
plt.bar([i - bar_width for i in x], write_tp, width=bar_width, label="Write Throughput (ops/sec)")
plt.bar(x, read_lat, width=bar_width, label="Read Latency (ms)")
plt.bar([i + bar_width for i in x], compression, width=bar_width, label="Compression (%)")

# Labels and legend
plt.xticks(x, phases)
plt.xlabel("Phase")
plt.ylabel("Metric Value")
plt.title("Database Benchmark Comparison Across Phases")
plt.legend()

# Save and show
plt.tight_layout()
plt.savefig("benchmark_chart.png")
plt.show()