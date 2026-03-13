import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

configs    = ["Local (1 worker)", "Distributed (2 workers)"]
runtimes   = [17.9, 26.19]
throughput = [10_000_000 / 17.9, 10_000_000 / 26.19]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))

ax1.bar(configs, runtimes, color=["#2ecc71", "#e74c3c"])
ax1.set_ylabel("Runtime (seconds)")
ax1.set_title("Runtime: Local vs Distributed")
for i, v in enumerate(runtimes):
    ax1.text(i, v + 0.3, f"{v}s", ha="center", fontweight="bold")

ax2.bar(configs, [round(t) for t in throughput], color=["#2ecc71", "#e74c3c"])
ax2.set_ylabel("Rows / second")
ax2.set_title("Throughput: Local vs Distributed")
for i, v in enumerate(throughput):
    ax2.text(i, v + 1000, f"{v:,.0f}", ha="center", fontweight="bold")

plt.suptitle("PySpark Feature Engineering - 10M Rows", fontweight="bold")
plt.tight_layout()
plt.savefig("performance_charts.png", dpi=150)
print("Saved performance_charts.png")