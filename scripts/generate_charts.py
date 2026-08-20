import matplotlib.pyplot as plt
import numpy as np
import os
import shutil

# Set publication dark-theme style
plt.style.use('dark_background')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Helvetica Neue', 'Arial', 'sans-serif']

c_base_dir = "/Users/dima/c_lang/async-worker-pool/docs/images"
zig_base_dir = "/Users/dima/c_lang/async-worker-pool_zig/docs/images"

os.makedirs(c_base_dir, exist_ok=True)
os.makedirs(zig_base_dir, exist_ok=True)

# -------------------------------------------------------------
# Chart 1: Multi-Threaded Pool Throughput Comparison (M msg/sec)
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10.5, 6.2), dpi=300)
fig.patch.set_facecolor('#0d1117')
ax.set_facecolor('#161b22')

categories = ['Zig 0.16\n(Phase 1 Prefaulted)', 'C11 libawp\n(Zero-Copy)', 'Rust awp-rs\n(v0.3.0 FFI)']
throughputs = [0.19, 0.52, 0.53]
colors = ['#F7A41D', '#007ACC', '#DEA584']

bars = ax.bar(categories, throughputs, color=colors, width=0.42, edgecolor='#30363d', linewidth=1.5, zorder=3)

for idx, bar in enumerate(bars):
    yval = bar.get_height()
    if idx == 0:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 0.02, 
                f"{yval:.2f} M msg/s\n(Low-Jitter Pinned)", 
                ha='center', va='bottom', fontsize=11, fontweight='bold', color='#F7A41D')
    else:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 0.02, 
                f"{yval:.2f} M msg/s", 
                ha='center', va='bottom', fontsize=11, fontweight='bold', color='#8b949e')

ax.set_ylabel('Throughput (Million Messages / Sec) [HIGHER IS BETTER]', fontsize=12, color='#c9d1d9', labelpad=10)
ax.set_title('Multi-Threaded Async Pool: Throughput Comparison (1M Messages)\nCalibrated Hardware Benchmarks', 
             fontsize=14, fontweight='bold', color='#f0f6fc', pad=18)
ax.set_ylim(0, 0.75)
ax.grid(axis='y', linestyle='--', alpha=0.25, color='#8b949e', zorder=0)
ax.tick_params(colors='#8b949e', labelsize=11)
for spine in ax.spines.values():
    spine.set_color('#30363d')

plt.tight_layout()
chart1_path = os.path.join(c_base_dir, "benchmark_throughput.png")
plt.savefig(chart1_path, facecolor=fig.get_facecolor(), bbox_inches='tight')
plt.close()

# -------------------------------------------------------------
# Chart 2: SPSC Raw Ring Throughput Comparison (M ops/sec)
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10.5, 6.2), dpi=300)
fig.patch.set_facecolor('#0d1117')
ax.set_facecolor('#161b22')

spsc_categories = ['Zig 0.16 SpscRing\n(0 CAS, Pure Pointer)', 'C11 SPSC Ring\n(Lock-Free Vyukov)', 'Zig Concurrent\n(4KB Frame Structs)']
spsc_ops = [85.18, 62.50, 3.52]
spsc_colors = ['#238636', '#2f81f7', '#a371f7']

bars = ax.bar(spsc_categories, spsc_ops, color=spsc_colors, width=0.42, edgecolor='#30363d', linewidth=1.5, zorder=3)

for idx, bar in enumerate(bars):
    yval = bar.get_height()
    if idx == 0:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 1.8, 
                f"{yval:.2f} M ops/s\n({(1000.0/yval):.2f} ns/op) [WINNER]", 
                ha='center', va='bottom', fontsize=11, fontweight='bold', color='#3fb950')
    else:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 1.8, 
                f"{yval:.2f} M ops/s\n({(1000.0/yval):.2f} ns/op)", 
                ha='center', va='bottom', fontsize=11, fontweight='bold', color='#c9d1d9')

ax.set_ylabel('Operations / Second (Millions) [HIGHER IS BETTER]', fontsize=12, color='#c9d1d9', labelpad=10)
ax.set_title('Pure Single-Ring Lock-Free Performance (1,000,000 Ops)\nZig SpscRing achieves 85.18M ops/s (11.74 ns/op) — 36.3% Faster than C11', 
             fontsize=14, fontweight='bold', color='#f0f6fc', pad=18)
ax.set_ylim(0, 105)
ax.grid(axis='y', linestyle='--', alpha=0.25, color='#8b949e', zorder=0)
ax.tick_params(colors='#8b949e', labelsize=11)
for spine in ax.spines.values():
    spine.set_color('#30363d')

plt.tight_layout()
chart2_path = os.path.join(c_base_dir, "benchmark_spsc_comparison.png")
plt.savefig(chart2_path, facecolor=fig.get_facecolor(), bbox_inches='tight')
plt.close()

# -------------------------------------------------------------
# Chart 3: Tail Latencies Percentile Curve (Log Scale, Nanoseconds)
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(11.5, 6.8), dpi=300)
fig.patch.set_facecolor('#0d1117')
ax.set_facecolor('#161b22')

percentiles = ['Min (Floor)', 'p50 (Median)', 'p90', 'p99 (Tail Jitter)', 'p99.9', 'Max']
zig_latencies = [750, 8709, 24333, 45125, 300375, 1902500]
c_latencies = [83, 3458, 11167, 1109791, 1270000, 1671458]

x = np.arange(len(percentiles))

ax.plot(x, zig_latencies, marker='o', markersize=10, linewidth=3.0, color='#F7A41D', label='Zig 0.16 Engine (Phase 1 Hardware Hardening)', zorder=4)
ax.plot(x, c_latencies, marker='s', markersize=10, linewidth=3.0, color='#58a6ff', label='C11 Engine (libawp Zero-Copy)', zorder=4)

ax.set_yscale('log')
ax.set_xticks(x)
ax.set_xticklabels(percentiles, fontsize=11, color='#f0f6fc')
ax.set_ylabel('Latency in Nanoseconds (Log Scale) [LOWER IS BETTER]', fontsize=12, color='#c9d1d9', labelpad=10)
ax.set_title('End-to-End Tail Latency Distribution: Zig vs C11 (1M Messages)\n[ Zig achieves 45.12 µs p99 Tail Jitter vs 1.11 ms in C11 — 24.6x Lower Jitter ]', 
             fontsize=13.5, fontweight='bold', color='#f0f6fc', pad=18)

# Annotations for Tail Latencies
ax.annotate('45.12 µs (Zig p99)\n[24.6x Lower Jitter!]', xy=(3, 45125), xytext=(2.3, 12000),
            arrowprops=dict(arrowstyle="->", color='#F7A41D', lw=1.8), fontsize=10.5, color='#F7A41D', fontweight='bold')
ax.annotate('1.11 ms (C11 p99)', xy=(3, 1109791), xytext=(2.6, 2500000),
            arrowprops=dict(arrowstyle="->", color='#58a6ff', lw=1.5), fontsize=10.5, color='#58a6ff', fontweight='bold')

ax.annotate('300 µs (Zig p99.9)\n[4.2x Lower]', xy=(4, 300375), xytext=(3.4, 80000),
            arrowprops=dict(arrowstyle="->", color='#F7A41D', lw=1.8), fontsize=10.5, color='#F7A41D', fontweight='bold')

ax.grid(True, linestyle='--', alpha=0.2, color='#8b949e', which='both', zorder=0)
ax.tick_params(colors='#8b949e', labelsize=11)
for spine in ax.spines.values():
    spine.set_color('#30363d')

legend = ax.legend(frameon=True, facecolor='#21262d', edgecolor='#30363d', fontsize=11, loc='upper left')
for text in legend.get_texts():
    text.set_color('#f0f6fc')

plt.tight_layout()
chart3_path = os.path.join(c_base_dir, "benchmark_tail_latencies.png")
plt.savefig(chart3_path, facecolor=fig.get_facecolor(), bbox_inches='tight')
plt.close()

# Copy all charts to Zig repo
for fname in ["benchmark_throughput.png", "benchmark_spsc_comparison.png", "benchmark_tail_latencies.png"]:
    shutil.copyfile(os.path.join(c_base_dir, fname), os.path.join(zig_base_dir, fname))

print("Clean updated charts generated and copied successfully to both repositories!")
