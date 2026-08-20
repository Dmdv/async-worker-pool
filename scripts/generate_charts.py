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

categories = ['Zig 0.16\n(Arena + Slabs)', 'C11 libawp\n(Zero-Copy)', 'Rust awp-rs\n(v0.3.0 FFI)']
throughputs = [3.49, 0.52, 0.53]
colors = ['#F7A41D', '#007ACC', '#DEA584']

bars = ax.bar(categories, throughputs, color=colors, width=0.42, edgecolor='#30363d', linewidth=1.5, zorder=3)

for idx, bar in enumerate(bars):
    yval = bar.get_height()
    if idx == 0:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 0.12, 
                f"{yval:.2f} M msg/s\n(6.7x FASTER)", 
                ha='center', va='bottom', fontsize=12, fontweight='bold', color='#F7A41D')
    else:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 0.12, 
                f"{yval:.2f} M msg/s", 
                ha='center', va='bottom', fontsize=11, fontweight='bold', color='#8b949e')

ax.set_ylabel('Throughput (Million Messages / Sec) [HIGHER IS BETTER]', fontsize=12, color='#c9d1d9', labelpad=10)
ax.set_title('Multi-Threaded Async Pool: Throughput Comparison (32 Workers, 1M Msgs)\nZig is 6.7x Faster than C11 and Rust', 
             fontsize=14, fontweight='bold', color='#f0f6fc', pad=18)
ax.set_ylim(0, 4.4)
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

spsc_categories = ['Zig 0.16 SpscRing\n(0 CAS, Cached Indices)', 'C11 SPSC Ring\n(Lock-Free Vyukov)', 'Zig Single-Ring\n(+ SIMD Stream)']
spsc_ops = [65.32, 62.50, 11.59]
spsc_colors = ['#238636', '#2f81f7', '#a371f7']

bars = ax.bar(spsc_categories, spsc_ops, color=spsc_colors, width=0.42, edgecolor='#30363d', linewidth=1.5, zorder=3)

for idx, bar in enumerate(bars):
    yval = bar.get_height()
    if idx == 0:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 1.5, 
                f"{yval:.2f} M ops/s\n({(1000.0/yval):.2f} ns/op) [WINNER]", 
                ha='center', va='bottom', fontsize=11, fontweight='bold', color='#3fb950')
    else:
        ax.text(bar.get_x() + bar.get_width()/2.0, yval + 1.5, 
                f"{yval:.2f} M ops/s\n({(1000.0/yval):.2f} ns/op)", 
                ha='center', va='bottom', fontsize=11, fontweight='bold', color='#c9d1d9')

ax.set_ylabel('Operations / Second (Millions) [HIGHER IS BETTER]', fontsize=12, color='#c9d1d9', labelpad=10)
ax.set_title('Pure Single-Ring Lock-Free Performance (1,000,000 Ops)\nZig SpscRing reaches 65.32M ops/s (15.31 ns)', 
             fontsize=14, fontweight='bold', color='#f0f6fc', pad=18)
ax.set_ylim(0, 80)
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

percentiles = ['Min (Floor)', 'p50 (Median)', 'p90', 'p99', 'Max']
zig_latencies = [18, 667, 45602, 4508518, 16089679]
c_latencies = [83, 3458, 11167, 1109791, 1671458]

x = np.arange(len(percentiles))

ax.plot(x, zig_latencies, marker='o', markersize=10, linewidth=3.0, color='#F7A41D', label='Zig 0.16 Engine (Arena + Slabs)', zorder=4)
ax.plot(x, c_latencies, marker='s', markersize=10, linewidth=3.0, color='#58a6ff', label='C11 Engine (libawp Zero-Copy)', zorder=4)

ax.set_yscale('log')
ax.set_xticks(x)
ax.set_xticklabels(percentiles, fontsize=12, color='#f0f6fc')
ax.set_ylabel('Latency in Nanoseconds (Log Scale) [LOWER IS BETTER]', fontsize=12, color='#c9d1d9', labelpad=10)
ax.set_title('End-to-End Latency Distribution: Zig vs C11 (1M Messages, 32 Workers)\n[ LOWER IS BETTER - Zig achieves 18 ns Min and 667 ns Median ]', 
             fontsize=14, fontweight='bold', color='#f0f6fc', pad=18)

# Annotations for Min & Median
ax.annotate('18 ns (Zig)\n[4.6x Lower Floor]', xy=(0, 18), xytext=(0.05, 3),
            arrowprops=dict(arrowstyle="->", color='#F7A41D', lw=1.8), fontsize=10.5, color='#F7A41D', fontweight='bold')
ax.annotate('83 ns (C11)', xy=(0, 83), xytext=(-0.35, 140),
            arrowprops=dict(arrowstyle="->", color='#58a6ff', lw=1.5), fontsize=10.5, color='#58a6ff', fontweight='bold')

ax.annotate('667 ns (Zig p50)\n[5.2x Faster Median]', xy=(1, 667), xytext=(0.65, 120),
            arrowprops=dict(arrowstyle="->", color='#F7A41D', lw=1.8), fontsize=10.5, color='#F7A41D', fontweight='bold')
ax.annotate('3,458 ns (C11 p50)', xy=(1, 3458), xytext=(1.12, 7000),
            arrowprops=dict(arrowstyle="->", color='#58a6ff', lw=1.5), fontsize=10.5, color='#58a6ff', fontweight='bold')

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

print("Clean updated charts generated successfully!")
