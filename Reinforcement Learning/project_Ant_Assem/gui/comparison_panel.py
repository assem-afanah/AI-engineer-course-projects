import tkinter as tk
import queue
import numpy as np
import matplotlib.colors as mcolors
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from utils.metrics import rolling_mean, save_plot

BG0  = "#0E0E10"
BG1  = "#1C1C1E"
BG2  = "#28282C"
BG3  = "#38383C"
BORDER_DARK   = "#3A3A3C"
BORDER_MID    = "#48484A"
TEXT_PRIMARY   = "#FFFFFF"
TEXT_SECONDARY = "#E0E0E0"
TEXT_TERTIARY  = "#A0A0A0"
TEXT_DANGER    = "#FF453A"
TEXT_SUCCESS   = "#30D158"
FONT_SMALL   = ("Helvetica", 10)
FONT_MONO_SM = ("Courier New", 9)

COLOR_A2C = "#4CAF50"
COLOR_SAC = "#2196F3"
COLOR_TD3 = "#FF5722"
ALGO_COLORS = {"A2C": COLOR_A2C, "SAC": COLOR_SAC, "TD3": COLOR_TD3}


class ComparisonPanel(tk.Frame):
    def __init__(self, master, data_queue):
        super().__init__(master, bg=BG2)
        self.data_queue = data_queue

        self._rewards = {}
        self._lines_raw = {}
        self._lines_avg = {}
        self._solved_at = {}  # Track solved episode for each algorithm
        self._sweep_colors = {}  # Track colors for sweep runs
        self._color_mapping = {}  # Maps parameter configurations to unique colors

        # Create matplotlib figure and canvas with white background
        self.fig = Figure(figsize=(8, 5), facecolor="white")
        # Adjust subplot layout to provide more space for labels and legend
        self.fig.subplots_adjust(left=0.1, right=0.75, top=0.88, bottom=0.12, hspace=0.15)
        self.ax_raw, self.ax_avg = self.fig.subplots(2, 1, sharex=True,
                                                       gridspec_kw={"hspace": 0.08})

        # Add main figure title with black text
        self.fig.suptitle("Ant-v5 RL Training Comparison", color="black",
                         fontsize=14, fontweight='bold', y=0.98)

        for ax in (self.ax_raw, self.ax_avg):
            ax.set_facecolor("white")
            ax.tick_params(colors="black", labelsize=10, width=1, length=5)
            ax.spines[:].set_color("black")
            ax.grid(True, color="#E0E0E0", linewidth=0.8, linestyle="--", alpha=0.7)
            for spine in ax.spines.values():
                spine.set_linewidth(0.8)

        # Enhanced axis labels and titles with black text on white background
        self.ax_raw.set_ylabel("Episode Reward", color="black", fontsize=11, fontweight='bold')
        self.ax_raw.set_title("Episode Reward (raw + 10-ep moving average)",
                              color="black", fontsize=12, fontweight='bold', pad=8)
        self.ax_avg.set_ylabel("100-ep Moving Average", color="black", fontsize=11, fontweight='bold')
        self.ax_avg.set_xlabel("Episode", color="black", fontsize=11, fontweight='bold')

        for ax in (self.ax_raw, self.ax_avg):
            ax.axhline(y=6000, color="#DC3545", linewidth=1.0, linestyle="--", alpha=0.8,
                      label="Solve (6 000)", zorder=2)

        self.canvas = FigureCanvasTkAgg(self.fig, self)
        self.canvas.get_tk_widget().pack(side="top", fill="both", expand=True)

        self.toolbar = NavigationToolbar2Tk(self.canvas, self, pack_toolbar=False)
        self.toolbar.config(bg=BG1)
        self.toolbar.pack(side="bottom", fill="x")

        # Create dynamic legend that updates as runs are added
        self._update_legend()

    def _get_color_for_run(self, run_label):
        """Get or assign a color for a run label."""
        if run_label in self._sweep_colors:
            return self._sweep_colors[run_label]

        # Check if it's a regular algorithm
        if run_label in ALGO_COLORS:
            color = ALGO_COLORS[run_label]
            self._sweep_colors[run_label] = color
            return color

        # For sweep runs, use highly distinguishable color palette with guaranteed uniqueness per run
        if '_' in run_label:
            # Large palette of highly distinguishable colors (50 colors)
            distinct_colors = [
                '#FF0000', '#00FF00', '#0000FF', '#FFFF00', '#FF00FF', '#00FFFF', '#FFA500', '#800080',
                '#FFC0CB', '#A52A2A', '#808080', '#000000', '#FF6347', '#32CD32', '#4169E1', '#FFD700',
                '#DA70D6', '#40E0D0', '#FF8C00', '#9932CC', '#FFB6C1', '#8B4513', '#696969', '#2F4F4F',
                '#DC143C', '#228B22', '#1E90FF', '#F0E68C', '#DDA0DD', '#48D1CC', '#6A5ACD',
                '#F08080', '#BC8F8F', '#708090', '#8B0000', '#006400', '#00008B', '#B8860B', '#8B008B',
                '#5F9EA0', '#D2691E', '#9ACD32', '#4B0082', '#F4A460', '#DEB887', '#20B2AA',
                '#87CEEB', '#778899', '#B0C4DE', '#FFFFE0', '#98FB98', '#F5DEB3', '#FFE4E1', '#D3D3D3'
            ]

            # Use the full run label as the key for guaranteed uniqueness
            if run_label not in self._color_mapping:
                # Assign the next available color
                current_index = len(self._color_mapping)
                self._color_mapping[run_label] = distinct_colors[current_index % len(distinct_colors)]

            color = self._color_mapping[run_label]
            self._sweep_colors[run_label] = color
            return color

        # Fallback for any other cases
        fallback_colors = list(mcolors.TABLEAU_COLORS.values())
        color_index = len(self._sweep_colors) % len(fallback_colors)
        color = fallback_colors[color_index]
        self._sweep_colors[run_label] = color
        return color

    def _update_legend(self):
        """Update the legend with current runs."""
        legend_handles = []

        # Add solve line
        legend_handles.append(Line2D([0], [0], color="#DC3545", lw=1.0, linestyle="--", label="Solve"))

        # Add all current runs
        for run_label in sorted(self._rewards.keys()):
            color = self._get_color_for_run(run_label)
            legend_handles.append(Line2D([0], [0], color=color, lw=1.8, label=run_label))

        # Clear existing legend and create new one
        if hasattr(self, '_legend'):
            self._legend.remove()

        # Position legend to avoid covering axis labels and titles
        # Use multiple columns for many items to keep it compact
        num_items = len(legend_handles)
        if num_items <= 6:
            ncol = 1
            loc = "upper right"
            bbox_to_anchor = None
        elif num_items <= 12:
            ncol = 2
            loc = "upper right"
            bbox_to_anchor = (0.98, 0.98)
        else:
            ncol = 3
            loc = "upper right"
            bbox_to_anchor = (0.98, 0.98)

        self._legend = self.fig.legend(handles=legend_handles, loc=loc, ncol=ncol, fontsize=7,
                                       facecolor="white", edgecolor="black", labelcolor="black",
                                       framealpha=0.9, bbox_to_anchor=bbox_to_anchor)

        # Re-apply axis labels to ensure they remain visible
        self._refresh_axis_labels()
        self.canvas.draw_idle()

    def _refresh_axis_labels(self):
        """Re-apply all axis labels and titles to ensure visibility."""
        # Enhanced axis labels and titles with black text on white background
        self.ax_raw.set_ylabel("Episode Reward", color="black", fontsize=11, fontweight='bold')
        self.ax_raw.set_title("Episode Reward (raw + 10-ep moving average)",
                              color="black", fontsize=12, fontweight='bold', pad=8)
        self.ax_avg.set_ylabel("100-ep Moving Average", color="black", fontsize=11, fontweight='bold')
        self.ax_avg.set_xlabel("Episode", color="black", fontsize=11, fontweight='bold')

        # Ensure grid lines remain visible
        for ax in (self.ax_raw, self.ax_avg):
            ax.grid(True, color="#E0E0E0", linewidth=0.8, linestyle="--", alpha=0.7)

        # Re-apply main figure title
        self.fig.suptitle("Ant-v5 RL Training Comparison", color="black",
                         fontsize=14, fontweight='bold', y=0.98)

    def on_data(self, item: dict):
        if "_done" in item or "_error" in item:
            return

        algo = item["algo"]
        reward = item["reward"]
        solved_at = item.get("solved_at")

        if algo not in self._rewards:
            self._rewards[algo] = []

        self._rewards[algo].append(reward)

        # Store solved_at if it's not None and not already stored
        if solved_at is not None and algo not in self._solved_at:
            self._solved_at[algo] = solved_at
        rewards = self._rewards[algo]
        x = list(range(1, len(rewards) + 1))

        rolling_10 = rolling_mean(rewards, 10)
        rolling_100 = rolling_mean(rewards, 100)
        x = list(range(1, len(rewards) + 1))

        color = self._get_color_for_run(algo)

        if algo not in self._lines_raw:
            line_faint, = self.ax_raw.plot(x, rewards, color=color, alpha=0.25, linewidth=0.7)
            line_mean, = self.ax_raw.plot(x, rolling_10, color=color, alpha=0.95, linewidth=1.6)
            line_avg, = self.ax_avg.plot(x, rolling_100, color=color, alpha=0.95, linewidth=2.0)
            self._lines_raw[algo] = (line_faint, line_mean)
            self._lines_avg[algo] = line_avg
            # Update legend when new run is added
            self._update_legend()
        else:
            line_faint, line_mean = self._lines_raw[algo]
            line_avg = self._lines_avg[algo]
            line_faint.set_data(x, rewards)
            line_mean.set_data(x, rolling_10)
            line_avg.set_data(x, rolling_100)

        self.ax_raw.relim()
        self.ax_raw.autoscale_view()
        self.ax_avg.relim()
        self.ax_avg.autoscale_view()
        self.canvas.draw_idle()

    def clear_all(self):
        self._rewards.clear()
        self._lines_raw.clear()
        self._lines_avg.clear()
        self._solved_at.clear()
        self._sweep_colors.clear()
        self._color_mapping.clear()
        self.ax_raw.clear()
        self.ax_avg.clear()
        self.ax_raw.set_facecolor("white")
        self.ax_avg.set_facecolor("white")
        for ax in (self.ax_raw, self.ax_avg):
            ax.tick_params(colors="black", labelsize=10, width=1, length=5)
            ax.spines[:].set_color("black")
            ax.grid(True, color="#E0E0E0", linewidth=0.8, linestyle="--", alpha=0.7)
            for spine in ax.spines.values():
                spine.set_linewidth(0.8)
            ax.axhline(y=6000, color="#DC3545", linewidth=1.0, linestyle="--", alpha=0.8, zorder=2)
        self.canvas.draw_idle()

    def save_plot(self) -> str:
        return save_plot(self.fig)

    def get_score_data(self) -> list[dict]:
        result = []
        for algo, rewards in self._rewards.items():
            if len(rewards) >= 100:
                last_100 = rewards[-100:]
                result.append({
                    "run": algo,
                    "mean_100": float(np.mean(last_100)),
                    "std_100": float(np.std(last_100)),
                    "max": float(max(rewards)),
                    "solved_at": self._solved_at.get(algo),
                })
        return result