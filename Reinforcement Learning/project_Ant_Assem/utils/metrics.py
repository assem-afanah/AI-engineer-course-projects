import os
import numpy as np
import matplotlib.figure
from datetime import datetime


def rolling_mean(rewards: list[float], window: int) -> list[float]:
    """Compute rolling mean. Returns a list of the same length as rewards."""
    out = []
    for i in range(len(rewards)):
        sl = rewards[max(0, i - window + 1): i + 1]
        out.append(float(np.mean(sl)))
    return out


def save_plot(fig: matplotlib.figure.Figure,
              output_dir: str = "results") -> str:
    """Save the figure to a timestamped PNG and return the path."""
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"training_plot_{ts}.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    return path