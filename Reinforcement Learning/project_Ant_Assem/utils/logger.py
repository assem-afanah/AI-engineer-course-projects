import csv, os
from datetime import datetime


class RunLogger:
    """Writes per-episode metrics to a timestamped CSV file."""

    FIELDNAMES = [
        "episode", "total_reward", "steps",
        "rolling_mean_100", "max_reward", "solved",
    ]

    def __init__(self, algo: str, run_id: str | None = None):
        ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
        rid     = f"_{run_id}" if run_id else ""
        os.makedirs("results", exist_ok=True)
        self._path = os.path.join("results", f"{algo}{rid}_{ts}.csv")

        with open(self._path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.FIELDNAMES)
            writer.writeheader()

    def log(
        self,
        episode: int,
        total_reward: float,
        steps: int,
        rolling_mean_100: float,
        max_reward: float,
        solved: bool,
    ) -> None:
        with open(self._path, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=self.FIELDNAMES).writerow({
                "episode"         : episode,
                "total_reward"    : round(total_reward, 4),
                "steps"           : steps,
                "rolling_mean_100": round(rolling_mean_100, 4),
                "max_reward"      : round(max_reward, 4),
                "solved"          : int(solved),
            })