import queue
import threading
import numpy as np
from collections import deque
from stable_baselines3.common.callbacks import BaseCallback

SOLVE_THRESHOLD = 6_000.0
SOLVE_WINDOW    = 100


class EpisodeCallback(BaseCallback):
    """
    Called by SB3 after every env step.
    Detects episode ends, computes metrics, and pushes them onto data_queue.
    Also monitors stop_event and terminates training when it is set.
    """

    def __init__(
        self,
        algo_label: str,
        data_queue: queue.Queue,
        stop_event: threading.Event,
        logger,          # utils.logger.RunLogger instance
        max_episodes: int = None,  # Target number of episodes to train
        total_eps: int = None,     # Estimated total episodes for progress display
        verbose: int = 0,
    ):
        super().__init__(verbose)
        self.algo_label     = algo_label
        self.data_queue     = data_queue
        self.stop_event     = stop_event
        self.run_logger     = logger
        self.max_episodes   = max_episodes
        self.total_eps      = total_eps  # Target episode count

        self._ep_reward     = 0.0
        self._ep_steps      = 0
        self._ep_count      = 0
        self._max_reward    = -np.inf
        self._reward_window = deque(maxlen=SOLVE_WINDOW)
        self._solved_at     = None

    # ── SB3 callback hooks ────────────────────────────────────────────────────

    def _on_step(self) -> bool:
        """Called after every environment step. Return False to abort training."""
        # Abort if the user pressed Stop
        if self.stop_event.is_set():
            return False

        # Check if we've reached the target number of episodes
        if self.max_episodes and self._ep_count >= self.max_episodes:
            return False  # Stop training

        reward = self.locals["rewards"][0]
        done   = self.locals["dones"][0]

        self._ep_reward += float(reward)
        self._ep_steps  += 1

        if done:
            self._ep_count += 1
            ep_reward = self._ep_reward

            # Rolling statistics
            self._reward_window.append(ep_reward)
            rolling_100 = float(np.mean(self._reward_window))
            self._max_reward = max(self._max_reward, ep_reward)

            solved = (
                len(self._reward_window) == SOLVE_WINDOW
                and rolling_100 >= SOLVE_THRESHOLD
                and self._solved_at is None
            )
            if solved:
                self._solved_at = self._ep_count

            # Use provided total_eps, or max_episodes, or estimate from timesteps
            if self.total_eps:
                total_eps = self.total_eps
            elif self.max_episodes:
                total_eps = self.max_episodes
            else:
                total_eps = self.model._total_timesteps // 1000  # rough estimate

            # Push metric dict for GUI
            self.data_queue.put({
                "algo"       : self.algo_label,
                "episode"    : self._ep_count,
                "total_eps"  : total_eps,
                "reward"     : ep_reward,
                "rolling_100": rolling_100,
                "max_reward" : self._max_reward,
                "solved_at"  : self._solved_at,
                "steps"      : self.num_timesteps,
            })

            # Log to CSV
            self.run_logger.log(
                episode      = self._ep_count,
                total_reward = ep_reward,
                steps        = self._ep_steps,
                rolling_mean_100 = rolling_100,
                max_reward   = self._max_reward,
                solved       = solved,
            )

            # Reset episode accumulators
            self._ep_reward = 0.0
            self._ep_steps  = 0

        return True   # continue training