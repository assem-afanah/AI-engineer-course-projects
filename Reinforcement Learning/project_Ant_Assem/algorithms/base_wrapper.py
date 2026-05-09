import threading
import numpy as np
from abc import ABC, abstractmethod


class BaseWrapper(ABC):
    """Abstract base for all SB3 algorithm wrappers."""
    model_class = None

    def __init__(self):
        self.model = None          # SB3 model — set by subclass __init__
        self.env   = None          # Gymnasium env — set by subclass __init__
        self._lock = threading.Lock()   # protects model for predict() during training

    @abstractmethod
    def build(self, env, params: dict, device: str) -> None:
        """Construct self.model from params. Called once before training starts."""

    def predict(self, obs: np.ndarray, deterministic: bool = True) -> np.ndarray:
        """Thread-safe action prediction for the animation popup."""
        with self._lock:
            action, _ = self.model.predict(obs, deterministic=deterministic)
        return action

    def get_device(self):
        return self.model.device

    def get_model(self):
        return self.model