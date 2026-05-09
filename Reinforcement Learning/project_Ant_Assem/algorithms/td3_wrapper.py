from stable_baselines3 import TD3
from stable_baselines3.common.noise import NormalActionNoise
import numpy as np
from .base_wrapper import BaseWrapper


class TD3Wrapper(BaseWrapper):
    model_class = TD3

    def build(self, env, params: dict, device: str) -> None:
        self.env = env
        net_arch = [params["hidden_size"]] * params["n_hidden_layers"]
        policy_kwargs = dict(net_arch=net_arch)

        action_dim = env.action_space.shape[0]   # 8
        action_noise = NormalActionNoise(
            mean  = np.zeros(action_dim),
            sigma = params["exploration_noise"] * np.ones(action_dim),
        )

        self.model = TD3(
            policy          = "MlpPolicy",
            env             = env,
            learning_rate   = params["learning_rate"],
            buffer_size     = params["buffer_size"],
            learning_starts = params["learning_starts"],
            batch_size      = params["batch_size"],
            tau             = params["tau"],
            gamma           = params["gamma"],
            train_freq      = params["train_freq"],
            gradient_steps  = params["gradient_steps"],
            action_noise    = action_noise,
            policy_delay    = params["policy_delay"],
            target_policy_noise = params["target_noise"],
            target_noise_clip   = params["noise_clip"],
            policy_kwargs   = policy_kwargs,
            device          = device,
            verbose         = 0,
        )