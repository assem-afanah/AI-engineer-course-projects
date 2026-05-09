from stable_baselines3 import SAC
from .base_wrapper import BaseWrapper


class SACWrapper(BaseWrapper):
    model_class = SAC

    def build(self, env, params: dict, device: str) -> None:
        self.env = env
        net_arch = [params["hidden_size"]] * params["n_hidden_layers"]
        policy_kwargs = dict(net_arch=net_arch)

        self.model = SAC(
            policy           = "MlpPolicy",
            env              = env,
            learning_rate    = params["learning_rate"],
            buffer_size      = params["buffer_size"],
            learning_starts  = params["learning_starts"],
            batch_size       = params["batch_size"],
            tau              = params["tau"],
            gamma            = params["gamma"],
            train_freq       = params["train_freq"],
            gradient_steps   = params["gradient_steps"],
            ent_coef         = params["ent_coef"],       # "auto" or float
            target_entropy   = params["target_entropy"], # "auto" or float
            policy_kwargs    = policy_kwargs,
            device           = device,
            verbose          = 0,
        )