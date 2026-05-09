import gymnasium as gym
from stable_baselines3 import A2C
from stable_baselines3.common.policies import ActorCriticPolicy
from stable_baselines3.common.env_util import make_vec_env
from .base_wrapper import BaseWrapper


class A2CWrapper(BaseWrapper):
    model_class = A2C

    def build(self, env, params: dict, device: str) -> None:
        n_envs = params.get("n_envs", 1)
        if n_envs > 1:
            self.env = make_vec_env(lambda: gym.make("Ant-v5",
                                                     ctrl_cost_weight=0.5,
                                                     contact_cost_weight=5e-4,
                                                     healthy_reward=1.0,
                                                     terminate_when_unhealthy=True,
                                                     render_mode=None), n_envs=n_envs)
        else:
            self.env = env
        net_arch = [params["hidden_size"]] * params["n_hidden_layers"]
        policy_kwargs = dict(net_arch=net_arch)

        self.model = A2C(
            policy              = "MlpPolicy",
            env                 = self.env,
            learning_rate       = params["learning_rate"],
            n_steps             = params["n_steps"],
            gamma               = params["gamma"],
            gae_lambda          = params["gae_lambda"],
            ent_coef            = params["ent_coef"],
            vf_coef             = params["vf_coef"],
            max_grad_norm       = params["max_grad_norm"],
            normalize_advantage = True,
            use_sde             = params["use_sde"],
            sde_sample_freq     = params["sde_sample_freq"],
            policy_kwargs       = policy_kwargs,
            device              = device,
            verbose             = 0,
        )