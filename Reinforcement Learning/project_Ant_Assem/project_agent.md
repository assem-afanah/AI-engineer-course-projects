# Project Agent — Ant-v5 RL Workbench (Stable-Baselines3 Edition)

This file is the **single authoritative specification** for an AI coding agent that must generate
every Python source file required to run the Ant-v5 RL Workbench.  The workbench trains and
compares three algorithms — **A2C**, **SAC**, and **TD3** — using **Stable-Baselines3 (SB3)** on
the `Ant-v5` MuJoCo/Gymnasium environment.  A Tkinter GUI provides full hyperparameter control,
live side-by-side reward charts, per-algorithm progress bars, a live animation popup, a score
comparison table, and **per-algorithm model save/load** so any trained policy can be persisted
and resumed later with different hyperparameters.

> **Agent instruction**: Read every section of this file before generating any code.  Each section
> contains implementation requirements that affect other sections.  Do **not** paraphrase or
> abbreviate any specification — implement every detail exactly as written.

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Dependencies & Setup](#2-dependencies--setup)
3. [Environment Overview](#3-environment-overview)
4. [SB3 Agent Wrappers](#4-sb3-agent-wrappers)
   - 4.1 [BaseWrapper](#41-basewrapper)
   - 4.2 [A2CWrapper](#42-a2cwrapper)
   - 4.3 [SACWrapper](#43-sacwrapper)
   - 4.4 [TD3Wrapper](#44-td3wrapper)
5. [Hyperparameter Reference](#5-hyperparameter-reference)
   - 5.1 [A2C](#51-a2c-hyperparameters)
   - 5.2 [SAC](#52-sac-hyperparameters)
   - 5.3 [TD3](#53-td3-hyperparameters)
6. [Training Thread Protocol](#6-training-thread-protocol)
7. [Model Persistence — Save & Load](#7-model-persistence--save--load)
   - 7.1 [Save](#71-save)
   - 7.2 [Load & Resume](#72-load--resume)
   - 7.3 [File Naming Convention](#73-file-naming-convention)
8. [GUI Architecture — Detailed Specification](#8-gui-architecture--detailed-specification)
   - 8.1 [Window & Root Settings](#81-window--root-settings)
   - 8.2 [Colour Palette & Fonts](#82-colour-palette--fonts)
   - 8.3 [Overall Layout](#83-overall-layout)
   - 8.4 [Header Bar](#84-header-bar)
   - 8.5 [HyperparamPanel — Left Column](#85-hyperparampanel--left-column)
   - 8.6 [ComparisonPanel — Right Column](#86-comparisoneanel--right-column)
   - 8.7 [TrainingPanel — Bottom Bar](#87-trainingpanel--bottom-bar)
   - 8.8 [AnimationWindow — Popup](#88-animationwindow--popup)
   - 8.9 [ScoreTable Popup](#89-scoretable-popup)
   - 8.10 [Thread-Safe Data Flow](#810-thread-safe-data-flow)
9. [Entrypoint](#9-entrypoint)
10. [Utils](#10-utils)
    - 10.1 [Logger](#101-logger)
    - 10.2 [Metrics](#102-metrics)
11. [Validation Rules](#11-validation-rules)
12. [Solve Criterion](#12-solve-criterion)

---

## 1. Project Structure

Generate **exactly** the following files.  Do not rename, merge, or omit any file.

```
ant_workbench/
│
├── project_agent.py            # § 9  — entrypoint; run this file
├── ant_ui.py                   # § 8  — top-level AntUI frame
│
├── __init__.py                 # empty
│
├── algorithms/
│   ├── __init__.py             # empty
│   ├── base_wrapper.py         # § 4.1
│   ├── a2c_wrapper.py          # § 4.2
│   ├── sac_wrapper.py          # § 4.3
│   ├── td3_wrapper.py          # § 4.4
│   └── episode_callback.py     # § 6
│
├── gui/
│   ├── __init__.py             # empty
│   ├── hyperparameter_panel.py # § 8.5
│   ├── comparison_panel.py     # § 8.6
│   ├── training_panel.py       # § 8.7
│   └── animation_window.py     # § 8.8
│
└── utils/
    ├── __init__.py             # empty
    ├── device.py               # § 2
    ├── logger.py               # § 10.1
    └── metrics.py              # § 10.2
```

Output directories (created automatically at runtime):
- `ant_workbench/results/`        — CSV logs and saved plots
- `ant_workbench/results/models/` — saved model `.zip` files and replay buffers

---

## 2. Dependencies & Setup

### Install

```bash
pip install "stable-baselines3[extra]>=2.3.0" \
            "gymnasium[mujoco]>=1.0.0" \
            torch \
            numpy \
            matplotlib \
            pandas \
            mujoco \
            Pillow
```

> **Gymnasium ≥ 1.0.0 is required for `Ant-v5`.**  Earlier versions only provide `Ant-v4`.

### Version requirements

| Package              | Minimum version | Note                                      |
|----------------------|-----------------|-------------------------------------------|
| stable-baselines3    | 2.3.0           |                                           |
| gymnasium            | 1.0.0           | Required for Ant-v5                       |
| torch                | 2.0.0           |                                           |
| mujoco               | 3.1.0           | Bundled with gymnasium[mujoco] ≥ 1.0      |
| matplotlib           | 3.7.0           |                                           |
| numpy                | 1.24.0          |                                           |
| Pillow               | 9.0.0           | Required for AnimationWindow              |
| Python               | 3.9             |                                           |

### Device detection helper

```python
# utils/device.py
import torch

def get_device(use_gpu: bool) -> str:
    """Return 'cuda' if use_gpu and CUDA is available, else 'cpu'."""
    if use_gpu and torch.cuda.is_available():
        return "cuda"
    return "cpu"
```

---

## 3. Environment Overview

### Ant-v5 vs Ant-v4 — key differences

| Property                          | Ant-v4                      | Ant-v5                                               |
|-----------------------------------|-----------------------------|------------------------------------------------------|
| Gymnasium version required        | ≥ 0.26                      | ≥ 1.0.0                                              |
| Default obs\_dim                  | 27                          | **105** (contact forces included by default)         |
| action\_dim                       | 8                           | 8 (unchanged)                                        |
| `include_cfrc_ext_in_observation` | `False` (excluded)          | `True` (included — adds 78 contact-force dims)       |
| `use_contact_forces`              | separate parameter          | Merged into `include_cfrc_ext_in_observation`        |
| Reward shaping                    | velocity + healthy − costs  | Same structure; healthy\_reward default unchanged    |
| `xml_file` parameter              | Not available               | Available — can substitute a custom ant model        |
| `forward_reward_weight`           | Implicit 1.0                | Explicit parameter (default 1.0)                     |

### Environment properties

| Property              | Value                                                                                                       |
|-----------------------|-------------------------------------------------------------------------------------------------------------|
| Gym ID                | `Ant-v5`                                                                                                    |
| Action space          | `Box(-1, +1, (8,), float32)` — 8 joint torques (unchanged from v4)                                         |
| Observation space     | `Box(-∞, +∞, (105,), float64)` — joint pos/vel (27) + contact forces (78) — **read dynamically from env**  |
| obs\_dim              | **105** (default config); always read as `env.observation_space.shape[0]` in code — never hardcode 105      |
| action\_dim           | **8** — always read as `env.action_space.shape[0]`                                                          |
| Reward per step       | `forward_reward_weight × velocity` + `healthy_reward` − `ctrl_cost` − `contact_cost`                       |
| Healthy bonus         | +1.0 per step the ant's z position is in `[0.2, 1.0]`                                                      |
| Solve criterion       | Mean episode reward ≥ 6 000 over the last 100 consecutive episodes                                          |
| Max episode steps     | 1 000 (Gymnasium `TimeLimit` wrapper)                                                                       |
| Termination           | z outside `[0.2, 1.0]` when `terminate_when_unhealthy=True`, or max steps exceeded                          |
| Render modes          | `"human"`, `"rgb_array"`, `None`                                                                            |

### Environment creation helper

```python
# Used in training threads and the animation window

import gymnasium as gym

def make_single_env(render_mode=None, seed: int = 0) -> gym.Env:
    """Create a single headless (or rgb_array) Ant-v5 environment."""
    env = gym.make(
        "Ant-v5",
        ctrl_cost_weight                = 0.5,
        contact_cost_weight             = 5e-4,
        healthy_reward                  = 1.0,
        terminate_when_unhealthy        = True,
        include_cfrc_ext_in_observation = True,   # keeps obs_dim = 105
        forward_reward_weight           = 1.0,
        render_mode                     = render_mode,
    )
    env.reset(seed=seed)
    return env
```

> **Obs-dim safety rule**: every file that needs `obs_dim` or `action_dim` must read them
> from the live environment: `obs_dim = env.observation_space.shape[0]` and
> `action_dim = env.action_space.shape[0]`.  Never hardcode 105 or 8 anywhere in the source.

---

## 4. SB3 Agent Wrappers

All agents are thin wrappers around the corresponding SB3 class.  They must:

- Instantiate the SB3 model with the correct `policy`, `env`, and hyperparameters.
- Accept a pre-built SB3-compatible Gymnasium environment (single env or `VecEnv`).
- Expose `predict(obs, deterministic=True) -> np.ndarray` for the animation popup.
- Expose `save(path: str) -> str` — saves model and (for SAC/TD3) the replay buffer.
- Expose `load(path: str, env, params: dict, device: str)` — restores model from disk
  so training can resume with `reset_num_timesteps=False`.

> **Important SB3 note**: SB3's `model.learn(total_timesteps=N)` is a blocking call that drives
> training by total env steps, not episodes.  Episode-level GUI updates are delivered via the
> `EpisodeCallback` in § 6, which is passed to `model.learn()` as the `callback` argument.

---

### 4.1 BaseWrapper

File: `algorithms/base_wrapper.py`

```python
import os
import threading
import numpy as np
from abc import ABC, abstractmethod


class BaseWrapper(ABC):
    """Abstract base for all SB3 algorithm wrappers."""

    ALGO_CLASS = None     # set by each subclass: e.g. A2C, SAC, TD3

    def __init__(self):
        self.model = None           # SB3 model — set by build() or load()
        self.env   = None           # env passed to build()
        self._lock = threading.Lock()

    # ── Abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    def build(self, env, params: dict, device: str) -> None:
        """Construct self.model from params. Called before training starts."""

    # ── Prediction (thread-safe) ──────────────────────────────────────────────

    def predict(self, obs: np.ndarray, deterministic: bool = True) -> np.ndarray:
        """Thread-safe action prediction for the animation popup."""
        with self._lock:
            action, _ = self.model.predict(obs, deterministic=deterministic)
        return action

    # ── Save ─────────────────────────────────────────────────────────────────

    def save(self, path: str) -> str:
        """
        Save the SB3 model to `path` (without extension — SB3 adds .zip).
        Subclasses that have a replay buffer should override this and call
        super().save(path) first, then save the replay buffer alongside.

        Returns the full path of the saved .zip file.
        """
        if self.model is None:
            raise RuntimeError("No model to save — call build() first.")
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        self.model.save(path)
        return path + ".zip"

    # ── Load & resume ─────────────────────────────────────────────────────────

    def load(self, path: str, env, params: dict, device: str) -> None:
        """
        Load a previously saved model from `path` (.zip, with or without extension).
        Sets self.model so that a subsequent model.learn(..., reset_num_timesteps=False)
        resumes training from where it left off.

        Subclasses with replay buffers should override this and call super().load()
        first, then attempt to load the replay buffer from path + "_replay_buffer.pkl".
        """
        zip_path = path if path.endswith(".zip") else path + ".zip"
        if not os.path.isfile(zip_path):
            raise FileNotFoundError(f"Model file not found: {zip_path}")

        self.env = env
        self.model = self.ALGO_CLASS.load(
            zip_path,
            env    = env,
            device = device,
        )

    def get_model(self):
        return self.model
```

---

### 4.2 A2CWrapper

File: `algorithms/a2c_wrapper.py`

```python
from stable_baselines3 import A2C
from stable_baselines3.common.env_util import make_vec_env
from stable_baselines3.common.vec_env import SubprocVecEnv
from .base_wrapper import BaseWrapper


class A2CWrapper(BaseWrapper):
    """
    Wraps SB3's A2C for Ant-v5.

    A2C is an on-policy algorithm that benefits greatly from collecting
    experience across multiple parallel environments simultaneously.
    n_envs=8 is required to gather enough diverse transitions per update
    for stable learning on the high-dimensional Ant-v5 task.
    """

    ALGO_CLASS = A2C

    def build(self, env, params: dict, device: str) -> None:
        """
        `env` passed here is ignored — A2CWrapper always creates its own
        SubprocVecEnv with n_envs parallel Ant-v5 instances.

        This is intentional: the training thread must pass None (or any
        placeholder) as the env argument for A2C; the VecEnv is built here.
        """
        n_envs    = params["n_envs"]           # default 8
        vec_env   = make_vec_env(
            "Ant-v5",
            n_envs      = n_envs,
            vec_env_cls = SubprocVecEnv,
            env_kwargs  = dict(
                ctrl_cost_weight                = 0.5,
                contact_cost_weight             = 5e-4,
                healthy_reward                  = 1.0,
                terminate_when_unhealthy        = True,
                include_cfrc_ext_in_observation = True,
                forward_reward_weight           = 1.0,
            ),
        )
        self.env = vec_env

        net_arch      = [params["hidden_size"]] * params["n_hidden_layers"]
        policy_kwargs = dict(net_arch=net_arch)

        self.model = A2C(
            policy              = "MlpPolicy",
            env                 = vec_env,
            learning_rate       = params["learning_rate"],
            n_steps             = params["n_steps"],
            gamma               = params["gamma"],
            gae_lambda          = params["gae_lambda"],
            ent_coef            = params["ent_coef"],
            vf_coef             = params["vf_coef"],
            max_grad_norm       = params["max_grad_norm"],
            normalize_advantage = params["normalize_advantage"],  # MUST be True for Ant-v5
            policy_kwargs       = policy_kwargs,
            device              = device,
            verbose             = 0,
        )

    def load(self, path: str, env, params: dict, device: str) -> None:
        """
        Load a saved A2C model and attach a fresh VecEnv so training can resume.
        The `env` argument is ignored — a new SubprocVecEnv is always created.
        """
        import os
        from stable_baselines3 import A2C

        n_envs  = params.get("n_envs", 8)
        vec_env = make_vec_env(
            "Ant-v5",
            n_envs      = n_envs,
            vec_env_cls = SubprocVecEnv,
            env_kwargs  = dict(
                ctrl_cost_weight                = 0.5,
                contact_cost_weight             = 5e-4,
                healthy_reward                  = 1.0,
                terminate_when_unhealthy        = True,
                include_cfrc_ext_in_observation = True,
                forward_reward_weight           = 1.0,
            ),
        )
        self.env = vec_env
        zip_path = path if path.endswith(".zip") else path + ".zip"
        if not os.path.isfile(zip_path):
            raise FileNotFoundError(f"Model file not found: {zip_path}")
        self.model = A2C.load(zip_path, env=vec_env, device=device)
```

**A2C implementation notes:**

- `normalize_advantage=True` is **mandatory** for Ant-v5.  Without it, the large variance
  in advantage estimates from a high-dimensional continuous control task causes noisy, unstable
  policy updates.  SB3's default is `False` — always override it.
- `n_envs=8` with `SubprocVecEnv` spawns 8 independent Ant-v5 processes.  Each call to
  `model.learn(total_timesteps=N)` collects `n_steps × n_envs = 2048 × 8 = 16 384` transitions
  before each gradient update, providing diverse experience coverage across the ant's state space.
- The rollout buffer inside SB3 A2C is automatically sized for `n_steps × n_envs`.
- `SubprocVecEnv` is required on Linux/macOS (not `DummyVecEnv`) to avoid the GIL bottleneck
  when running 8 MuJoCo physics simulations in parallel.
- `total_timesteps` counts individual env steps summed across all 8 sub-processes.  To train
  for ~10 M effective steps, set `total_timesteps = 10_000_000`.

---

### 4.3 SACWrapper

File: `algorithms/sac_wrapper.py`

```python
import os
from stable_baselines3 import SAC
from .base_wrapper import BaseWrapper


class SACWrapper(BaseWrapper):
    """
    Wraps SB3's SAC for Ant-v5.
    SAC is off-policy and uses a single environment (n_envs=1).
    """

    ALGO_CLASS = SAC

    def build(self, env, params: dict, device: str) -> None:
        self.env = env
        net_arch      = [params["hidden_size"]] * params["n_hidden_layers"]
        policy_kwargs = dict(net_arch=net_arch)

        self.model = SAC(
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
            ent_coef        = params["ent_coef"],
            target_entropy  = params["target_entropy"],
            policy_kwargs   = policy_kwargs,
            device          = device,
            verbose         = 0,
        )

    # ── Save: model + replay buffer ───────────────────────────────────────────

    def save(self, path: str) -> str:
        """Save model zip and replay buffer pkl side-by-side."""
        zip_path = super().save(path)
        rb_path  = path + "_replay_buffer"
        self.model.save_replay_buffer(rb_path)
        return zip_path   # return primary .zip path for GUI display

    # ── Load: model + optional replay buffer ─────────────────────────────────

    def load(self, path: str, env, params: dict, device: str) -> None:
        """Load model and attempt to restore replay buffer if present."""
        super().load(path, env, params, device)
        rb_path = (path.removesuffix(".zip") if path.endswith(".zip") else path) \
                  + "_replay_buffer.pkl"
        if os.path.isfile(rb_path):
            self.model.load_replay_buffer(rb_path)
```

**SAC notes for Ant-v5:**

- `ent_coef="auto"` with `target_entropy="auto"` is **critical**.  Auto-tuning sets the entropy
  target to `−action_dim = −8` and learns α that balances exploration throughout training.
- `learning_starts=10_000` fills the replay buffer with random transitions before gradient
  updates begin — prevents early Q-value collapse on the 105-dimensional obs space.
- The replay buffer is saved alongside the model as `<name>_replay_buffer.pkl`.  Restoring
  it allows SAC to resume off-policy learning without cold-starting the buffer.

---

### 4.4 TD3Wrapper

File: `algorithms/td3_wrapper.py`

```python
import os
import numpy as np
from stable_baselines3 import TD3
from stable_baselines3.common.noise import NormalActionNoise
from .base_wrapper import BaseWrapper


class TD3Wrapper(BaseWrapper):
    """
    Wraps SB3's TD3 for Ant-v5.
    TD3 is off-policy and uses a single environment (n_envs=1).
    """

    ALGO_CLASS = TD3

    def build(self, env, params: dict, device: str) -> None:
        self.env    = env
        action_dim  = env.action_space.shape[0]   # 8 for Ant-v5
        net_arch    = [params["hidden_size"]] * params["n_hidden_layers"]
        policy_kwargs = dict(net_arch=net_arch)

        action_noise = NormalActionNoise(
            mean  = np.zeros(action_dim),
            sigma = params["exploration_noise"] * np.ones(action_dim),
        )

        self.model = TD3(
            policy              = "MlpPolicy",
            env                 = env,
            learning_rate       = params["learning_rate"],
            buffer_size         = params["buffer_size"],
            learning_starts     = params["learning_starts"],
            batch_size          = params["batch_size"],
            tau                 = params["tau"],
            gamma               = params["gamma"],
            train_freq          = params["train_freq"],
            gradient_steps      = params["gradient_steps"],
            action_noise        = action_noise,
            policy_delay        = params["policy_delay"],
            target_policy_noise = params["target_noise"],
            target_noise_clip   = params["noise_clip"],
            policy_kwargs       = policy_kwargs,
            device              = device,
            verbose             = 0,
        )

    # ── Save: model + replay buffer ───────────────────────────────────────────

    def save(self, path: str) -> str:
        """Save model zip and replay buffer pkl side-by-side."""
        zip_path = super().save(path)
        rb_path  = path + "_replay_buffer"
        self.model.save_replay_buffer(rb_path)
        return zip_path

    # ── Load: model + optional replay buffer ─────────────────────────────────

    def load(self, path: str, env, params: dict, device: str) -> None:
        """Load model and attempt to restore replay buffer if present."""
        super().load(path, env, params, device)
        rb_path = (path.removesuffix(".zip") if path.endswith(".zip") else path) \
                  + "_replay_buffer.pkl"
        if os.path.isfile(rb_path):
            self.model.load_replay_buffer(rb_path)
```

**TD3 notes for Ant-v5:**

- `learning_starts=25_000` — TD3 uses a deterministic policy during rollout (no entropy),
  so it needs more random warm-up than SAC to explore the 8D action space adequately.
- `policy_delay=2` — the actor updates every 2 critic steps, preventing the policy from
  chasing a noisy Q-function during early training.
- `exploration_noise=0.1` — if the ant frequently falls over during early training, increase
  to `0.2` to encourage wider exploration of recovery behaviours.
- The replay buffer (`.pkl`) is saved alongside the `.zip` and auto-loaded on restore.

---

## 5. Hyperparameter Reference

All defaults are sourced from the
[SB3 Zoo Ant-v4/v5 tuned hyperparameters](https://github.com/DLR-RM/rl-baselines3-zoo).
Every parameter listed here **must** appear as an editable widget in the GUI (§ 8.5).

---

### 5.1 A2C Hyperparameters

| SB3 / workbench parameter | GUI label           | Default     | Type  | Description                                                    |
|---------------------------|---------------------|-------------|-------|----------------------------------------------------------------|
| `learning_rate`           | learning_rate       | `3e-4`      | float | LR for the combined actor-critic Adam optimizer                |
| `n_steps`                 | n_steps             | `2048`      | int   | Steps collected **per env** per rollout before each update     |
| `gamma`                   | gamma               | `0.99`      | float | Discount factor γ                                              |
| `gae_lambda`              | gae_lambda          | `0.95`      | float | GAE smoothing λ; 1.0 = pure n-step returns                     |
| `ent_coef`                | ent_coef            | `0.0`       | float | Entropy regularisation coefficient                             |
| `vf_coef`                 | vf_coef             | `0.5`       | float | Value-function loss weight                                     |
| `max_grad_norm`           | max_grad_norm       | `0.5`       | float | Gradient clipping norm                                         |
| `normalize_advantage`     | normalize_advantage | `True`      | bool  | **Must be True for Ant-v5** — normalises advantages per batch  |
| `n_envs`                  | n_envs              | `8`         | int   | Number of parallel `SubprocVecEnv` workers                     |
| `hidden_size`             | hidden_size         | `256`       | int   | Units per hidden layer in actor & critic MLPs                  |
| `n_hidden_layers`         | n_hidden_layers     | `2`         | int   | Number of hidden layers                                        |
| —                         | total_timesteps     | `10000000`  | int   | Total env steps across all n\_envs workers                     |
| —                         | max_episode_steps   | `1000`      | int   | Max steps per episode (used to estimate episode count in GUI)  |

> `normalize_advantage` is a `tk.BooleanVar` checkbox in the GUI, not a text entry.
> `n_envs` must be validated as an integer ≥ 2 and a power of 2 is recommended (2, 4, 8, 16).

---

### 5.2 SAC Hyperparameters

| SB3 parameter   | GUI label         | Default     | Type       | Description                                              |
|-----------------|-------------------|-------------|------------|----------------------------------------------------------|
| `learning_rate` | learning_rate     | `3e-4`      | float      | LR for actor, critics, and α                             |
| `buffer_size`   | buffer_size       | `1000000`   | int        | Replay buffer capacity                                   |
| `learning_starts`| learning_starts  | `10000`     | int        | Env steps before first gradient update                   |
| `batch_size`    | batch_size        | `256`       | int        | Mini-batch size                                          |
| `tau`           | tau               | `0.005`     | float      | Soft target-network update coefficient                   |
| `gamma`         | gamma             | `0.99`      | float      | Discount factor γ                                        |
| `train_freq`    | train_freq        | `1`         | int        | Env steps between gradient cycles                        |
| `gradient_steps`| gradient_steps    | `1`         | int        | Gradient updates per `train_freq` env steps              |
| `ent_coef`      | ent_coef          | `"auto"`    | str/float  | `"auto"` = learnable α; or fixed float                   |
| `target_entropy`| target_entropy    | `"auto"`    | str/float  | `"auto"` = −action\_dim (−8)                             |
| `hidden_size`   | hidden_size       | `256`       | int        | Units per hidden layer                                   |
| `n_hidden_layers`| n_hidden_layers  | `2`         | int        | Number of hidden layers                                  |
| —               | total_timesteps   | `1000000`   | int        | Total env steps (SB3 Zoo: 1e6 sufficient for Ant)        |
| —               | max_episode_steps | `1000`      | int        | Max steps per episode                                    |

---

### 5.3 TD3 Hyperparameters

| SB3 parameter          | GUI label          | Default   | Type  | Description                                              |
|------------------------|--------------------|-----------|-------|----------------------------------------------------------|
| `learning_rate`        | learning_rate      | `3e-4`    | float | LR for actor and both critic networks                    |
| `buffer_size`          | buffer_size        | `1000000` | int   | Replay buffer capacity                                   |
| `learning_starts`      | learning_starts    | `25000`   | int   | Env steps before first gradient update                   |
| `batch_size`           | batch_size         | `256`     | int   | Mini-batch size                                          |
| `tau`                  | tau                | `0.005`   | float | Soft target-network update coefficient                   |
| `gamma`                | gamma              | `0.99`    | float | Discount factor γ                                        |
| `train_freq`           | train_freq         | `1`       | int   | Env steps between gradient updates                       |
| `gradient_steps`       | gradient_steps     | `1`       | int   | Gradient updates per `train_freq` env steps              |
| `policy_delay`         | policy_delay       | `2`       | int   | Actor & target update every N critic steps               |
| `target_policy_noise`  | target_noise       | `0.2`     | float | Std of smoothing noise on target actions                 |
| `target_noise_clip`    | noise_clip         | `0.5`     | float | Max absolute value of smoothing noise                    |
| `exploration_noise`    | exploration_noise  | `0.1`     | float | Std of Gaussian exploration noise                        |
| `hidden_size`          | hidden_size        | `256`     | int   | Units per hidden layer                                   |
| `n_hidden_layers`      | n_hidden_layers    | `2`       | int   | Number of hidden layers                                  |
| —                      | total_timesteps    | `3000000` | int   | Total env steps                                          |
| —                      | max_episode_steps  | `1000`    | int   | Max steps per episode                                    |

---

## 6. Training Thread Protocol

Each algorithm runs in its own `threading.Thread`.  All selected algorithms start simultaneously
when the user clicks **▶ Start**.

### EpisodeCallback — VecEnv-aware per-episode GUI updates

File: `algorithms/episode_callback.py`

Because SB3's `model.learn(total_timesteps=N)` drives training by steps, a custom
`BaseCallback` subclass is used to:

1. Detect episode boundaries for **both** single-env (SAC/TD3) and VecEnv (A2C).
2. Compute episode reward and push a metric dict onto `data_queue`.
3. Check `stop_event` and abort by returning `False` from `_on_step`.

```python
# algorithms/episode_callback.py

import queue
import threading
import numpy as np
from collections import deque
from stable_baselines3.common.callbacks import BaseCallback

SOLVE_THRESHOLD = 6_000.0
SOLVE_WINDOW    = 100


class EpisodeCallback(BaseCallback):
    """
    VecEnv-aware episode callback.

    Works for both:
      - Single env  (SAC, TD3): rewards/dones arrays of length 1
      - VecEnv      (A2C n_envs=8): rewards/dones arrays of length n_envs

    Episode rewards are read preferentially from `infos[i]["episode"]["r"]`
    (injected by SB3's Monitor/VecMonitor wrapper) so they are accurate for
    VecEnv environments where partial episodes are cut off at rollout boundaries.
    """

    def __init__(
        self,
        algo_label:   str,
        data_queue:   queue.Queue,
        stop_event:   threading.Event,
        logger,
        total_timesteps: int,
        max_episode_steps: int = 1000,
        verbose: int = 0,
    ):
        super().__init__(verbose)
        self.algo_label        = algo_label
        self.data_queue        = data_queue
        self.stop_event        = stop_event
        self.run_logger        = logger
        self.total_timesteps   = total_timesteps
        self.max_episode_steps = max_episode_steps

        self._ep_count      = 0
        self._max_reward    = -np.inf
        self._reward_window = deque(maxlen=SOLVE_WINDOW)
        self._solved_at     = None

        # Per-env reward accumulators (used when Monitor wrapper is absent)
        self._ep_rewards: dict[int, float] = {}
        self._ep_steps:   dict[int, int]   = {}

    def _on_step(self) -> bool:
        """Called after every env step. Return False to stop training."""
        if self.stop_event.is_set():
            return False

        rewards = self.locals["rewards"]   # np.ndarray shape (n_envs,)
        dones   = self.locals["dones"]     # np.ndarray shape (n_envs,)
        infos   = self.locals["infos"]     # list[dict] length n_envs

        for i, (reward, done, info) in enumerate(zip(rewards, dones, infos)):
            # Accumulate per-env reward as fallback
            self._ep_rewards[i] = self._ep_rewards.get(i, 0.0) + float(reward)
            self._ep_steps[i]   = self._ep_steps.get(i, 0)    + 1

            if done:
                # Prefer Monitor-injected episode reward (accurate for VecEnv)
                ep_r = float(
                    info.get("episode", {}).get("r", self._ep_rewards[i])
                )
                self._process_episode(ep_r, self._ep_steps[i])
                self._ep_rewards[i] = 0.0
                self._ep_steps[i]   = 0

        return True

    def _process_episode(self, ep_reward: float, ep_steps: int) -> None:
        """Update statistics and push a data dict onto the queue."""
        self._ep_count += 1
        self._reward_window.append(ep_reward)
        self._max_reward = max(self._max_reward, ep_reward)
        rolling_100 = float(np.mean(self._reward_window))

        solved = (
            len(self._reward_window) == SOLVE_WINDOW
            and rolling_100 >= SOLVE_THRESHOLD
            and self._solved_at is None
        )
        if solved:
            self._solved_at = self._ep_count

        total_eps = self.total_timesteps // self.max_episode_steps

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

        self.run_logger.log(
            episode          = self._ep_count,
            total_reward     = ep_reward,
            steps            = ep_steps,
            rolling_mean_100 = rolling_100,
            max_reward       = self._max_reward,
            solved           = solved,
        )
```

### Training thread function

```python
# algorithms/episode_callback.py (also define this helper here)

def run_training(
    wrapper,
    params:     dict,
    algo_label: str,
    data_queue: queue.Queue,
    stop_event: threading.Event,
    device:     str,
    resume:     bool = False,   # True = loaded model, continue from checkpoint
) -> None:
    """
    Called in a daemon Thread for each algorithm.

    For A2C: wrapper.build() creates SubprocVecEnv internally — pass env=None.
    For SAC/TD3: a single Ant-v5 env is created here and passed to build().
    If resume=True, wrapper.model already exists and env is already set;
    skip build() and call learn() with reset_num_timesteps=False.
    """
    import gymnasium as gym
    from utils.logger import RunLogger

    single_env_algos = {"SAC", "TD3"}
    env = None

    try:
        if not resume:
            if algo_label in single_env_algos:
                env = gym.make(
                    "Ant-v5",
                    ctrl_cost_weight                = 0.5,
                    contact_cost_weight             = 5e-4,
                    healthy_reward                  = 1.0,
                    terminate_when_unhealthy        = True,
                    include_cfrc_ext_in_observation = True,
                    forward_reward_weight           = 1.0,
                    render_mode                     = None,
                )
            # A2C: env=None — A2CWrapper.build() creates SubprocVecEnv itself
            wrapper.build(env, params, device)
        # If resume=True: wrapper.model already loaded; env already set

        logger = RunLogger(algo=algo_label)
        callback = EpisodeCallback(
            algo_label        = algo_label,
            data_queue        = data_queue,
            stop_event        = stop_event,
            logger            = logger,
            total_timesteps   = params["total_timesteps"],
            max_episode_steps = params.get("max_episode_steps", 1000),
        )

        wrapper.model.learn(
            total_timesteps     = params["total_timesteps"],
            callback            = callback,
            reset_num_timesteps = not resume,
        )

    except Exception as exc:
        data_queue.put({"algo": algo_label, "_error": str(exc)})

    finally:
        # Close single env (A2C's VecEnv is closed by the wrapper)
        if env is not None:
            try:
                env.close()
            except Exception:
                pass
        data_queue.put({"algo": algo_label, "_done": True})
```

---

## 7. Model Persistence — Save & Load

This section specifies every aspect of the save/load feature visible in the GUI (§ 8.7).

---

### 7.1 Save

**When the user clicks [💾 Save] for an algorithm's row:**

1. Check that `wrapper.model` is not `None`; if it is, show `messagebox.showwarning` and abort.
2. Open `tkinter.filedialog.asksaveasfilename` with:
   ```python
   path = filedialog.asksaveasfilename(
       title            = f"Save {algo} model",
       initialdir       = "ant_workbench/results/models",
       initialfile      = f"{algo}_{timestamp}",
       defaultextension = ".zip",
       filetypes        = [("SB3 model", "*.zip"), ("All files", "*.*")],
   )
   ```
3. If the user cancels, do nothing.
4. Strip the `.zip` extension from `path` before passing to `wrapper.save(path)` — SB3 adds it.
5. Call `wrapper.save(path_without_ext)` in a **background thread** to avoid freezing the GUI
   (replay buffers for SAC/TD3 can be >1 GB).
6. On completion, log to the status log:
   ```
   [SAC] model saved → ant_workbench/results/models/SAC_20260504_143022.zip
   [SAC] replay buffer saved → SAC_20260504_143022_replay_buffer.pkl
   ```

**Files written per algorithm:**

| Algorithm | Files written                                    |
|-----------|--------------------------------------------------|
| A2C       | `<name>.zip`                                     |
| SAC       | `<name>.zip` + `<name>_replay_buffer.pkl`        |
| TD3       | `<name>.zip` + `<name>_replay_buffer.pkl`        |

---

### 7.2 Load & Resume

**When the user clicks [📂 Load] for an algorithm's row:**

1. Open `tkinter.filedialog.askopenfilename` with:
   ```python
   path = filedialog.askopenfilename(
       title      = f"Load {algo} model",
       initialdir = "ant_workbench/results/models",
       filetypes  = [("SB3 model", "*.zip"), ("All files", "*.*")],
   )
   ```
2. If the user cancels, do nothing.
3. If training is currently active for that algorithm, show `messagebox.askyesno`:
   > "Training is in progress for {algo}. Stop it and load the model?"
   If "No", abort.  If "Yes", call `stop_event.set()` and wait for the training thread.
4. In a **background thread**:
   a. Create a fresh env appropriate for the algorithm (single env for SAC/TD3; `None` for A2C
      since `A2CWrapper.load()` creates the VecEnv internally).
   b. Call `wrapper.load(path, env, params, device)`.
   c. Attempt to load the replay buffer from `<path_stem>_replay_buffer.pkl` (SAC/TD3 only —
      done inside `SACWrapper.load()` / `TD3Wrapper.load()` automatically).
5. On success, push a special queue item so the GUI updates:
   ```python
   data_queue.put({"algo": algo_label, "_loaded": path})
   ```
6. The `TrainingPanel.on_data()` handler for `"_loaded"` logs:
   ```
   [SAC] model loaded ← ant_workbench/results/models/SAC_20260504_143022.zip
   [SAC] replay buffer restored (847 312 transitions)
   ```
7. The **[▶ Start]** button now resumes training from the loaded checkpoint using
   `reset_num_timesteps=False` (enforced by `run_training(resume=True)`).
8. The **progress bar** for the loaded algorithm resets to 0% so the new run's progress
   is displayed cleanly, but the status log retains the load message.

**Resume with different hyperparameters:**

- After loading, the user may edit any hyperparameter in the GUI before clicking **[▶ Start]**.
- The new `learning_rate`, `batch_size`, etc. from the GUI are passed to `run_training`
  as the `params` dict.
- For SAC/TD3, the replay buffer is preserved so off-policy learning resumes with accumulated
  experience.
- For A2C (on-policy), there is no replay buffer; the new VecEnv and updated hyperparameters
  apply immediately on the next rollout collection.
- **Structural parameters** (`hidden_size`, `n_hidden_layers`) cannot be changed after loading
  because the network architecture is fixed in the saved weights.  If the user modifies these,
  show a `messagebox.showwarning`:
  > "Network architecture (hidden_size, n_hidden_layers) cannot be changed after loading.
  > The saved values will be used."

---

### 7.3 File Naming Convention

```
ant_workbench/results/models/
    SAC_20260504_143022.zip
    SAC_20260504_143022_replay_buffer.pkl
    TD3_20260504_151844.zip
    TD3_20260504_151844_replay_buffer.pkl
    A2C_20260504_162201.zip
```

Timestamp format: `YYYYMMDD_HHMMSS`.

The default `initialfile` in the save dialog pre-fills this pattern; the user may change it.

---

## 8. GUI Architecture — Detailed Specification

> This section is the **primary reference** for `ant_ui.py` and all files in `gui/`.
> Implement every pixel detail exactly as described.  Where exact pixel sizes are given, use them.
> Where colours are given as hex codes, use exactly those codes.

---

### 8.1 Window & Root Settings

```python
root = tk.Tk()
root.title("Ant-v5 RL Workbench")
root.minsize(1200, 720)
root.geometry("1280x780")
root.configure(bg="#0E0E10")
```

- The window is **resizable** in both axes.
- On close: call `training_panel.stop_all()`, wait 400 ms, then `root.destroy()`.

---

### 8.2 Colour Palette & Fonts

```python
# ── Backgrounds ───────────────────────────────────────────────────────────────
BG0  = "#0E0E10"
BG1  = "#1C1C1E"
BG2  = "#28282C"
BG3  = "#38383C"

# ── Borders ───────────────────────────────────────────────────────────────────
BORDER_DARK   = "#3A3A3C"
BORDER_MID    = "#48484A"
BORDER_LIGHT  = "#636366"

# ── Text ──────────────────────────────────────────────────────────────────────
TEXT_PRIMARY   = "#F2F2F7"
TEXT_SECONDARY = "#AEAEB2"
TEXT_TERTIARY  = "#636366"
TEXT_DANGER    = "#FF453A"
TEXT_INFO      = "#0A84FF"
TEXT_SUCCESS   = "#30D158"

# ── Algorithm accents ─────────────────────────────────────────────────────────
COLOR_A2C       = "#4CAF50"
COLOR_SAC       = "#2196F3"
COLOR_TD3       = "#FF5722"
ALGO_COLORS     = {"A2C": COLOR_A2C, "SAC": COLOR_SAC, "TD3": COLOR_TD3}

# ── Button styles ─────────────────────────────────────────────────────────────
BTN_BG          = "#2C2C2E"
BTN_BG_HOVER    = "#3A3A3C"
BTN_START_BG    = "#1B3A1C"
BTN_START_FG    = "#4CAF50"
BTN_STOP_FG     = "#FF453A"
BTN_ANIM_FG     = "#0A84FF"
BTN_SAVE_FG     = "#30D158"
BTN_LOAD_FG     = "#FFD60A"
BTN_BORDER      = "#48484A"

# ── Pills ─────────────────────────────────────────────────────────────────────
PILL_RUNNING_BG = "#1B3A1C"
PILL_RUNNING_FG = "#4CAF50"
PILL_IDLE_BG    = "#28282C"
PILL_IDLE_FG    = "#636366"

# ── Fonts ─────────────────────────────────────────────────────────────────────
FONT_HEADER  = ("Helvetica", 13, "bold")
FONT_LABEL   = ("Helvetica", 11)
FONT_SMALL   = ("Helvetica", 10)
FONT_MONO    = ("Courier New", 10)
FONT_MONO_SM = ("Courier New", 9)
FONT_SECTION = ("Helvetica", 9, "bold")

# ── Solve line ────────────────────────────────────────────────────────────────
SOLVE_LINE_COLOR = "#FF453A"
SOLVE_THRESHOLD  = 6_000.0
```

---

### 8.3 Overall Layout

```
┌──────────────────────────────────────────────────────────────────────────┐  BG0
│  HEADER BAR  (40 px)                                                     │  § 8.4
├───────────────────────┬──────────────────────────────────────────────────┤
│                       │                                                  │
│  HyperparamPanel      │  ComparisonPanel                                 │
│  (fixed 270 px wide)  │  (fills remaining width)                         │
│                       │                                                  │
│  § 8.5                │  § 8.6                                           │
│                       │                                                  │
├───────────────────────┴──────────────────────────────────────────────────┤
│  BOTTOM BAR  TrainingPanel  (min 190 px)                                 │  § 8.7
└──────────────────────────────────────────────────────────────────────────┘
```

```python
class AntUI(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg=BG0)
        self.data_queue = queue.Queue()

        self.header = HeaderBar(self)
        self.header.pack(side="top", fill="x")

        middle = tk.Frame(self, bg=BG0)
        middle.pack(side="top", fill="both", expand=True)

        self.hyperparam_panel = HyperparamPanel(middle)
        self.hyperparam_panel.pack(side="left", fill="y")
        tk.Frame(middle, bg=BORDER_DARK, width=1).pack(side="left", fill="y")

        self.comparison_panel = ComparisonPanel(middle, self.data_queue)
        self.comparison_panel.pack(side="left", fill="both", expand=True)

        tk.Frame(self, bg=BORDER_DARK, height=1).pack(side="top", fill="x")

        self.training_panel = TrainingPanel(
            self,
            hyperparam_panel = self.hyperparam_panel,
            comparison_panel = self.comparison_panel,
            data_queue       = self.data_queue,
            header_bar       = self.header,
        )
        self.training_panel.pack(side="bottom", fill="x")

        self._poll()

    def _poll(self):
        try:
            while True:
                item = self.data_queue.get_nowait()
                self.comparison_panel.on_data(item)
                self.training_panel.on_data(item)
        except queue.Empty:
            pass
        self.after(200, self._poll)
```

---

### 8.4 Header Bar

Implemented as `HeaderBar` class inside `ant_ui.py`.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Ant-v5 RL Workbench        [● idle]   [obs: 105] [act: 8] [CPU]           │
└─────────────────────────────────────────────────────────────────────────────┘
```

- Background: `BG1`, height 40 px (`pady=8`), 1 px `BORDER_DARK` bottom line.
- **Left**: title label (font `FONT_HEADER`, fg `TEXT_PRIMARY`) + status pill.
- **Right**: three tag labels — `"obs: 105"`, `"act: 8"`, `"CPU"`/`"CUDA"`.
- Status pill styles: idle (`PILL_IDLE_BG/FG`, text `"○ idle"`),
  running (`PILL_RUNNING_BG/FG`, text `"● training"`),
  stopped (`PILL_IDLE_BG/FG`, text `"○ stopped"`).
- Exposes `set_status(style)` and `set_device(label)`.

---

### 8.5 HyperparamPanel — Left Column

File: `gui/hyperparameter_panel.py`

- Fixed width 270 px (`self.pack_propagate(False)`).  Background `BG1`.
- `ttk.Notebook` with four tabs: **A2C**, **SAC**, **TD3**, **Sweep**.
- Styled with `ttk.Style()` using the `"clam"` theme (dark colours, see previous spec).

#### Inside each algorithm tab

Scrollable canvas containing:

1. **Section headers** — `tk.Label` uppercase, `TEXT_TERTIARY`, `FONT_SECTION`.
2. **Parameter rows** — `Label` (monospace, `TEXT_SECONDARY`, width 18) + `Entry`
   (`BG2`, `TEXT_PRIMARY`, 1 px `BORDER_MID` frame).
3. For `normalize_advantage` (A2C only): a `tk.Checkbutton` (`BG1`,
   `selectcolor=BG3`), not a text entry.
4. **"Reset to Defaults"** button at the bottom of each tab.

On `<FocusOut>` / `<Return>`: call `_validate_field` — invalid entries turn `"#3A0A0A"` bg
with `TEXT_DANGER` border.

**Section groupings:**

*A2C tab:*
- **NETWORK**: `learning_rate`, `gamma`, `gae_lambda`, `ent_coef`, `vf_coef`, `max_grad_norm`
- **TRAINING CONFIG**: `normalize_advantage` (checkbox), `n_envs`
- **ARCHITECTURE**: `hidden_size`, `n_hidden_layers`
- **RUN**: `total_timesteps`, `max_episode_steps`, `n_steps`

*SAC tab:*
- **CORE**: `learning_rate`, `gamma`, `tau`, `ent_coef`, `target_entropy`
- **BUFFER**: `buffer_size`, `learning_starts`, `batch_size`
- **UPDATE**: `train_freq`, `gradient_steps`
- **ARCHITECTURE**: `hidden_size`, `n_hidden_layers`
- **RUN**: `total_timesteps`, `max_episode_steps`

*TD3 tab:*
- **CORE**: `learning_rate`, `gamma`, `tau`
- **BUFFER**: `buffer_size`, `learning_starts`, `batch_size`
- **UPDATE**: `train_freq`, `gradient_steps`, `policy_delay`
- **NOISE**: `target_noise`, `noise_clip`, `exploration_noise`
- **ARCHITECTURE**: `hidden_size`, `n_hidden_layers`
- **RUN**: `total_timesteps`, `max_episode_steps`

*Sweep tab:* (unchanged from previous spec)

#### `get_params(algo: str) -> dict`

Returns a typed dict including `normalize_advantage` as a `bool` for A2C.

---

### 8.6 ComparisonPanel — Right Column

File: `gui/comparison_panel.py`

Embeds a `matplotlib` figure via `FigureCanvasTkAgg` with two dark-themed subplots:

- **Top**: raw episode reward (alpha 0.25, lw 0.7) + 10-ep rolling mean (alpha 0.95, lw 1.6).
- **Bottom**: 100-ep rolling mean (lw 2.0).
- Both subplots: solve threshold dashed red line at y = 6 000.
- `facecolor="#28282C"` for both axes and the figure.
- Legend: A2C (green), SAC (blue), TD3 (orange), Solve (red dashed).
- `NavigationToolbar2Tk` below the canvas.
- `on_data(item)` updates lines via `set_data` + `draw_idle`.
- `clear_all()` removes all lines, redraws empty axes.
- `save_plot() -> str` calls `utils.metrics.save_plot(fig)`.
- `get_score_data() -> list[dict]` returns per-run stats for the score table.

---

### 8.7 TrainingPanel — Bottom Bar

File: `gui/training_panel.py`

Four sub-rows stacked vertically inside `tk.Frame(bg=BG1)`:

```
ROW 1 — Control buttons  |  Algorithm checkboxes  |  Action buttons
ROW 2 — Per-algorithm progress bars + Save/Load buttons   ← NEW
ROW 3 — Status log
```

---

#### ROW 1 — Controls

`tk.Frame(bg=BG1)`, padx=12, pady=6.

**Left group — control buttons:**
```
[▶ Start]   [■ Stop]   [↺ Reset]
```
- `▶ Start`: bg `BTN_START_BG`, fg `BTN_START_FG`.
  Disabled + text `"▶ Running…"` while training.
- `■ Stop`: fg `BTN_STOP_FG`.  Disabled when idle.
- `↺ Reset`: always enabled.

**Centre group — algorithm checkboxes:**
```
☑ A2C   ☑ SAC   ☑ TD3   ☐ Use GPU
```
Each `tk.Checkbutton` with `BooleanVar`.  Wrapper frame uses algorithm accent colour when
checked, `BORDER_DARK` when unchecked.  GPU checkbox calls `header_bar.set_device`.

**Right group — action buttons:**
```
[🎬 Show Animation]   [📋 Score Table]   [💾 Save Plot]
```
Same as previous spec.

---

#### ROW 2 — Per-algorithm progress + Save/Load

`tk.Frame(bg=BG1)`, padx=12, pady=4.

Each algorithm has **one row** with the following layout:

```
[algo label]  [══════════░░░░░░] ep 1142/~10000 · 4 312   [💾 Save]  [📂 Load]
```

Exact specification per row:

- **Algo label**: `tk.Label`, width 4, fg = `ALGO_COLORS[algo]`, font `FONT_MONO_SM`,
  bg `BG1`, anchor `"w"`.

- **Progress bar** (canvas-based bar as before): fills `fill="x", expand=True`.

- **Episode info label**: `tk.Label`, width 26, fg `TEXT_TERTIARY`, font `FONT_MONO_SM`,
  bg `BG1`, anchor `"e"`.  Format: `"ep {episode}/~{total_eps} · {reward:,.0f}"`.

- **[💾 Save] button**:
  ```python
  tk.Button(
      row,
      text            = "💾",
      bg              = BTN_BG,
      fg              = BTN_SAVE_FG,       # green
      activebackground= BTN_BG_HOVER,
      relief          = "flat",
      bd              = 0,
      font            = FONT_MONO_SM,
      padx            = 6,
      pady            = 2,
      cursor          = "hand2",
      command         = lambda a=algo: self._save_model(a),
  )
  ```

- **[📂 Load] button**:
  ```python
  tk.Button(
      row,
      text            = "📂",
      bg              = BTN_BG,
      fg              = BTN_LOAD_FG,       # yellow
      activebackground= BTN_BG_HOVER,
      relief          = "flat",
      bd              = 0,
      font            = FONT_MONO_SM,
      padx            = 6,
      pady            = 2,
      cursor          = "hand2",
      command         = lambda a=algo: self._load_model(a),
  )
  ```

- After a successful **load**, append a small badge next to the info label:
  `tk.Label(row, text=" ⟳ loaded", fg=BTN_LOAD_FG, bg=BG1, font=FONT_MONO_SM)`.
  This badge is destroyed when the algorithm starts a new (non-resumed) training run.

#### `_save_model(algo: str)` implementation

```python
def _save_model(self, algo: str) -> None:
    wrapper = self._wrappers.get(algo)
    if wrapper is None or wrapper.model is None:
        messagebox.showwarning("No model", f"No trained {algo} model to save.")
        return

    os.makedirs("ant_workbench/results/models", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    path = filedialog.asksaveasfilename(
        title            = f"Save {algo} model",
        initialdir       = "ant_workbench/results/models",
        initialfile      = f"{algo}_{ts}",
        defaultextension = ".zip",
        filetypes        = [("SB3 model", "*.zip"), ("All files", "*.*")],
    )
    if not path:
        return   # user cancelled

    path_stem = path.removesuffix(".zip")

    def _do_save():
        try:
            saved = wrapper.save(path_stem)
            self.data_queue.put({"algo": algo, "_saved": saved})
        except Exception as exc:
            self.data_queue.put({"algo": algo, "_error": f"Save failed: {exc}"})

    threading.Thread(target=_do_save, daemon=True).start()
```

#### `_load_model(algo: str)` implementation

```python
def _load_model(self, algo: str) -> None:
    path = filedialog.askopenfilename(
        title      = f"Load {algo} model",
        initialdir = "ant_workbench/results/models",
        filetypes  = [("SB3 model", "*.zip"), ("All files", "*.*")],
    )
    if not path:
        return

    # Stop active training for this algo if running
    if self._is_training(algo):
        if not messagebox.askyesno(
            "Training active",
            f"Training is running for {algo}. Stop it and load the model?"
        ):
            return
        self._stop_one(algo)

    params = self.hyperparam_panel.get_params(algo)
    device = "cuda" if self._gpu_var.get() and torch.cuda.is_available() else "cpu"

    def _do_load():
        try:
            # Build fresh env for SAC/TD3; A2CWrapper.load() creates its own VecEnv
            env = None
            if algo in ("SAC", "TD3"):
                import gymnasium as gym
                env = gym.make(
                    "Ant-v5",
                    ctrl_cost_weight=0.5, contact_cost_weight=5e-4,
                    healthy_reward=1.0, terminate_when_unhealthy=True,
                    include_cfrc_ext_in_observation=True,
                    forward_reward_weight=1.0, render_mode=None,
                )
            wrapper = self._get_or_create_wrapper(algo)
            wrapper.load(path, env, params, device)
            self._wrappers[algo] = wrapper
            self._loaded_flags[algo] = True
            self.data_queue.put({"algo": algo, "_loaded": path})
        except Exception as exc:
            self.data_queue.put({"algo": algo, "_error": f"Load failed: {exc}"})

    threading.Thread(target=_do_load, daemon=True).start()
```

#### `on_data` additions for save/load events

```python
def on_data(self, item: dict):
    algo = item["algo"]

    if "_saved" in item:
        self._log(algo, f"model saved → {item['_saved']}")
        return

    if "_loaded" in item:
        self._log(algo, f"model loaded ← {item['_loaded']}")
        self._show_loaded_badge(algo)
        return

    # ... existing handling for episode metrics, _done, _error ...
```

#### `_start_training` — resume-aware start

```python
def _start_training(self):
    """Called when ▶ Start is clicked."""
    selected = [a for a in ("A2C","SAC","TD3") if self._algo_vars[a].get()]
    if not selected:
        messagebox.showwarning("No algorithm", "Select at least one algorithm.")
        return

    device = "cuda" if self._gpu_var.get() and torch.cuda.is_available() else "cpu"

    for algo in selected:
        params  = self.hyperparam_panel.get_params(algo)
        wrapper = self._get_or_create_wrapper(algo)
        resume  = self._loaded_flags.get(algo, False)

        self._stop_events[algo] = threading.Event()
        t = threading.Thread(
            target = run_training,
            kwargs = dict(
                wrapper    = wrapper,
                params     = params,
                algo_label = algo,
                data_queue = self.data_queue,
                stop_event = self._stop_events[algo],
                device     = device,
                resume     = resume,
            ),
            daemon = True,
            name   = f"train-{algo}",
        )
        self._threads[algo] = t
        self._loaded_flags[algo] = False   # clear resume flag after use
        t.start()

    self._set_training_ui_state(active=True)
```

---

#### ROW 3 — Status log

`tk.Text` widget, height 3, read-only, bg `BG0`, font `FONT_MONO_SM`.
Coloured tags: `A2C`, `SAC`, `TD3`, `SYS`, `OK`, `ERR`.
Additional tags: `SAVE` (fg `BTN_SAVE_FG`), `LOAD` (fg `BTN_LOAD_FG`).

Log every 10 episodes per algorithm.  Always log save, load, error, and done events
regardless of episode frequency.

---

### 8.8 AnimationWindow — Popup

File: `gui/animation_window.py`

> **Unchanged from the version specified in the previous iteration of this document.**
> All render-thread architecture, `rgb_array` mode, Pillow frame pipeline, `_poll_frames`,
> `_close` non-blocking shutdown, and lag-prevention checklist remain identical.
> Only the environment ID changes: use `"Ant-v5"` with `include_cfrc_ext_in_observation=True`.

The `_make_render_env` static method must be updated:

```python
@staticmethod
def _make_render_env():
    import gymnasium as gym
    return gym.make(
        "Ant-v5",
        render_mode                     = "rgb_array",
        ctrl_cost_weight                = 0.5,
        contact_cost_weight             = 5e-4,
        healthy_reward                  = 1.0,
        terminate_when_unhealthy        = True,
        include_cfrc_ext_in_observation = True,
        forward_reward_weight           = 1.0,
    )
```

All other methods, geometry, stat tiles, title bar, and thread management are identical to
the `animation_window.py` file already generated — do not regenerate it from scratch;
apply only the env-ID change above.

---

### 8.9 ScoreTable Popup

Geometry: `560×280`, titled `"Score Comparison — Ant-v5"`.

Same layout as previous spec.  Table columns: Run | Mean (100) | Std | Max | Solved.
Data sourced from `comparison_panel.get_score_data()`.

---

### 8.10 Thread-Safe Data Flow

```
Training thread (per algo)
        │  puts dict onto data_queue (never touches Tkinter)
        ▼
    data_queue  (queue.Queue)
        │  drained every 200 ms by AntUI._poll()
        ├─→ comparison_panel.on_data(item)   — matplotlib updates
        └─→ training_panel.on_data(item)     — progress bars, log, badges
```

**Queue item schema (complete):**

```python
# Episode metric
{"algo": str, "episode": int, "total_eps": int, "reward": float,
 "rolling_100": float, "max_reward": float, "solved_at": int|None, "steps": int}

# Training finished
{"algo": str, "_done": True}

# Training error
{"algo": str, "_error": str}

# Model saved successfully
{"algo": str, "_saved": str}        # str = full path of .zip file

# Model loaded successfully
{"algo": str, "_loaded": str}       # str = path of loaded .zip file

# Reset command
{"_command": "reset"}
```

---

## 9. Entrypoint

File: `project_agent.py`

```python
#!/usr/bin/env python3
"""
Ant-v5 RL Workbench — Entrypoint
Run:  python project_agent.py
"""

import tkinter as tk
from ant_ui import AntUI


def main() -> None:
    root = tk.Tk()
    root.title("Ant-v5 RL Workbench")
    root.minsize(1200, 720)
    root.geometry("1280x780")
    root.configure(bg="#0E0E10")

    app = AntUI(root)
    app.pack(fill="both", expand=True)

    def on_close():
        app.training_panel.stop_all()
        root.after(400, root.destroy)

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
```

---

## 10. Utils

### 10.1 Logger

File: `utils/logger.py`

```python
import csv, os
from datetime import datetime

class RunLogger:
    FIELDNAMES = ["episode", "total_reward", "steps",
                  "rolling_mean_100", "max_reward", "solved"]

    def __init__(self, algo: str, run_id: str | None = None):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        rid = f"_{run_id}" if run_id else ""
        os.makedirs("ant_workbench/results", exist_ok=True)
        self._path = f"ant_workbench/results/{algo}{rid}_{ts}.csv"
        with open(self._path, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=self.FIELDNAMES).writeheader()

    def log(self, episode, total_reward, steps, rolling_mean_100, max_reward, solved):
        with open(self._path, "a", newline="") as f:
            csv.DictWriter(f, fieldnames=self.FIELDNAMES).writerow({
                "episode": episode, "total_reward": round(total_reward, 4),
                "steps": steps, "rolling_mean_100": round(rolling_mean_100, 4),
                "max_reward": round(max_reward, 4), "solved": int(solved),
            })
```

### 10.2 Metrics

File: `utils/metrics.py`

```python
import os, numpy as np, matplotlib.figure
from datetime import datetime

def rolling_mean(rewards: list[float], window: int) -> list[float]:
    return [float(np.mean(rewards[max(0,i-window+1):i+1])) for i in range(len(rewards))]

def save_plot(fig: matplotlib.figure.Figure,
              output_dir: str = "ant_workbench/results") -> str:
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"training_plot_{ts}.png")
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    return path
```

---

## 11. Validation Rules

Enforce in `HyperparamPanel._validate_field`.  Invalid entries: bg `"#3A0A0A"`, border
`TEXT_DANGER`.  Block **▶ Start** if any active algorithm tab has invalid fields.

| Parameter           | Rule                                                  |
|---------------------|-------------------------------------------------------|
| `learning_rate`     | `0 < x ≤ 1`                                           |
| `gamma`             | `0 < x < 1`                                           |
| `tau`               | `0 < x ≤ 1`                                           |
| `batch_size`        | `int ≥ 1`; ≤ `buffer_size` (off-policy)               |
| `buffer_size`       | `int ≥ batch_size`                                    |
| `learning_starts`   | `int ≥ batch_size`                                    |
| `n_steps`           | `int ≥ 1`                                             |
| `n_envs`            | `int ≥ 2`; recommended power of 2                     |
| `gae_lambda`        | `0 ≤ x ≤ 1`                                           |
| `vf_coef`           | `x > 0`                                               |
| `ent_coef`          | `x ≥ 0` (A2C) or `"auto"` / `x ≥ 0` (SAC)           |
| `target_entropy`    | `"auto"` or any float                                 |
| `hidden_size`       | `int ≥ 1`                                             |
| `n_hidden_layers`   | `int ≥ 1`                                             |
| `policy_delay`      | `int ≥ 1`                                             |
| `target_noise`      | `x ≥ 0`                                               |
| `noise_clip`        | `x ≥ target_noise`                                    |
| `exploration_noise` | `x ≥ 0`                                               |
| `total_timesteps`   | `int ≥ 1000`                                          |
| `max_episode_steps` | `int ≥ 1`, warn if > 1000                             |
| `normalize_advantage`| must be `True` for A2C — warn if unchecked          |

---

## 12. Solve Criterion

```python
SOLVE_THRESHOLD = 6_000.0
SOLVE_WINDOW    = 100
```

Solved when the 100-episode rolling mean first reaches ≥ 6 000.
Detected in `EpisodeCallback._process_episode()` and propagated via queue `"solved_at"`.

**Expected convergence on GPU (RTX 3090, recommended defaults):**

| Algorithm | Total timesteps to solve | Wall time  | Notes                                    |
|-----------|--------------------------|------------|------------------------------------------|
| SAC       | ~1 000 000               | ~20 min    | Most sample-efficient; auto entropy tuning is key |
| TD3       | ~2 000 000 – 3 000 000   | ~30–45 min | Stable but slower than SAC               |
| A2C       | ~8 000 000 – 12 000 000  | ~90–150 min| Requires n_envs=8 + normalize_advantage  |

> A2C wall time with n\_envs=8 is higher than SAC/TD3 despite more compute, because
> on-policy algorithms are fundamentally less sample-efficient than off-policy ones.
> A2C may not reliably reach 6 000 on Ant-v5 within 10 M steps — increase
> `total_timesteps` to 15 M or 20 M if convergence plateaus below the threshold.
