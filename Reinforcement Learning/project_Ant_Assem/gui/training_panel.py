import tkinter as tk
from tkinter import ttk, messagebox, filedialog as fd
import threading
import queue
import gymnasium as gym
import traceback
import datetime
from algorithms.a2c_wrapper import A2CWrapper
from algorithms.sac_wrapper import SACWrapper
from algorithms.td3_wrapper import TD3Wrapper
from algorithms.episode_callback import EpisodeCallback
from utils.logger import RunLogger
from utils.device import get_device
from gui.animation_window import AnimationWindow

def train_agent(data_queue, stop_event, algo_label, wrapper, params):
    """Standalone training function for threading."""
    print(f"[TRAINING] Starting {algo_label} on device: {wrapper.get_device()}")

    env = wrapper.env

    # Calculate episodes
    target_episodes = params["total_timesteps"] // params["max_episode_steps"]
    if algo_label.startswith("A2C"):
        n_envs = params.get("n_envs", 1)
        total_eps = target_episodes * n_envs
        max_episodes = None
    else:
        total_eps = target_episodes
        max_episodes = target_episodes

    logger = RunLogger(algo=algo_label)
    callback = EpisodeCallback(
        algo_label=algo_label,
        data_queue=data_queue,
        stop_event=stop_event,
        logger=logger,
        max_episodes=max_episodes,
        total_eps=total_eps,
    )

    try:
        wrapper.model.learn(
            total_timesteps=params["total_timesteps"],
            callback=callback,
            reset_num_timesteps=True,
        )
    except Exception as exc:
        error_msg = f"{exc}\n{traceback.format_exc()}"
        data_queue.put({"algo": algo_label, "_error": error_msg})
    finally:
        env.close()
        data_queue.put({"algo": algo_label, "_done": True})

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
TEXT_INFO      = "#0A84FF"
TEXT_SUCCESS   = "#30D158"
FONT_SMALL   = ("Helvetica", 10)
FONT_MONO_SM = ("Courier New", 9)
COLOR_A2C = "#4CAF50"
COLOR_SAC = "#2196F3"
COLOR_TD3 = "#FF5722"
BTN_BG = "#2C2C2E"
BTN_BG_HOVER = "#3A3A3C"
BTN_START_BG = "#1B3A1C"
BTN_START_FG = "#4CAF50"
BTN_STOP_FG = "#FF453A"
BTN_ANIM_FG = "#0A84FF"


class TrainingPanel(tk.Frame):
    def __init__(self, master, hyperparam_panel, comparison_panel, data_queue, header_bar):
        super().__init__(master, bg=BG1)
        self.hyperparam_panel = hyperparam_panel
        self.comparison_panel = comparison_panel
        self.data_queue = data_queue
        self.header_bar = header_bar

        self._stop_events = {}
        self._threads = {}
        self._animation = None
        self._training_active = False
        self._current_agents = {}  # Store current training agents

        self._build_ui()

    def _build_ui(self):
        self.row1 = tk.Frame(self, bg=BG1, padx=12, pady=6)
        self.row1.pack(side="top", fill="x")

        self.btn_start = tk.Button(self.row1, text="▶ Start", bg=BTN_START_BG, fg=BTN_START_FG,
                                   font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                   cursor="hand2", command=self._start_training)
        self.btn_start.pack(side="left", padx=4)

        self.btn_stop = tk.Button(self.row1, text="■ Stop", bg=BTN_BG, fg=BTN_STOP_FG,
                                  font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                  cursor="hand2", command=self.stop_all, state="disabled")
        self.btn_stop.pack(side="left", padx=4)

        self.btn_reset = tk.Button(self.row1, text="↺ Reset", bg=BTN_BG, fg=TEXT_SECONDARY,
                                   font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                   cursor="hand2", command=self.reset_all)
        self.btn_reset.pack(side="left", padx=4)

        sep = tk.Frame(self.row1, bg=BORDER_DARK, width=1, height=20)
        sep.pack(side="left", padx=8)

        self._check_vars = {}
        self._main_checkboxes = []  # Store checkbox widgets for sweep mode control
        for algo, color in [("A2C", COLOR_A2C), ("SAC", COLOR_SAC), ("TD3", COLOR_TD3)]:
            var = tk.BooleanVar(value=True)
            self._check_vars[algo] = var
            cb = tk.Checkbutton(self.row1, text=algo, variable=var, bg=BG1,
                               fg=TEXT_SECONDARY, selectcolor=BG3,
                               activebackground=BG1, activeforeground=TEXT_PRIMARY,
                               font=FONT_SMALL, cursor="hand2")
            cb.pack(side="left", padx=4)
            self._main_checkboxes.append(cb)

        # Connect sweep mode to main checkboxes
        self.hyperparam_panel.set_main_checkboxes(self._main_checkboxes)

        self.gpu_var = tk.BooleanVar(value=False)
        self.cb_gpu = tk.Checkbutton(self.row1, text="Use GPU", variable=self.gpu_var, bg=BG1,
                                     fg=TEXT_SECONDARY, selectcolor=BG3,
                                     activebackground=BG1, activeforeground=TEXT_PRIMARY,
                                     font=FONT_SMALL, cursor="hand2",
                                     command=self._toggle_gpu)
        self.cb_gpu.pack(side="left", padx=4)

        sep2 = tk.Frame(self.row1, bg=BORDER_DARK, width=1, height=20)
        sep2.pack(side="left", padx=8)

        self.btn_anim = tk.Button(self.row1, text="🎬 Show Animation", bg=BTN_BG, fg=BTN_ANIM_FG,
                                  font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                  cursor="hand2", command=self._toggle_animation_popup)
        self.btn_anim.pack(side="left", padx=4)

        self.btn_score = tk.Button(self.row1, text="📋 Score Table", bg=BTN_BG, fg=TEXT_SECONDARY,
                                   font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                   cursor="hand2", command=self._show_score_table)
        self.btn_score.pack(side="left", padx=4)

        self.btn_save = tk.Button(self.row1, text="💾 Save Plot", bg=BTN_BG, fg=TEXT_SECONDARY,
                                   font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                   cursor="hand2", command=self._save_plot)
        self.btn_save.pack(side="left", padx=4)

        self.btn_save_model = tk.Button(self.row1, text="💾 Save Model", bg=BTN_BG, fg=TEXT_SECONDARY,
                                        font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                        cursor="hand2", command=self._save_model)
        self.btn_save_model.pack(side="left", padx=4)

        self.btn_load_model = tk.Button(self.row1, text="📁 Load Model", bg=BTN_BG, fg=TEXT_SECONDARY,
                                        font=FONT_SMALL, relief="flat", bd=0, padx=10, pady=4,
                                        cursor="hand2", command=self._load_model)
        self.btn_load_model.pack(side="left", padx=4)

        self.row2 = tk.Frame(self, bg=BG1, padx=12, pady=4)
        self.row2.pack(side="top", fill="x")

        self._progress_bars = {}
        self._progress_labels = {}
        self._sweep_progress_rows = {}  # Track sweep run progress rows

        # Create default progress bars for main algorithms
        for algo in ["A2C", "SAC", "TD3"]:
            row = tk.Frame(self.row2, bg=BG1)
            row.pack(fill="x", pady=2)

            lbl = tk.Label(row, text=algo, width=4, bg=BG1, fg=TEXT_SECONDARY,
                          font=FONT_MONO_SM, anchor="w")
            lbl.pack(side="left")

            canvas = tk.Canvas(row, bg=BORDER_DARK, height=6, bd=0, highlightthickness=0)
            canvas.pack(side="left", fill="x", expand=True, padx=(4, 8))
            filled = canvas.create_rectangle(0, 0, 0, 6, fill={"A2C": COLOR_A2C, "SAC": COLOR_SAC, "TD3": COLOR_TD3}[algo], outline="")
            self._progress_bars[algo] = {"canvas": canvas, "filled": filled}

            info = tk.Label(row, text="ep 0/3000 · —", width=24, bg=BG1, fg=TEXT_TERTIARY,
                           font=FONT_MONO_SM, anchor="e")
            info.pack(side="left")
            self._progress_labels[algo] = info

        # Create status log section
        self.row3 = tk.Frame(self, bg=BG0, padx=12)
        self.row3.pack(side="bottom", fill="x")

        self.log = tk.Text(self.row3, height=3, bg=BG0, fg=TEXT_SECONDARY,
                          font=FONT_MONO_SM, state="disabled", relief="flat", bd=0,
                          wrap="none", exportselection=False)
        self.log.pack(side="left", fill="both", expand=True)

        scroll = tk.Scrollbar(self.row3, orient="vertical", command=self.log.yview)
        scroll.pack(side="right", fill="y")
        self.log.config(yscrollcommand=scroll.set)

        for tag, color in [("A2C", COLOR_A2C), ("SAC", COLOR_SAC), ("TD3", COLOR_TD3),
                           ("SYS", TEXT_TERTIARY), ("OK", TEXT_SUCCESS), ("ERR", TEXT_DANGER)]:
            self.log.tag_configure(tag, foreground=color)

        # Add initial status message
        self._log("SYS", "Ant-v5 RL Workbench ready. Select algorithms and click Start to begin training.")

    def _add_progress_row(self, run_label):
        """Dynamically add a progress row for sweep runs."""
        if run_label in self._progress_bars:
            return  # Already exists

        # Only add progress rows for sweep runs (those with underscores)
        if '_' not in run_label:
            return

        row = tk.Frame(self.row2, bg=BG1)
        row.pack(fill="x", pady=2)

        # Determine color based on base algorithm
        base_algo = run_label.split('_')[0]
        color = {"A2C": COLOR_A2C, "SAC": COLOR_SAC, "TD3": COLOR_TD3}.get(base_algo, TEXT_SECONDARY)

        lbl = tk.Label(row, text=run_label, width=12, bg=BG1, fg=TEXT_SECONDARY,
                      font=FONT_MONO_SM, anchor="w")
        lbl.pack(side="left")

        canvas = tk.Canvas(row, bg=BORDER_DARK, height=6, bd=0, highlightthickness=0)
        canvas.pack(side="left", fill="x", expand=True, padx=(4, 8))
        filled = canvas.create_rectangle(0, 0, 0, 6, fill=color, outline="")
        self._progress_bars[run_label] = {"canvas": canvas, "filled": filled}

        info = tk.Label(row, text="ep 0/3000 · —", width=24, bg=BG1, fg=TEXT_TERTIARY,
                       font=FONT_MONO_SM, anchor="e")
        info.pack(side="left")
        self._progress_labels[run_label] = info

        self._sweep_progress_rows[run_label] = row

    def _log(self, algo_tag: str, message: str):
        self.log.config(state="normal")
        self.log.insert("end", f"[{algo_tag}] ", algo_tag)
        self.log.insert("end", message + "\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def _start_training(self):
        if self._training_active:
            return

        self._training_active = True
        self.header_bar.set_status("training", "running")
        self.btn_start.config(text="▶ Running…", state="disabled")
        self.btn_stop.config(state="normal")

        device = get_device(self.gpu_var.get())

        wrappers = {}  # Initialize to prevent UnboundLocalError

        # Check if sweep mode is enabled
        sweep_config = self.hyperparam_panel.get_sweep_config()

        env_kwargs = {
            "ctrl_cost_weight": 0.5,
            "contact_cost_weight": 5e-4,
            "healthy_reward": 1.0,
            "terminate_when_unhealthy": True,
        }

        if sweep_config:
            # Sweep mode: create multiple parameter sets
            self._run_sweep_training(sweep_config, device)
        else:
            # Normal mode: one instance per selected algorithm
            wrappers = {"A2C": A2CWrapper(), "SAC": SACWrapper(), "TD3": TD3Wrapper()}

            for algo in ["A2C", "SAC", "TD3"]:
                if self._check_vars[algo].get():
                    self._stop_events[algo] = threading.Event()
                    params = self.hyperparam_panel.get_params(algo)
                    env = gym.make("Ant-v5", **env_kwargs, render_mode=None)
                    wrappers[algo].build(env, params, device)
                    self._current_agents[algo] = wrappers[algo]

                    t = threading.Thread(target=train_agent, args=(self.data_queue, self._stop_events[algo], algo, wrappers[algo], params))
                    self._threads[algo] = t
                    t.start()

    def _run_sweep_training(self, sweep_config, device):
        """Run sweep training with multiple parameter values in parallel."""
        algo = sweep_config["algorithm"]
        param_name = sweep_config["parameter"]
        param_values = sweep_config["values"]

        # Get base parameters for the algorithm
        base_params = self.hyperparam_panel.get_params(algo)

        env_kwargs = {
            "ctrl_cost_weight": 0.5,
            "contact_cost_weight": 5e-4,
            "healthy_reward": 1.0,
            "terminate_when_unhealthy": True,
        }

        # Create wrapper instances and threads for each parameter value
        for i, value in enumerate(param_values):
            # Create unique label for this sweep run
            sweep_label = f"{algo}_{param_name}={value}"

            # Create new parameter set with the sweep parameter
            sweep_params = base_params.copy()
            sweep_params[param_name] = value

            # Create wrapper instance
            if algo == "A2C":
                wrapper = A2CWrapper()
            elif algo == "SAC":
                wrapper = SACWrapper()
            elif algo == "TD3":
                wrapper = TD3Wrapper()

            env = gym.make("Ant-v5", **env_kwargs, render_mode=None)
            wrapper.build(env, sweep_params, device)

            # Create stop event for this sweep run
            self._stop_events[sweep_label] = threading.Event()
            self._current_agents[sweep_label] = wrapper

            t = threading.Thread(target=train_agent, args=(self.data_queue, self._stop_events[sweep_label], sweep_label, wrapper, sweep_params))
            self._threads[sweep_label] = t
            t.start()

    def _set_agent_on_animation(self, algo_label):
        """Set the agent on the animation window if it's open."""
        print(f"TrainingPanel: Attempting to set agent for {algo_label}")
        print(f"TrainingPanel: Animation exists: {self._animation is not None}")
        if self._animation:
            try:
                exists = self._animation.winfo_exists()
            except Exception as e:
                print(f"TrainingPanel: Error checking popup: {e}")
                exists = False
        print(f"TrainingPanel: Agent available: {algo_label in self._current_agents}")

        if self._animation and self._animation.winfo_exists() and algo_label in self._current_agents:
            agent = self._current_agents[algo_label]
            print(f"TrainingPanel: Setting agent {type(agent)} on animation window")
            try:
                self._animation.set_agent(agent, algo_label)
                print("TrainingPanel: Agent set successfully")
            except Exception as e:
                print(f"TrainingPanel: Failed to set agent: {e}")
        else:
            print("TrainingPanel: Cannot set agent - animation window not ready or agent not available")

    def _run_sweep_training_single(self, wrapper, params, algo_label):
        env = gym.make(
            "Ant-v5",
            ctrl_cost_weight=0.5,
            contact_cost_weight=5e-4,
            healthy_reward=1.0,
            terminate_when_unhealthy=True,
            render_mode=None,
        )

        device = get_device(self.gpu_var.get())
        if self._current_agents.get(algo_label) and self._current_agents[algo_label].model:
            wrapper = self._current_agents[algo_label]
        else:
            wrapper.build(env, params, device)

        # Notify main thread to attach the built agent to the animation window (if open)
        self.master.after(0, self._set_agent_on_animation, algo_label)

        logger = RunLogger(algo=algo_label)

        # Calculate target episodes for this sweep run
        target_episodes = params["total_timesteps"] // params["max_episode_steps"]

        # For A2C with parallel envs, adjust total_eps for display and rely on total_timesteps limit
        if algo_label.startswith("A2C"):
            n_envs = params.get("n_envs", 1)
            total_eps = target_episodes * n_envs
            max_episodes = None
        else:
            total_eps = target_episodes
            max_episodes = target_episodes

        callback = EpisodeCallback(
            algo_label=algo_label,
            data_queue=self.data_queue,
            stop_event=self._stop_events[algo_label],
            logger=logger,
            max_episodes=max_episodes,
            total_eps=total_eps,
        )

        try:
            # For sweep runs, we still use total_timesteps but the callback will stop at target episodes
            wrapper.model.learn(
                total_timesteps=params["total_timesteps"],  # Keep high to avoid early stopping
                callback=callback,
                reset_num_timesteps=True,
            )
        except Exception as exc:
            import traceback
            error_msg = f"{exc}\n{traceback.format_exc()}"
            self.data_queue.put({"algo": algo_label, "_error": error_msg})
        finally:
            env.close()
            self.data_queue.put({"algo": algo_label, "_done": True})

    def _run_training(self, wrapper, params, algo_label):
        env = gym.make(
            "Ant-v5",
            ctrl_cost_weight=0.5,
            contact_cost_weight=5e-4,
            healthy_reward=1.0,
            terminate_when_unhealthy=True,
            render_mode=None,
        )

        device = get_device(self.gpu_var.get())
        if self._current_agents.get(algo_label) and self._current_agents[algo_label].model:
            wrapper = self._current_agents[algo_label]
            # Assume the loaded model is compatible with the env
        else:
            wrapper.build(env, params, device)

        # Notify main thread to attach the built agent to the animation window (if open)
        self.master.after(0, self._set_agent_on_animation, algo_label)

        logger = RunLogger(algo=algo_label)
        # Calculate target episodes: total_timesteps / max_episode_steps
        target_episodes = params["total_timesteps"] // params["max_episode_steps"]

        # For A2C with parallel envs, adjust total_eps for display and rely on total_timesteps limit
        if algo_label.startswith("A2C"):
            n_envs = params.get("n_envs", 1)
            total_eps = target_episodes * n_envs
            max_episodes = None
        else:
            total_eps = target_episodes
            max_episodes = target_episodes

        callback = EpisodeCallback(
            algo_label=algo_label,
            data_queue=self.data_queue,
            stop_event=self._stop_events[algo_label],
            logger=logger,
            max_episodes=max_episodes,
            total_eps=total_eps,
        )

        try:
            wrapper.model.learn(
                total_timesteps=params["total_timesteps"],
                callback=callback,
                reset_num_timesteps=True,
            )
        except Exception as exc:
            import traceback
            error_msg = f"{exc}\n{traceback.format_exc()}"
            self.data_queue.put({"algo": algo_label, "_error": error_msg})
        finally:
            env.close()
            self.data_queue.put({"algo": algo_label, "_done": True})

    def stop_all(self):
        for run_label, evt in self._stop_events.items():
            evt.set()
        for run_label, thr in self._threads.items():
            if thr.is_alive():
                thr.join(timeout=1.0)  # Wait for thread to finish
        self._training_active = False
        self.header_bar.set_status("stopped", "stopped")
        self.btn_start.config(text="▶ Start", state="normal")
        self.btn_stop.config(state="disabled")

    def reset_all(self):
        self.stop_all()
        self._threads.clear()
        self.comparison_panel.clear_all()

        # Clear all progress bars and remove sweep ones
        for run_label in list(self._progress_bars.keys()):
            if run_label in self._sweep_progress_rows:
                self._sweep_progress_rows[run_label].destroy()
                del self._sweep_progress_rows[run_label]
                del self._progress_bars[run_label]
                del self._progress_labels[run_label]
            else:
                # Reset main algorithm progress bars
                canvas = self._progress_bars[run_label]["canvas"]
                canvas.coords(self._progress_bars[run_label]["filled"], 0, 0, 0, 6)
                self._progress_labels[run_label].config(text="ep 0/3000 · —")

        self.log.config(state="normal")
        self.log.delete("1.0", "end")
        self.log.config(state="disabled")
        self._log("SYS", "workbench reset. ready to train.")

    def on_data(self, item: dict):
        algo = item.get("algo", "")

        if "_error" in item:
            self._log("ERR", f"{algo} error: {item['_error']}")
            return

        if "_done" in item:
            self._log(algo, "training complete.")
            return

        # Ensure progress bar exists for this run
        if algo not in self._progress_bars:
            self._add_progress_row(algo)

        ep = item.get("episode", 0)
        total_ep = item.get("total_eps", 3000)
        reward = item.get("reward", 0)
        avg100 = item.get("rolling_100", 0)
        solved = item.get("solved_at") is not None

        if ep % 10 == 0:
            suffix = "  ✓ solved" if solved else ""
            self._log(algo, f"ep {ep:>5} | step {item.get('steps',0):>8,} | reward {reward:>8,.1f} | avg(100) {avg100:>8,.1f}{suffix}")

        self._update_progress(algo, ep, total_ep, reward)

    def _update_progress(self, algo, ep, total_ep, reward):
        fraction = min(1.0, ep / max(1, total_ep))
        canvas_info = self._progress_bars[algo]
        w = canvas_info["canvas"].winfo_width()
        if w > 0:
            canvas_info["canvas"].coords(canvas_info["filled"], 0, 0, int(w * fraction), 6)
        self._progress_labels[algo].config(text=f"ep {ep}/{total_ep} · {int(reward):,}")

    def _toggle_animation_popup(self):
        print(f"TrainingPanel: _toggle_animation_popup called. Current animation: {self._animation}")
        if self._animation and self._animation.winfo_exists():
            print("TrainingPanel: Closing existing animation window")
            self._animation._close()
            self._animation = None
            self.btn_anim.config(text="🎬 Show Animation", fg=BTN_ANIM_FG)
        else:
            print("TrainingPanel: Creating new animation window")
            # Find any currently running training (including sweep runs)
            current_algo = None
            for label in self._stop_events:
                if not self._stop_events[label].is_set():
                    current_algo = label
                    break

            print(f"TrainingPanel: Creating animation with algo_label={current_algo or 'SAC'}")
            try:
                self._animation = AnimationWindow(self.master, root=self.master)
                print(f"TrainingPanel: Animation window created successfully: {self._animation}")
                # If training is already active for this algorithm, set the agent immediately
                if current_algo and current_algo in self._current_agents:
                    print(f"TrainingPanel: Training already active - setting agent immediately")
                    self._animation.set_agent(self._current_agents[current_algo], current_algo)
            except Exception as e:
                traceback.print_exc()
                print(f"TrainingPanel: Failed to create animation window: {e}")
                self._animation = None
                self.btn_anim.config(text="🎬 Show Animation", fg=BTN_ANIM_FG)
                return
            self.btn_anim.config(text="🔴 Hide Animation", fg=BTN_STOP_FG)

    def _show_score_table(self):
        popup = tk.Toplevel(self)
        popup.title("Score Comparison")
        popup.geometry("560x260")
        popup.resizable(False, False)

        tk.Label(popup, text="Score Comparison", bg=BG1, fg=TEXT_SECONDARY,
                font=FONT_SMALL).pack(fill="x")

        # Header
        header = tk.Frame(popup, bg=BG0)
        header.pack(fill="x")
        for txt in ["Run", "Mean (100)", "Std", "Max", "Solved"]:
            tk.Label(header, text=txt, width=12, bg=BG0, fg=TEXT_TERTIARY,
                    font=FONT_MONO_SM).pack(side="left", padx=2)

        # Data rows
        score_data = self.comparison_panel.get_score_data()
        for i, data in enumerate(score_data):
            row_bg = BG1 if i % 2 == 0 else BG2
            row = tk.Frame(popup, bg=row_bg)
            row.pack(fill="x")

            # Algorithm name (with accent color)
            algo_colors = {"A2C": COLOR_A2C, "SAC": COLOR_SAC, "TD3": COLOR_TD3}
            algo_fg = algo_colors.get(data["run"], TEXT_SECONDARY)
            tk.Label(row, text=data["run"], width=12, bg=row_bg, fg=algo_fg,
                    font=FONT_MONO_SM).pack(side="left", padx=2)

            # Mean (100-episode rolling average)
            tk.Label(row, text=f"{data['mean_100']:,.1f}", width=12, bg=row_bg, fg=TEXT_SECONDARY,
                    font=FONT_MONO_SM).pack(side="left", padx=2)

            # Standard deviation
            tk.Label(row, text=f"{data['std_100']:,.1f}", width=12, bg=row_bg, fg=TEXT_SECONDARY,
                    font=FONT_MONO_SM).pack(side="left", padx=2)

            # Maximum reward
            tk.Label(row, text=f"{data['max']:,.1f}", width=12, bg=row_bg, fg=TEXT_SECONDARY,
                    font=FONT_MONO_SM).pack(side="left", padx=2)

            # Solved episode (or "—" if not solved)
            solved_text = f"ep {data['solved_at']}" if data.get("solved_at") else "—"
            solved_fg = TEXT_SUCCESS if data.get("solved_at") else TEXT_TERTIARY
            tk.Label(row, text=solved_text, width=12, bg=row_bg, fg=solved_fg,
                    font=FONT_MONO_SM).pack(side="left", padx=2)

        # Close button
        tk.Button(popup, text="Close", bg=BG2, fg=TEXT_SECONDARY,
                 font=FONT_SMALL, relief="flat", command=popup.destroy,
                 padx=12, pady=5).pack(pady=10)

    def _save_plot(self):
        path = self.comparison_panel.save_plot()
        self._log("SYS", f"plot saved → {path}")

    def _save_model(self):
        if not self._current_agents:
            self._log("SYS", "No trained models to save.")
            return
        import os
        os.makedirs("models", exist_ok=True)
        for algo, agent in self._current_agents.items():
            if agent and agent.model:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                path = f"models/{algo}_{ts}.zip"
                agent.model.save(path)
                self._log("SYS", f"Saved {algo} model to {path}")

    def _load_model(self):
        file_path = fd.askopenfilename(initialdir="models", title="Select model file",
                                       filetypes=(("ZIP files", "*.zip"), ("all files", "*.*")))
        if not file_path:
            return
        import os
        filename = os.path.basename(file_path)
        algo = filename.split('_')[0]
        if algo not in ["A2C", "SAC", "TD3"]:
            self._log("ERR", "Invalid model file or unrecognized algorithm.")
            return
        # Load the model
        wrapper_class = globals()[f"{algo}Wrapper"]
        wrapper = wrapper_class()
        wrapper.model = wrapper.model_class.load(file_path)
        self._current_agents[algo] = wrapper
        self._log("SYS", f"Loaded {algo} model from {file_path}")

    def _toggle_gpu(self):
        if self.gpu_var.get():
            import torch
            if torch.cuda.is_available():
                self.header_bar.set_device("CUDA")
            else:
                messagebox.showwarning("GPU Unavailable", "CUDA is not available on this system.")
                self.gpu_var.set(False)
        else:
            self.header_bar.set_device("CPU")