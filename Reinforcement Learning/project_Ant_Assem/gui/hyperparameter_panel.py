import tkinter as tk
from tkinter import ttk
import queue

BG0  = "#0E0E10"
BG1  = "#1C1C1E"
BG2  = "#28282C"
BG3  = "#38383C"
BORDER_DARK   = "#3A3A3C"
BORDER_MID    = "#48484A"
BORDER_LIGHT  = "#636366"
TEXT_PRIMARY   = "#FFFFFF"
TEXT_SECONDARY = "#E0E0E0"
TEXT_TERTIARY  = "#A0A0A0"
TEXT_DANGER    = "#FF453A"
TEXT_INFO      = "#0A84FF"
TEXT_SUCCESS   = "#30D158"
FONT_HEADER  = ("Helvetica", 13, "bold")
FONT_LABEL   = ("Helvetica", 11)
FONT_SMALL   = ("Helvetica", 10)
FONT_MONO    = ("Courier New", 10)
FONT_MONO_SM = ("Courier New", 9)
FONT_SECTION = ("Helvetica", 9, "bold")
PILL_RUNNING_BG = "#1B3A1C"
PILL_RUNNING_FG = "#4CAF50"
PILL_IDLE_BG = "#28282C"
PILL_IDLE_FG = "#636366"

A2C_DEFAULTS = {
    "learning_rate": 3e-4,
    "n_steps": 2048,
    "gamma": 0.99,
    "gae_lambda": 0.94,
    "ent_coef": 0.0,
    "vf_coef": 0.5,
    "max_grad_norm": 0.5,
    "hidden_size": 256,
    "n_hidden_layers": 2,
    "total_timesteps": 3000000,
    "max_episode_steps": 1000,
    "n_envs": 1,
    "use_sde": True,
    "sde_sample_freq": 4,
}

SAC_DEFAULTS = {
    "learning_rate": 3e-4,
    "buffer_size": 1000000,
    "learning_starts": 10000,
    "batch_size": 256,
    "tau": 0.005,
    "gamma": 0.99,
    "train_freq": 1,
    "gradient_steps": 1,
    "ent_coef": "auto",
    "target_entropy": "auto",
    "hidden_size": 256,
    "n_hidden_layers": 2,
    "total_timesteps": 3000000,
    "max_episode_steps": 1000,
}

TD3_DEFAULTS = {
    "learning_rate": 3e-4,
    "buffer_size": 1000000,
    "learning_starts": 10000,
    "batch_size": 256,
    "tau": 0.005,
    "gamma": 0.99,
    "train_freq": 1,
    "gradient_steps": 1,
    "policy_delay": 2,
    "target_noise": 0.2,
    "noise_clip": 0.5,
    "exploration_noise": 0.1,
    "hidden_size": 256,
    "n_hidden_layers": 2,
    "total_timesteps": 3000000,
    "max_episode_steps": 1000,
}

A2C_SECTIONS = [
    ("NETWORK", ["learning_rate", "gamma", "gae_lambda", "ent_coef", "vf_coef", "max_grad_norm", "use_sde", "sde_sample_freq"]),
    ("ARCHITECTURE", ["hidden_size", "n_hidden_layers"]),
    ("TRAINING", ["total_timesteps", "max_episode_steps", "n_steps", "n_envs"]),
]

SAC_SECTIONS = [
    ("CORE", ["learning_rate", "gamma", "tau", "ent_coef", "target_entropy"]),
    ("BUFFER", ["buffer_size", "learning_starts", "batch_size"]),
    ("UPDATE", ["train_freq", "gradient_steps"]),
    ("ARCHITECTURE", ["hidden_size", "n_hidden_layers"]),
    ("TRAINING", ["total_timesteps", "max_episode_steps"]),
]

TD3_SECTIONS = [
    ("CORE", ["learning_rate", "gamma", "tau"]),
    ("BUFFER", ["buffer_size", "learning_starts", "batch_size"]),
    ("UPDATE", ["train_freq", "gradient_steps", "policy_delay"]),
    ("NOISE", ["target_noise", "noise_clip", "exploration_noise"]),
    ("ARCHITECTURE", ["hidden_size", "n_hidden_layers"]),
    ("TRAINING", ["total_timesteps", "max_episode_steps"]),
]


class HyperparamPanel(tk.Frame):
    def __init__(self, master):
        super().__init__(master, width=270)
        self.pack_propagate(False)
        self.configure(bg=BG1)

        self._entries = {}
        self._validation_errors = {}
        self._algo_defaults = {"A2C": A2C_DEFAULTS, "SAC": SAC_DEFAULTS, "TD3": TD3_DEFAULTS}
        self._algo_sections = {"A2C": A2C_SECTIONS, "SAC": SAC_SECTIONS, "TD3": TD3_SECTIONS}

        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "Dark.TNotebook",
            background=BG0,
            borderwidth=0,
            tabmargins=[0, 0, 0, 0],
        )
        style.configure(
            "Dark.TNotebook.Tab",
            background=BG0,
            foreground=TEXT_SECONDARY,
            font=FONT_SMALL,
            padding=[10, 5],
            borderwidth=0,
        )
        style.map(
            "Dark.TNotebook.Tab",
            background=[("selected", BG1)],
            foreground=[("selected", TEXT_PRIMARY)],
        )

        self.notebook = ttk.Notebook(self, style="Dark.TNotebook")
        self.notebook.pack(fill="both", expand=True)

        self._create_algos_tabs()
        self._create_sweep_tab()

    def _create_algos_tabs(self):
        for algo in ["A2C", "SAC", "TD3"]:
            frame = tk.Frame(self.notebook, bg=BG1)
            self._create_param_tab(frame, algo, self._algo_defaults[algo], self._algo_sections[algo])
            self.notebook.add(frame, text=algo)

    def _create_param_tab(self, parent, algo, defaults, sections):
        canvas = tk.Canvas(parent, bg=BG1, highlightthickness=0)
        scrollbar = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scroll_frame = tk.Frame(canvas, bg=BG1)

        scroll_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # Mouse wheel scrolling support
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def _bind_to_canvas(widget):
            widget.bind("<MouseWheel>", _on_mousewheel)  # Windows
            widget.bind("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))  # Linux scroll up
            widget.bind("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))  # Linux scroll down

        _bind_to_canvas(canvas)
        _bind_to_canvas(scroll_frame)

        for sec_name, params in sections:
            sec_label = tk.Label(scroll_frame, text=sec_name.upper(), bg=BG1, fg=TEXT_TERTIARY,
                                font=FONT_SECTION, anchor="w")
            sec_label.pack(fill="x", padx=8, pady=6)

            for param in params:
                row = tk.Frame(scroll_frame, bg=BG1)
                row.pack(fill="x", padx=8, pady=2)

                lbl = tk.Label(row, text=param, bg=BG1, fg=TEXT_SECONDARY,
                              font=FONT_MONO_SM, width=16, anchor="w")
                lbl.pack(side="left")

                entry_frame = tk.Frame(row, bg=BORDER_MID, padx=1, pady=1)
                entry_frame.pack(side="right")

                entry = tk.Entry(entry_frame, font=FONT_MONO_SM, bg=BG2, fg=TEXT_PRIMARY,
                               insertbackground=TEXT_PRIMARY, relief="flat", bd=0, width=10)
                entry.pack()
                entry.insert(0, str(defaults.get(param, "")))
                entry.bind("<FocusOut>", lambda e, p=param, en=entry: self._validate_field(p, en))
                entry.bind("<Return>", lambda e, p=param, en=entry: self._validate_field(p, en))

                self._entries[param] = entry

        reset_btn = tk.Button(scroll_frame, text="Reset to Defaults", bg=BG2, fg=TEXT_SECONDARY,
                             font=FONT_SMALL, relief="flat", activebackground=BG3, cursor="hand2")
        reset_btn.pack(fill="x", padx=8, pady=10)
        reset_btn.configure(command=lambda d=defaults: self._reset_defaults(d))

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def _create_sweep_tab(self):
        canvas = tk.Canvas(self.notebook, bg=BG1, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self.notebook, orient="vertical", command=canvas.yview)
        frame = tk.Frame(canvas, bg=BG1)

        frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        # Mouse wheel support
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def _bind_to_canvas(widget):
            widget.bind("<MouseWheel>", _on_mousewheel)  # Windows
            widget.bind("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))  # Linux scroll up
            widget.bind("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))  # Linux scroll down

        _bind_to_canvas(canvas)
        _bind_to_canvas(frame)
        tk.Label(frame, text="SWEEP CONFIGURATION", bg=BG1, fg=TEXT_TERTIARY,
                font=FONT_SECTION).pack(anchor="w", padx=8, pady=6)

        algo_frame = tk.Frame(frame, bg=BG1)
        algo_frame.pack(fill="x", padx=8)
        tk.Label(algo_frame, text="Algorithm", bg=BG1, fg=TEXT_SECONDARY, font=FONT_MONO_SM).pack(anchor="w")
        # Algorithm to parameter mapping
        algo_params = {
            "A2C": ["learning_rate", "n_steps", "gamma", "gae_lambda", "ent_coef", "vf_coef", "max_grad_norm", "hidden_size", "n_hidden_layers", "total_timesteps", "max_episode_steps", "n_envs", "use_sde"],
            "SAC": ["learning_rate", "buffer_size", "learning_starts", "batch_size", "tau", "gamma", "train_freq", "gradient_steps", "ent_coef", "target_entropy", "hidden_size", "n_hidden_layers", "total_timesteps", "max_episode_steps"],
            "TD3": ["learning_rate", "buffer_size", "learning_starts", "batch_size", "tau", "gamma", "train_freq", "gradient_steps", "policy_delay", "target_noise", "noise_clip", "exploration_noise", "hidden_size", "n_hidden_layers", "total_timesteps", "max_episode_steps"]
        }

        algo_cb = ttk.Combobox(algo_frame, values=["A2C", "SAC", "TD3"], state="readonly")
        algo_cb.pack(fill="x", pady=2)
        algo_cb.set("SAC")

        param_frame = tk.Frame(frame, bg=BG1)
        param_frame.pack(fill="x", padx=8)
        tk.Label(param_frame, text="Parameter", bg=BG1, fg=TEXT_SECONDARY, font=FONT_MONO_SM).pack(anchor="w")
        param_cb = ttk.Combobox(param_frame, values=algo_params["SAC"], state="readonly")
        param_cb.pack(fill="x", pady=2)

        # Function to update parameter combobox when algorithm changes
        def on_algorithm_change(event=None):
            selected_algo = algo_cb.get()
            if selected_algo in algo_params:
                param_cb['values'] = algo_params[selected_algo]
                param_cb.set(algo_params[selected_algo][0])  # Set first parameter as default

        algo_cb.bind("<<ComboboxSelected>>", on_algorithm_change)

        values_frame = tk.Frame(frame, bg=BG1)
        values_frame.pack(fill="x", padx=8)
        tk.Label(values_frame, text="Values", bg=BG1, fg=TEXT_SECONDARY, font=FONT_MONO_SM).pack(anchor="w")
        values_entry = tk.Entry(values_frame, font=FONT_MONO_SM, bg=BG2, fg=TEXT_PRIMARY, relief="flat", bd=0)
        values_entry.pack(fill="x", pady=2)
        values_entry.insert(0, "1e-4,3e-4,1e-3")

        sweep_var = tk.BooleanVar()
        sweep_cb = tk.Checkbutton(frame, text="Enable Sweep", variable=sweep_var, bg=BG1,
                                 fg=TEXT_SECONDARY, selectcolor=BG3, activebackground=BG1,
                                 command=self._toggle_sweep_mode)
        sweep_cb.pack(anchor="w", padx=8, pady=6)

        tk.Label(frame, text="SWEEP STATUS", bg=BG1, fg=TEXT_TERTIARY,
                font=FONT_SECTION).pack(anchor="w", padx=8, pady=6)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.notebook.add(canvas, text="Sweep")

        # Store references for sweep configuration access
        self._sweep_algo_cb = algo_cb
        self._sweep_param_cb = param_cb
        self._sweep_values_entry = values_entry
        self._sweep_var = sweep_var

        # Reference to main algorithm checkboxes (will be set by parent)
        self._main_algo_checkboxes = None

    def _validate_field(self, param, entry):
        raw = entry.get()
        try:
            if param in ["learning_rate", "gamma", "gae_lambda", "tau", "ent_coef", "exploration_noise",
                        "target_noise", "noise_clip", "vf_coef", "max_grad_norm"]:
                val = float(raw)
                if param == "gamma" and not (0 < val < 1):
                    raise ValueError()
                if param == "tau" and not (0 < val <= 1):
                    raise ValueError()
                if param == "learning_rate" and not (0 < val <= 1):
                    raise ValueError()
            else:
                val = int(raw)
                if param in ["hidden_size", "n_hidden_layers", "n_steps", "batch_size",
                            "buffer_size", "learning_starts", "total_timesteps", "max_episode_steps", "n_envs"]:
                    if val < 1:
                        raise ValueError()
            entry.configure(bg=BG2)
            self._validation_errors[param] = False
            return True
        except ValueError:
            entry.configure(bg="#3A0A0A")
            self._validation_errors[param] = True
            return False

    def _reset_defaults(self, defaults):
        for param, val in defaults.items():
            if param in self._entries:
                self._entries[param].delete(0, tk.END)
                self._entries[param].insert(0, str(val))

    def get_params(self, algo: str) -> dict:
        params = {}
        defaults = self._algo_defaults.get(algo, {})
        for param, default_val in defaults.items():
            if param in self._entries:
                raw = self._entries[param].get()
                try:
                    if isinstance(default_val, float):
                        params[param] = float(raw) if raw else default_val
                    elif isinstance(default_val, bool):
                        params[param] = raw.lower() in ["true", "1"] if raw else default_val
                    else:
                        params[param] = int(raw) if raw else default_val
                except (ValueError, AttributeError):
                    params[param] = default_val
            else:
                params[param] = default_val
        return params

    def get_sweep_config(self) -> dict:
        """Get sweep configuration if enabled."""
        if not self._sweep_var.get():
            return None

        try:
            values_str = self._sweep_values_entry.get().strip()
            if not values_str:
                return None

            # Parse comma-separated values
            values = []
            for val in values_str.split(','):
                val = val.strip()
                if val:
                    # Try to parse as number, keep as string if not
                    try:
                        # Check if it's a scientific notation or float
                        if 'e' in val.lower() or '.' in val:
                            values.append(float(val))
                        else:
                            values.append(int(val))
                    except ValueError:
                        values.append(val)

            return {
                "algorithm": self._sweep_algo_cb.get(),
                "parameter": self._sweep_param_cb.get(),
                "values": values
            }
        except Exception:
            return None

    def _toggle_sweep_mode(self):
        """Enable/disable main algorithm checkboxes when sweep mode changes."""
        if self._main_algo_checkboxes:
            enabled = not self._sweep_var.get()
            for cb in self._main_algo_checkboxes:
                cb.config(state="normal" if enabled else "disabled")

    def set_main_checkboxes(self, checkboxes):
        """Set reference to main algorithm checkboxes for enabling/disabling."""
        self._main_algo_checkboxes = checkboxes
        # Initially disable if sweep is enabled
        if self._sweep_var.get():
            for cb in checkboxes:
                cb.config(state="disabled")