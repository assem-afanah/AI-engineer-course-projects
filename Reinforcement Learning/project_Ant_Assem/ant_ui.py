import tkinter as tk
import queue
from gui.hyperparameter_panel import HyperparamPanel, BG0, BG1, BG2, BG3, BORDER_DARK, BORDER_MID, TEXT_PRIMARY, TEXT_SECONDARY, TEXT_TERTIARY, FONT_HEADER, FONT_MONO_SM, FONT_SMALL, PILL_IDLE_BG, PILL_IDLE_FG, PILL_RUNNING_BG, PILL_RUNNING_FG
from gui.comparison_panel import ComparisonPanel
from gui.training_panel import TrainingPanel

class HeaderBar(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg=BG1, height=40)
        self.pack_propagate(False)

        self.status_pill_frame = tk.Frame(self, bg=BG3, padx=1, pady=1)
        self.status_pill_frame.pack(side="left", padx=16)

        self.status_pill = tk.Label(self.status_pill_frame, text="● idle",
                                     bg=PILL_IDLE_BG, fg=PILL_IDLE_FG,
                                     padx=8, pady=2, relief="flat", font=FONT_MONO_SM, bd=0)
        self.status_pill.pack()

        tk.Label(self, text="Ant-v5 RL Workbench", bg=BG1, fg=TEXT_PRIMARY,
                font=FONT_HEADER).pack(side="left", padx=8)

        self.right_tags = tk.Frame(self, bg=BG1)
        self.right_tags.pack(side="right", padx=16)

        for txt in ["obs: 27", "act: 8"]:
            tk.Label(self.right_tags, text=txt, bg=BG3, fg=TEXT_TERTIARY,
                    font=FONT_MONO_SM, padx=6, pady=2, relief="flat").pack(side="left", padx=2)

        self.device_tag = tk.Label(self.right_tags, text="CPU", bg=BG3, fg=TEXT_TERTIARY,
                                   font=FONT_MONO_SM, padx=6, pady=2, relief="flat")
        self.device_tag.pack(side="left", padx=2)

        sep = tk.Frame(self, bg=BORDER_DARK, height=1)
        sep.pack(side="bottom", fill="x")

    def set_status(self, text, style):
        if style == "idle":
            self.status_pill.config(text="● idle", bg=PILL_IDLE_BG, fg=PILL_IDLE_FG)
        elif style == "running":
            self.status_pill.config(text="● training", bg=PILL_RUNNING_BG, fg=PILL_RUNNING_FG)
        elif style == "stopped":
            self.status_pill.config(text="○ stopped", bg=PILL_IDLE_BG, fg=PILL_IDLE_FG)

    def set_device(self, label):
        self.device_tag.config(text=label)


class AntUI(tk.Frame):
    def __init__(self, master):
        super().__init__(master, bg=BG0)

        self.header = HeaderBar(self)
        self.header.pack(side="top", fill="x")

        self.middle = tk.Frame(self, bg=BG0)
        self.middle.pack(side="top", fill="both", expand=True)

        self.data_queue = queue.Queue()

        self.hyperparam_panel = HyperparamPanel(self.middle)
        self.hyperparam_panel.pack(side="left", fill="y")

        sep = tk.Frame(self.middle, bg=BORDER_DARK, width=1)
        sep.pack(side="left", fill="y")

        self.comparison_panel = ComparisonPanel(self.middle, self.data_queue)
        self.comparison_panel.pack(side="left", fill="both", expand=True)

        top_sep = tk.Frame(self, bg=BORDER_DARK, height=1)
        top_sep.pack(side="top", fill="x")

        self.training_panel = TrainingPanel(
            self,
            hyperparam_panel=self.hyperparam_panel,
            comparison_panel=self.comparison_panel,
            data_queue=self.data_queue,
            header_bar=self.header,
        )
        self.training_panel.pack(side="bottom", fill="x")

        self._poll()

    def _poll(self):
        try:
            while True:
                item = self.data_queue.get_nowait()
                self.comparison_panel.on_data(item)
                self.training_panel.on_data(item)
                # Also send to animation window if it exists
                if hasattr(self.training_panel, '_animation') and self.training_panel._animation:
                    self.training_panel._animation.on_training_data(item)
        except queue.Empty:
            pass
        self.after(200, self._poll)