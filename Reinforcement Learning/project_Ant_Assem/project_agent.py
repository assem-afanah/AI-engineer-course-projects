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
    root.geometry("1280x760")
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