"""
gui/animation_window.py
=======================
Live MuJoCo simulation popup for the Ant-v5 RL Workbench.

Architecture
------------
  Main thread   : builds the Toplevel, drives the Tkinter event loop,
                  polls _frame_queue at 30 Hz, updates Canvas + stat tiles.
  Render thread : owns its own gymnasium env (render_mode="rgb_array"),
                  steps the env using the current agent, pushes raw numpy
                  frames onto _frame_queue.  Never touches Tkinter directly.

Thread-safety rules
-------------------
  * env.step() / env.render() — render thread only
  * canvas / label updates    — main thread only (via after() callbacks)
  * _agent access             — protected by _agent_lock (held briefly)
  * _frame_queue              — maxsize=2; put_nowait drops on full so the
                                render thread never blocks on a slow GUI

Dependencies
------------
  pip install Pillow gymnasium[mujoco] numpy
"""

from __future__ import annotations

import queue
import threading
import time
import tkinter as tk
from tkinter import ttk
from typing import Optional

import numpy as np

try:
    from PIL import Image, ImageTk
except ImportError as exc:
    raise ImportError(
        "Pillow is required for the animation window.\n"
        "Install it with:  pip install Pillow"
    ) from exc

# ── Colour palette (mirrors ant_ui.py constants) ──────────────────────────────
# If ant_ui.py is on sys.path you can replace these with:
#   from ant_ui import BG0, BG1, BG2, ...
BG0            = "#0E0E10"
BG1            = "#1C1C1E"
BG2            = "#28282C"
BG3            = "#38383C"
BORDER_DARK    = "#3A3A3C"
BORDER_MID     = "#48484A"
TEXT_PRIMARY   = "#F2F2F7"
TEXT_SECONDARY = "#AEAEB2"
TEXT_TERTIARY  = "#636366"
TEXT_DANGER    = "#FF453A"
TEXT_SUCCESS   = "#30D158"
TEXT_INFO      = "#0A84FF"
COLOR_A2C      = "#4CAF50"
COLOR_SAC      = "#2196F3"
COLOR_TD3      = "#FF5722"

FONT_SMALL   = ("Helvetica", 10)
FONT_MONO_SM = ("Courier New", 9)
FONT_BOLD_12 = ("Courier New", 12, "bold")

# ── Render constants ──────────────────────────────────────────────────────────
FRAME_INTERVAL  = 1.0 / 30.0   # target ≤ 30 FPS
QUEUE_MAXSIZE   = 2             # drop frames rather than build a backlog
SHUTDOWN_POLLS  = 30            # 30 × 100 ms = 3 s max shutdown wait
ALGO_COLORS     = {"A2C": COLOR_A2C, "SAC": COLOR_SAC, "TD3": COLOR_TD3}


class AnimationWindow(tk.Toplevel):
    """
    Standalone popup window that renders a live Ant-v5 MuJoCo simulation
    inside a Tkinter Canvas using gymnasium's rgb_array render mode and
    Pillow for numpy→PhotoImage conversion.

    Usage (from TrainingPanel)
    --------------------------
        self._anim_win = AnimationWindow(self, root=self.winfo_toplevel())
        self._anim_win.set_agent(wrapper, "SAC")   # call after training starts
        # ... later ...
        self._anim_win.request_close()             # from any thread
    """

    # ─────────────────────────────────────────────────────────────────────────
    # Construction
    # ─────────────────────────────────────────────────────────────────────────

    def __init__(self, master: tk.Misc, root: tk.Tk) -> None:
        super().__init__(master)
        self._root   = root
        self._closed = False

        # ── Window chrome ─────────────────────────────────────────────────────
        self.title("Ant-v5 — Live Render")
        self.configure(bg=BG1)
        self.minsize(400, 360)
        self.resizable(True, True)

        # Position offset from main window
        rx = root.winfo_x()
        ry = root.winfo_y()
        self.geometry(f"520x460+{rx + 80}+{ry + 60}")

        # ── Shared state ──────────────────────────────────────────────────────
        self._agent:      Optional[object] = None   # BaseWrapper subclass
        self._agent_lock: threading.Lock   = threading.Lock()

        # ── Frame pipeline ────────────────────────────────────────────────────
        self._frame_queue: queue.Queue = queue.Queue(maxsize=QUEUE_MAXSIZE)

        # PhotoImage MUST be kept as an instance attribute to prevent CPython
        # garbage-collecting it between canvas redraws → blank canvas.
        self._photo:           Optional[ImageTk.PhotoImage] = None
        self._canvas_image_id: Optional[int]                = None

        # ── FPS tracking ──────────────────────────────────────────────────────
        self._frame_count: int   = 0
        self._fps_ts:      float = time.monotonic()

        # ── Polling callback handle ───────────────────────────────────────────
        self._poll_id: Optional[str] = None

        # ── Render thread shutdown signal ─────────────────────────────────────
        self._stop_render: threading.Event = threading.Event()

        # ── Build UI ──────────────────────────────────────────────────────────
        self._build_ui()

        # ── Start render thread (waits for set_agent before stepping env) ─────
        self._render_thread = threading.Thread(
            target   = self._render_loop,
            daemon   = True,
            name     = "ant-render",
        )
        self._render_thread.start()

        # ── Start main-thread frame polling ───────────────────────────────────
        self._poll_id = self.after(33, self._poll_frames)

        # ── Wire close protocol ───────────────────────────────────────────────
        self.protocol("WM_DELETE_WINDOW", self._close)

    # ─────────────────────────────────────────────────────────────────────────
    # UI construction
    # ─────────────────────────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        """Build all widgets top-to-bottom: title bar → canvas → stats → button."""
        self._build_title_bar()
        self._build_canvas()
        self._build_stat_tiles()
        self._build_close_button()

    def _build_title_bar(self) -> None:
        """34 px header with title label, FPS counter, and close button."""
        bar = tk.Frame(self, bg=BG0, height=34)
        bar.pack(side="top", fill="x")
        bar.pack_propagate(False)

        # ── Left: title ───────────────────────────────────────────────────────
        self._title_label = tk.Label(
            bar,
            text   = "🎬  Ant-v5 — Live Render  [waiting for agent…]",
            bg     = BG0,
            fg     = TEXT_PRIMARY,
            font   = FONT_SMALL,
            anchor = "w",
        )
        self._title_label.pack(side="left", padx=12, pady=6)

        # ── Right: close button ───────────────────────────────────────────────
        tk.Button(
            bar,
            text            = "✕",
            bg              = BG0,
            fg              = TEXT_SECONDARY,
            activebackground= BG3,
            activeforeground= TEXT_PRIMARY,
            relief          = "flat",
            bd              = 0,
            font            = FONT_SMALL,
            command         = self._close,
            cursor          = "hand2",
            padx            = 6,
        ).pack(side="right", padx=4, pady=4)

        # ── Right: FPS counter ────────────────────────────────────────────────
        self._fps_label = tk.Label(
            bar,
            text   = "– fps",
            bg     = BG0,
            fg     = TEXT_TERTIARY,
            font   = FONT_MONO_SM,
            anchor = "e",
            width  = 7,
        )
        self._fps_label.pack(side="right", padx=8, pady=6)

        # 1 px separator beneath the title bar
        tk.Frame(self, bg=BORDER_DARK, height=1).pack(side="top", fill="x")

    def _build_canvas(self) -> None:
        """
        Expandable Canvas that fills all space between the title bar and stat
        tiles.  MuJoCo rgb_array frames are drawn here as PhotoImages.
        """
        self._canvas = tk.Canvas(
            self,
            bg                = BG0,
            highlightthickness = 0,
            cursor             = "crosshair",
        )
        self._canvas.pack(
            side   = "top",
            fill   = "both",
            expand = True,
            padx   = 6,
            pady   = (6, 2),
        )

        # Placeholder text shown before the first frame arrives
        self._placeholder_id = self._canvas.create_text(
            260, 180,
            text   = "Waiting for simulation…",
            fill   = TEXT_TERTIARY,
            font   = FONT_MONO_SM,
            anchor = "center",
            tags   = "placeholder",
        )

    def _build_stat_tiles(self) -> None:
        """Three side-by-side stat tiles: Episode | Step | Reward."""
        # 1 px separator above stats
        tk.Frame(self, bg=BORDER_DARK, height=1).pack(side="top", fill="x")

        container = tk.Frame(self, bg=BG1, height=62)
        container.pack(side="top", fill="x", padx=6, pady=(4, 4))
        container.pack_propagate(False)

        tile_defs = [
            ("Episode",  "_ep_label",  "—"),
            ("Step",     "_step_label","—"),
            ("Reward",   "_rew_label", "—"),
        ]

        for i, (title, attr, initial) in enumerate(tile_defs):
            padx = (0, 4) if i < len(tile_defs) - 1 else (0, 0)

            tile = tk.Frame(container, bg=BG2, padx=10, pady=6)
            tile.pack(side="left", fill="both", expand=True, padx=padx)

            tk.Label(
                tile,
                text   = title,
                bg     = BG2,
                fg     = TEXT_TERTIARY,
                font   = FONT_MONO_SM,
                anchor = "w",
            ).pack(anchor="w")

            val_label = tk.Label(
                tile,
                text   = initial,
                bg     = BG2,
                fg     = TEXT_PRIMARY,
                font   = FONT_BOLD_12,
                anchor = "w",
            )
            val_label.pack(anchor="w")
            setattr(self, attr, val_label)

        # 1 px separator below stats
        tk.Frame(self, bg=BORDER_DARK, height=1).pack(side="top", fill="x")

    def _build_close_button(self) -> None:
        """Full-width close button at the bottom of the popup."""
        btn_frame = tk.Frame(self, bg=BG1, pady=6)
        btn_frame.pack(side="bottom", fill="x", padx=6)

        tk.Button(
            btn_frame,
            text            = "■   Close Animation",
            bg              = BG2,
            fg              = TEXT_DANGER,
            activebackground= BG3,
            activeforeground= TEXT_DANGER,
            relief          = "flat",
            bd              = 0,
            font            = FONT_SMALL,
            command         = self._close,
            cursor          = "hand2",
            pady            = 7,
        ).pack(fill="x")

    # ─────────────────────────────────────────────────────────────────────────
    # Public API — called from TrainingPanel (main thread)
    # ─────────────────────────────────────────────────────────────────────────

    def set_agent(self, wrapper, algo_label: str) -> None:
        """
        Assign the agent whose policy the render thread will use.
        Thread-safe: _agent_lock is held only for the assignment.

        Parameters
        ----------
        wrapper    : BaseWrapper subclass with a .predict(obs, deterministic) method.
        algo_label : Human-readable algorithm name shown in the title bar,
                     e.g. "SAC", "TD3", "A2C".
        """
        with self._agent_lock:
            self._agent = wrapper
        print(f"[ANIM] Agent set for {algo_label}")

        # Update title bar to reflect the active algorithm
        color = ALGO_COLORS.get(algo_label, TEXT_PRIMARY)
        self._title_label.config(
            text = f"🎬  Ant-v5 — Live Render  [{algo_label}]",
            fg   = color,
        )

    def clear_agent(self) -> None:
        """Remove the active agent (render thread enters the wait-loop)."""
        with self._agent_lock:
            self._agent = None
        self._title_label.config(
            text = "🎬  Ant-v5 — Live Render  [waiting for agent…]",
            fg   = TEXT_PRIMARY,
        )

    def request_close(self) -> None:
        """
        Thread-safe close trigger that can be called from any thread.
        Schedules _close() on the main thread via after().
        """
        self.after(0, self._close)

    def on_training_data(self, item: dict) -> None:
        """
        Update the stat tiles with current training episode data.
        Called from AntUI._poll() to synchronize the popup stats with training progress.
        """
        ep = item.get("episode", 0)
        step = item.get("steps", 0)
        reward = item.get("reward", 0.0)
        self._update_stat_tiles(ep, step, reward)

    # ─────────────────────────────────────────────────────────────────────────
    # Main-thread frame consumer
    # ─────────────────────────────────────────────────────────────────────────

    def _poll_frames(self) -> None:
        """
        Drain at most ONE frame per call so the Tkinter event loop is never
        blocked by a burst of incoming frames.  Reschedules itself at ~30 Hz.
        """
        if self._closed:
            return

        try:
            frame_rgb, ep, step, ep_rew = self._frame_queue.get_nowait()
            print(f"[POLL] Received frame: shape {frame_rgb.shape}, displaying")
            self._display_frame(frame_rgb)
            self._update_stat_tiles(ep, step, ep_rew)
            self._update_fps()
        except queue.Empty:
            pass

        # Reschedule unconditionally — do not accumulate missed ticks
        self._poll_id = self.after(33, self._poll_frames)

    def _display_frame(self, frame_rgb: np.ndarray) -> None:
        """
        Convert a numpy uint8 (H, W, 3) frame to a Tkinter PhotoImage
        and draw it on the canvas, scaled to the current canvas dimensions.

        MUST be called from the main thread only.
        """
        cw = self._canvas.winfo_width()
        ch = self._canvas.winfo_height()
        if cw < 2 or ch < 2:
            # Canvas not yet laid out — skip this frame
            return

        # Remove placeholder text on first real frame
        if self._placeholder_id is not None:
            self._canvas.delete("placeholder")
            self._placeholder_id = None

        img = Image.fromarray(frame_rgb, mode="RGB")
        img = img.resize((cw, ch), Image.LANCZOS)

        # Keep reference on self — prevents CPython GC from collecting the
        # PhotoImage before Tkinter finishes drawing it (classic Tkinter gotcha)
        self._photo = ImageTk.PhotoImage(img)

        if self._canvas_image_id is None:
            self._canvas_image_id = self._canvas.create_image(
                0, 0, anchor="nw", image=self._photo
            )
        else:
            self._canvas.itemconfig(self._canvas_image_id, image=self._photo)

    def _update_stat_tiles(self, ep: int, step: int, ep_rew: float) -> None:
        """Refresh the three stat tiles with current episode metrics."""
        self._ep_label.config(text=str(ep))
        self._step_label.config(text=str(step))
        self._rew_label.config(text=f"{ep_rew:,.1f}")

    def _update_fps(self) -> None:
        """Recompute and display the FPS counter once per second."""
        self._frame_count += 1
        now = time.monotonic()
        elapsed = now - self._fps_ts
        if elapsed >= 1.0:
            fps = self._frame_count / elapsed
            self._fps_label.config(text=f"{fps:.0f} fps")
            self._frame_count = 0
            self._fps_ts = now

    # ─────────────────────────────────────────────────────────────────────────
    # Render thread — runs entirely outside the Tkinter main thread
    # ─────────────────────────────────────────────────────────────────────────

    def _render_loop(self) -> None:
        """
        Background render thread.

        Lifecycle
        ---------
        1. Creates its own independent gymnasium Ant-v5 env
           (render_mode="rgb_array" — no OS window, thread-safe).
        2. Waits for set_agent() to provide a policy.
        3. Steps the env and pushes (frame, ep, step, reward) tuples
           onto _frame_queue at ≤ 30 FPS.
        4. On physics errors: resets the env silently and continues.
        5. Exits when _stop_render is set, calling env.close().

        NEVER touches any Tkinter widget.
        """
        print("[RENDER] Render thread started")
        try:
            import gymnasium as gym
            env = self._make_render_env()
            print("[RENDER] Env created")
        except Exception as e:
            print(f"[RENDER] Failed to create env: {e}")
            return
        try:
            obs, _ = env.reset(seed=0)
            print(f"[RENDER] Initial obs shape: {obs.shape}")
        except Exception as e:
            print(f"[RENDER] Failed to reset env: {e}")
            return

        ep:     int   = 0
        step:   int   = 0
        ep_rew: float = 0.0

        while not self._stop_render.is_set():
            t0 = time.monotonic()

            # ── Get agent (briefly holds lock) ────────────────────────────────
            with self._agent_lock:
                agent = self._agent

            if agent is None:
                # No agent yet — idle-wait without spinning
                time.sleep(0.05)
                continue

            print("[RENDER] Agent available, stepping")

            # ── Environment step ──────────────────────────────────────────────
            try:
                action = agent.predict(obs, deterministic=True)
                print(f"[RENDER] Action shape: {action.shape}, obs shape: {obs.shape}")
                obs, reward, terminated, truncated, _ = env.step(action)
                ep_rew += float(reward)
                step   += 1

                # ── Grab rendered frame ───────────────────────────────────────
                frame_rgb: np.ndarray = env.render()  # (H, W, 3) uint8
                print(f"[RENDER] Frame shape: {frame_rgb.shape}, dtype: {frame_rgb.dtype}, mean: {frame_rgb.mean():.2f}")

                # ── Push to queue — silently drop if full ─────────────────────
                try:
                    self._frame_queue.put_nowait((frame_rgb, ep, step, ep_rew))
                    print("[RENDER] Frame queued")
                except queue.Full:
                    print("[RENDER] Queue full, dropped frame")
                    pass  # main thread will catch up — no action needed

                # ── Handle episode end ────────────────────────────────────────
                if terminated or truncated:
                    obs, _ = env.reset()
                    ep    += 1
                    step   = 0
                    ep_rew = 0.0

            except Exception as e:
                print(f"[RENDER] Exception in step/render: {e}")
                # MuJoCo physics error or reset failure — recover silently
                try:
                    obs, _ = env.reset()
                except Exception:
                    pass
                ep_rew = 0.0
                step   = 0

            # ── Rate-limit: sleep remainder of 33 ms budget ───────────────────
            budget_remaining = FRAME_INTERVAL - (time.monotonic() - t0)
            if budget_remaining > 0:
                time.sleep(budget_remaining)

        # ── Cleanup ───────────────────────────────────────────────────────────
        try:
            env.close()
        except Exception:
            pass

    @staticmethod
    def _make_render_env():
        """
        Create a headless Ant-v5 render environment.

        render_mode="rgb_array"
            MuJoCo renders to an offscreen buffer and returns a numpy array.
            No OS window is opened.  Safe to call from any background thread.
        """
        import gymnasium as gym
        return gym.make(
            "Ant-v5",
            render_mode              = "rgb_array",
            ctrl_cost_weight         = 0.5,
            contact_cost_weight      = 5e-4,
            healthy_reward           = 1.0,
            terminate_when_unhealthy = True,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Shutdown
    # ─────────────────────────────────────────────────────────────────────────

    def _close(self) -> None:
        """
        Safe, non-blocking shutdown sequence (always called from main thread):

        1. Guard against double-close.
        2. Signal the render thread to exit.
        3. Cancel the pending after() poll callback.
        4. Begin non-blocking thread join via _wait_for_thread().
        """
        if self._closed:
            return
        self._closed = True

        # Step 1 — signal render thread
        self._stop_render.set()

        # Step 2 — cancel frame-poll callback to prevent any further
        #           _display_frame calls after the canvas is destroyed
        if self._poll_id is not None:
            try:
                self.after_cancel(self._poll_id)
            except Exception:
                pass
            self._poll_id = None

        # Step 3 — begin non-blocking join
        self._wait_for_thread(attempt=0)

    def _wait_for_thread(self, attempt: int = 0) -> None:
        """
        Poll whether the render thread has finished every 100 ms.
        Destroys the window once it has exited or after SHUTDOWN_POLLS attempts
        (≈ 3 s), whichever comes first.

        Using after() instead of thread.join() prevents freezing the Tkinter
        event loop during the shutdown wait.
        """
        thread_done = not self._render_thread.is_alive()
        timed_out   = attempt >= SHUTDOWN_POLLS

        if thread_done or timed_out:
            # Flush any remaining PhotoImage reference so it is collected
            self._photo = None
            try:
                self.destroy()
            except tk.TclError:
                pass  # already destroyed — harmless
            return

        # Render thread still alive — check again in 100 ms
        self.after(100, lambda: self._wait_for_thread(attempt + 1))


# ─────────────────────────────────────────────────────────────────────────────
# Standalone smoke-test (run:  python animation_window.py)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Opens a minimal Tkinter root window with a button to launch/close the
    AnimationWindow popup in isolation (no training agent needed — the popup
    will show 'Waiting for simulation…' until an agent is attached).

    Useful for verifying layout, resize behaviour, and clean shutdown
    without running the full training workbench.
    """
    root = tk.Tk()
    root.title("AnimationWindow — smoke test")
    root.geometry("400x120")
    root.configure(bg=BG0)

    popup_ref: list[Optional[AnimationWindow]] = [None]

    def toggle():
        if popup_ref[0] is None or popup_ref[0]._closed:
            win = AnimationWindow(root, root=root)
            popup_ref[0] = win
            btn.config(text="Close popup")
        else:
            popup_ref[0].request_close()
            popup_ref[0] = None
            btn.config(text="Open popup")

    btn = tk.Button(
        root,
        text            = "Open popup",
        command         = toggle,
        bg              = BG2,
        fg              = TEXT_PRIMARY,
        relief          = "flat",
        font            = FONT_SMALL,
        padx            = 16,
        pady            = 8,
        cursor          = "hand2",
        activebackground= BG3,
    )
    btn.pack(expand=True)

    def on_close():
        if popup_ref[0] and not popup_ref[0]._closed:
            popup_ref[0].request_close()
        root.after(400, root.destroy)

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()
