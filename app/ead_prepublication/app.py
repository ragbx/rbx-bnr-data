"""
EAD Pré-publication — Charte Médiathèque de Roubaix · La Grand-Plage
Prépare les fichiers EAD Mnesys en vue de leur publication (aujourd'hui :
synchronisation des <dao>/<daogrp> à partir des <odd>, cf. ead_preprocess.py).
"""

import os
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def _resource_path(*parts: str) -> str:
    """Chemin absolu vers une ressource embarquée (img/...).

    En exécution normale : relatif à ce fichier. Une fois figé par PyInstaller,
    les données déclarées dans le .spec sont extraites dans sys._MEIPASS.
    """
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return str(base.joinpath(*parts))


# ─────────────────────────────────────────────
# Thème — modifier ici pour changer l'apparence
# ─────────────────────────────────────────────
@dataclass
class Theme:
    # Couleurs
    bg: str          = "#ffffff"   # fond principal
    bg_alt: str      = "#f8f9fa"   # fond secondaire (zones grises)
    border: str      = "#dee2e6"   # bordures

    primary: str         = "#9bd4d0"  # bouton/accent principal
    primary_hover: str   = "#7bbab6"
    secondary: str       = "#e5c0b1"  # bouton/accent secondaire
    secondary_hover: str = "#c9a99c"
    danger: str          = "#dc3545"
    success: str         = "#198754"

    text: str       = "#212529"   # texte principal
    text_muted: str = "#6c757d"   # texte secondaire

    # Polices (remplacées au démarrage par la meilleure police disponible)
    # Forcer une valeur spécifique ici pour désactiver la résolution automatique.
    font_family: str = ""   # auto → Segoe UI / DejaVu Sans / Liberation Sans / Arial
    font_mono:   str = ""   # auto → Consolas / DejaVu Sans Mono / Liberation Mono

    # Identité / Logo
    window_title: str  = "EAD Pré-publication"
    org_name: str      = "Médiathèque et Archives de Roubaix"
    org_tag: str       = "LA Grand-Plage"   # badge coloré dans l'en-tête
    app_subtitle: str  = "Synchronisation des liens <dao> à partir des <odd>"
    logo_path: Optional[str] = field(default_factory=lambda: _resource_path("img", "logo.png"))

    # Dimensions
    padding:  int = 22
    window_w: int = 780
    window_h: int = 620

    # ── Polices dérivées (calculées à partir de font_family) ──────────
    def f_title(self):   return (self.font_family, 18, "bold")
    def f_sub(self):     return (self.font_family,  9)
    def f_label(self):   return (self.font_family, 10, "bold")
    def f_body(self):    return (self.font_family, 10)
    def f_small(self):   return (self.font_family,  9)
    def f_btn(self):     return (self.font_family, 10, "bold")
    def f_btn_sm(self):  return (self.font_family,  9)
    def f_mono(self):    return (self.font_mono,    9)


# Thème actif — remplacez par votre propre instance pour personnaliser
THEME = Theme()


# ─────────────────────────────────────────────
# Résolution de police — première dispo sur le système
# ─────────────────────────────────────────────
def _best_font(candidates: list[str]) -> str:
    """Retourne la première police de la liste installée sur le système."""
    from tkinter import font as tkfont
    available = set(tkfont.families())
    for name in candidates:
        if name in available:
            return name
    return candidates[-1]   # fallback garanti


# Polices candidates par ordre de préférence
# Sans-serif : Segoe UI (Windows) · DejaVu Sans / Liberation Sans (Debian/Ubuntu)
_SANS_CANDIDATES = ["Segoe UI", "DejaVu Sans", "Liberation Sans", "Arial", "Helvetica"]
# Mono      : Consolas (Windows) · DejaVu Sans Mono / Liberation Mono (Linux)
_MONO_CANDIDATES = ["Consolas", "DejaVu Sans Mono", "Liberation Mono", "Courier New"]


# ─────────────────────────────────────────────
# Chargement du logo (PNG/GIF natif ou SVG via ImageMagick)
# ─────────────────────────────────────────────
def _load_logo(path: str, max_h: int = 60):
    """Retourne un tk.PhotoImage depuis path (PNG/GIF), ou None."""
    if not path or not os.path.isfile(path):
        return None
    try:
        img = tk.PhotoImage(file=path)
        h = img.height()
        if h > max_h:
            factor = -(-h // max_h)  # division entière par excès
            img = img.subsample(factor, factor)
        return img
    except Exception:
        return None


# ─────────────────────────────────────────────
# Contraste texte/fond — WCAG relative luminance
# ─────────────────────────────────────────────
def _readable_fg(hex_color: str) -> str:
    """Retourne '#212529' (sombre) ou '#ffffff' selon la luminance du fond."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i+2], 16) / 255 for i in (0, 2, 4))
    # Linearisation sRGB
    def lin(c): return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4
    lum = 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b)
    return "#212529" if lum > 0.179 else "#ffffff"


# ─────────────────────────────────────────────
# Widgets utilitaires
# ─────────────────────────────────────────────
def make_button(parent, text, command, w=180, h=36, primary=True, theme=THEME):
    font   = theme.f_btn() if primary else theme.f_btn_sm()
    color  = theme.primary     if primary else theme.secondary
    colorh = theme.primary_hover if primary else theme.secondary_hover
    cv     = tk.Canvas(parent, width=w, height=h,
                       bg=theme.bg, bd=0, highlightthickness=0, cursor="hand2")
    enabled = [True]

    def draw(hover=False):
        cv.delete("all")
        if not enabled[0]:
            fill, fg, brd = theme.bg_alt, theme.border, theme.border
        else:
            fill = colorh if hover else color
            fg   = _readable_fg(fill)
            brd  = fill
        cv.create_rectangle(0, 0, w, h, fill=fill, outline=brd, width=1)
        cv.create_text(w // 2, h // 2, text=text, fill=fg, font=font)

    draw()
    cv.bind("<Enter>",    lambda _: draw(True))
    cv.bind("<Leave>",    lambda _: draw(False))
    cv.bind("<Button-1>", lambda _: command() if enabled[0] else None)
    cv.set_state = lambda on: (enabled.__setitem__(0, on), draw(False))
    return cv


def make_progress(parent, w=600, h=20, theme=THEME):
    cv    = tk.Canvas(parent, width=w, height=h,
                      bg=theme.bg, bd=0, highlightthickness=0)
    state = [0]

    def draw():
        cv.delete("all")
        cv.create_rectangle(0, 0, w, h, fill=theme.bg_alt, outline=theme.border, width=1)
        if state[0] > 0:
            fw = max(2, int((w - 2) * state[0] / 100))
            cv.create_rectangle(1, 1, fw, h - 1, outline="", fill=theme.primary)
        cv.create_text(w // 2, h // 2, text=f"{state[0]} %",
                       fill=theme.text if state[0] < 50 else "#ffffff",
                       font=theme.f_small())

    draw()
    cv.set = lambda v: (state.__setitem__(0, max(0, min(100, v))), draw())
    return cv


def make_section_header(parent, title, w=712, accent=None, theme=THEME):
    accent = accent or theme.primary
    cv = tk.Canvas(parent, width=w, height=34,
                   bg=theme.bg_alt, bd=0, highlightthickness=0)
    cv.create_rectangle(0, 0, 4, 34, fill=accent, outline="")
    cv.create_text(16, 17, text=title, anchor="w", fill=theme.text, font=theme.f_label())
    return cv


# ─────────────────────────────────────────────
# Application principale
# ─────────────────────────────────────────────
class EADPrepublicationApp(tk.Tk):
    def __init__(self, theme: Theme = THEME):
        super().__init__()
        # Résolution des polices après init de tkinter (families() indisponible avant)
        from dataclasses import replace as dc_replace
        t = dc_replace(theme,
                       font_family=theme.font_family or _best_font(_SANS_CANDIDATES),
                       font_mono=theme.font_mono     or _best_font(_MONO_CANDIDATES))
        self.t = t
        self.title(t.window_title)
        self.configure(bg=theme.bg)
        self.resizable(False, False)

        self._source_path = tk.StringVar()
        self._output_path = tk.StringVar()
        self._status_msg  = tk.StringVar(value="En attente d'un fichier source.")

        self._build_ui()
        self.update_idletasks()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        ww, wh = theme.window_w, theme.window_h
        self.geometry(f"{ww}x{wh}+{(sw-ww)//2}+{(sh-wh)//2}")

    def _build_ui(self):
        t = self.t
        tk.Frame(self, bg=t.primary, height=5).pack(fill="x", side="top")

        root = tk.Frame(self, bg=t.bg)
        root.pack(fill="both", expand=True, padx=t.padding, pady=(16, t.padding))

        self._build_header(root)

        tk.Frame(root, bg=t.primary, height=2).pack(fill="x", pady=(0, 14))

        self._build_file_section(
            root, "Fichier source",
            "Sélectionnez le fichier EAD (results/ead/ead_cor/bnr2mnesys/) à synchroniser :",
            self._source_path, self._browse_source, accent=t.primary)

        tk.Frame(root, bg=t.bg, height=10).pack()

        self._build_file_section(
            root, "Fichier de sortie",
            "Chemin d'enregistrement du fichier synchronisé :",
            self._output_path, self._browse_output, accent=t.secondary)

        tk.Frame(root, bg=t.bg, height=18).pack()

        self._build_progress(root)

        tk.Frame(root, bg=t.bg, height=14).pack()

        self._build_run_button(root)

        tk.Frame(root, bg=t.border, height=1).pack(fill="x", pady=(16, 10))

        self._build_log(root)

        tk.Frame(self, bg=t.primary, height=4).pack(fill="x", side="bottom")

    def _build_header(self, parent):
        t   = self.t
        hdr = tk.Frame(parent, bg=t.bg)
        hdr.pack(fill="x", pady=(0, 12))

        # Logo image (PNG, GIF natif ; SVG converti via ImageMagick)
        logo_img = _load_logo(t.logo_path) if t.logo_path else None
        if logo_img:
            lbl = tk.Label(hdr, image=logo_img, bg=t.bg)
            lbl.image = logo_img   # empêche le garbage-collector de supprimer l'image
            lbl.pack(side="left", anchor="n", padx=(0, 10), pady=(4, 0))

        # Badge texte (org_tag) — affiché uniquement si aucun logo n'est chargé
        if not logo_img:
            tag_frame = tk.Frame(hdr, bg=t.primary)
            tag_frame.pack(side="left", anchor="n", pady=(4, 0))
            tk.Label(tag_frame, text=t.org_tag,
                     font=(t.font_family, 7, "bold"),
                     fg="#ffffff", bg=t.primary,
                     padx=6, pady=2).pack()
            tk.Frame(hdr, bg=t.bg, width=10).pack(side="left")

        titles = tk.Frame(hdr, bg=t.bg)
        titles.pack(side="left")
        tk.Label(titles, text=t.org_name,
                 font=t.f_title(), fg=t.text, bg=t.bg).pack(anchor="w")
        tk.Label(titles, text=t.app_subtitle,
                 font=t.f_sub(), fg=t.text_muted, bg=t.bg).pack(anchor="w")

    def _build_file_section(self, parent, title, subtitle, var, browse_cmd, accent):
        t = self.t
        outer = tk.Frame(parent, bg=t.bg,
                         highlightthickness=1, highlightbackground=t.border)
        outer.pack(fill="x")

        make_section_header(outer, title, w=736, accent=accent, theme=t).pack(fill="x")

        inner = tk.Frame(outer, bg=t.bg)
        inner.pack(fill="x", padx=12, pady=(8, 10))
        tk.Label(inner, text=subtitle, font=t.f_small(),
                 fg=t.text_muted, bg=t.bg).pack(anchor="w", pady=(0, 5))

        row = tk.Frame(inner, bg=t.bg)
        row.pack(fill="x")

        ef = tk.Frame(row, bg=t.bg,
                      highlightthickness=1, highlightbackground=t.border)
        ef.pack(side="left", fill="x", expand=True)
        tk.Entry(ef, textvariable=var, font=t.f_body(),
                 bg=t.bg, fg=t.text, insertbackground=t.text,
                 bd=0, relief="flat").pack(padx=10, pady=6, fill="x")

        make_button(row, text="Parcourir", command=browse_cmd,
                    w=105, h=34, primary=False, theme=t).pack(side="left", padx=(8, 0))

    def _build_progress(self, parent):
        t    = self.t
        prog = tk.Frame(parent, bg=t.bg)
        prog.pack(fill="x")
        self._progress = make_progress(prog, w=736, h=20, theme=t)
        self._progress.pack(anchor="w")
        self._status_lbl = tk.Label(prog, textvariable=self._status_msg,
                                    font=t.f_small(), fg=t.text_muted, bg=t.bg,
                                    anchor="w")
        self._status_lbl.pack(anchor="w", pady=(4, 0))

    def _build_run_button(self, parent):
        t       = self.t
        btn_row = tk.Frame(parent, bg=t.bg)
        btn_row.pack(fill="x")
        self._run_btn = make_button(btn_row,
                                    text="Lancer la synchronisation",
                                    command=self._start_conversion,
                                    w=210, h=38, primary=True, theme=t)
        self._run_btn.pack(side="left")
        tk.Label(btn_row,
                 text="Les fichiers originaux ne sont pas modifiés.",
                 font=t.f_small(), fg=t.text_muted, bg=t.bg
                 ).pack(side="left", padx=(14, 0))

    def _build_log(self, parent):
        t = self.t
        make_section_header(parent, "Journal des opérations",
                            w=736, accent=t.secondary, theme=t).pack(anchor="w")

        log_wrap = tk.Frame(parent, bg=t.bg,
                            highlightthickness=1, highlightbackground=t.border)
        log_wrap.pack(fill="both", expand=True)

        self._log = tk.Text(log_wrap, height=6,
                            bg=t.bg_alt, fg=t.text_muted, font=t.f_mono(),
                            bd=0, highlightthickness=0,
                            state="disabled", wrap="word",
                            padx=10, pady=8, relief="flat",
                            selectbackground=t.primary,
                            selectforeground="#ffffff")
        sb = tk.Scrollbar(log_wrap, command=self._log.yview,
                          bg=t.bg_alt, troughcolor=t.bg, width=10)
        sb.pack(side="right", fill="y")
        self._log.pack(side="left", fill="both", expand=True)
        self._log.config(yscrollcommand=sb.set)

    # ── Actions ──────────────────────────────
    def _browse_source(self):
        path = filedialog.askopenfilename(
            title="Sélectionner le fichier source",
            filetypes=[("EAD / XML", "*.xml *.ead"), ("Tous", "*.*")]
        )
        if not path:
            return
        self._source_path.set(path)
        from ead_preprocess import EAD_preprocess
        name = EAD_preprocess(path).output_filename
        self._output_path.set(os.path.join(os.path.dirname(path), name))
        self._log_append(f"Source sélectionnée : {path}")
        self._set_status(0, "Fichier source chargé. Prêt pour la conversion.")

    def _browse_output(self):
        path = filedialog.asksaveasfilename(
            title="Enregistrer sous…",
            defaultextension=".xml",
            filetypes=[("XML", "*.xml"), ("Tous", "*.*")]
        )
        if path:
            self._output_path.set(path)
            self._log_append(f"Destination définie : {path}")

    def _start_conversion(self):
        src = self._source_path.get()
        out = self._output_path.get()
        if not src or not os.path.isfile(src):
            messagebox.showerror("Fichier introuvable",
                                 "Veuillez sélectionner un fichier source valide.")
            return
        if not out:
            messagebox.showerror("Destination manquante",
                                 "Veuillez définir le fichier de sortie.")
            return
        self._run_btn.set_state(False)
        self._set_status(0, "Initialisation de la synchronisation…")
        threading.Thread(target=self._run, args=(src, out), daemon=True).start()

    def _run(self, src, out):
        from ead_preprocess import EAD_preprocess
        try:
            self._log_append("Chargement du fichier source…")
            conv = EAD_preprocess(src)
            self.after(0, self._set_status, 10, "Lecture du fichier…")
            conv.load()
            self.after(0, self._set_status, 20, "Synchronisation des <dao>/<daoloc>…")
            self._log_append("Synchronisation à partir des <odd>…")
            stats = conv.transform(
                progress_callback=lambda v, m: self.after(0, self._set_status, v, m)
            )
            self._log_append(
                f"{stats['ajoutes']} ajoutés, {stats['supprimes']} supprimés."
            )
            self.after(0, self._set_status, 95, "Écriture du fichier de sortie…")
            self._log_append(f"Enregistrement : {out}")
            conv.save(out)
            self.after(0, self._set_status, 100, "Synchronisation terminée avec succès.")
            self._log_append("Opération terminée.")
            self.after(0, lambda: messagebox.showinfo(
                "Synchronisation réussie", f"Fichier enregistré :\n\n{out}"))
        except Exception as e:
            self.after(0, self._set_status, 0, f"Erreur : {e}")
            self._log_append(f"ERREUR : {e}")
            self.after(0, lambda: messagebox.showerror("Erreur", str(e)))
        finally:
            self.after(0, lambda: self._run_btn.set_state(True))

    def _set_status(self, value, message):
        t = self.t
        self._progress.set(value)
        self._status_msg.set(message)
        if "succès" in message or "terminée" in message.lower():
            self._status_lbl.config(fg=t.success)
        elif "erreur" in message.lower():
            self._status_lbl.config(fg=t.danger)
        else:
            self._status_lbl.config(fg=t.text_muted)

    def _log_append(self, text):
        def _do():
            self._log.config(state="normal")
            self._log.insert(tk.END, f"  {text}\n")
            self._log.see(tk.END)
            self._log.config(state="disabled")
        self.after(0, _do)


if __name__ == "__main__":
    # ── Pour personnaliser : créez votre propre thème ────────────────────
    #
    # mon_theme = Theme(
    #     primary       = "#2d6a4f",   # vert forêt
    #     primary_hover = "#1b4332",
    #     secondary     = "#52b788",
    #     font_family   = "Georgia",
    #     org_name      = "Médiathèque de Lyon",
    #     org_tag       = "LYON",
    #     app_subtitle  = "Pré-publication EAD · BnR → Mnesys",
    #     window_title  = "EAD Pré-publication · Lyon",
    #     logo_path     = "logo.png",  # PNG ou GIF supporté nativement par tkinter
    # )
    # app = EADPrepublicationApp(theme=mon_theme)
    #
    # ─────────────────────────────────────────────────────────────────────
    app = EADPrepublicationApp()
    app.mainloop()
