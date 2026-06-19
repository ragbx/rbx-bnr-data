"""Fenêtre principale de l'Éditeur EAD BnR."""

from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from .. import __version__
from ..model import EadDocument
from .controlaccess_panel import ControlAccessPanel
from .dao_panel import DaoPanel
from .navigator import Navigator

APP_TITLE = "Éditeur EAD BnR"


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.doc: EadDocument | None = None
        self.geometry("1100x680")
        self._build_menu()
        self._build_body()
        self._build_statusbar()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._update_title()

    # construction -------------------------------------------------------- #

    def _build_menu(self) -> None:
        menubar = tk.Menu(self)

        filem = tk.Menu(menubar, tearoff=0)
        filem.add_command(label="Ouvrir…", command=self.open_file, accelerator="Ctrl+O")
        filem.add_command(label="Enregistrer", command=self.save_file, accelerator="Ctrl+S")
        filem.add_command(label="Enregistrer sous…", command=self.save_file_as)
        filem.add_separator()
        filem.add_command(label="Quitter", command=self._on_close)
        menubar.add_cascade(label="Fichier", menu=filem)

        toolsm = tk.Menu(menubar, tearoff=0)
        toolsm.add_command(label="Traitement par lot…", command=self.open_batch)
        menubar.add_cascade(label="Outils", menu=toolsm)

        helpm = tk.Menu(menubar, tearoff=0)
        helpm.add_command(label="À propos", command=self._about)
        menubar.add_cascade(label="Aide", menu=helpm)

        self.config(menu=menubar)
        self.bind_all("<Control-o>", lambda _e: self.open_file())
        self.bind_all("<Control-s>", lambda _e: self.save_file())

    def _build_body(self) -> None:
        paned = ttk.PanedWindow(self, orient="horizontal")
        paned.pack(fill="both", expand=True)

        self.navigator = Navigator(paned, on_select=self._on_component_selected)
        paned.add(self.navigator, weight=1)

        self.notebook = ttk.Notebook(paned)
        self.dao_panel = DaoPanel(self.notebook, on_change=self._on_change)
        self.ca_panel = ControlAccessPanel(self.notebook, on_change=self._on_change)
        self.notebook.add(self.dao_panel, text="Numérisations (dao)")
        self.notebook.add(self.ca_panel, text="Indexation (controlaccess)")
        paned.add(self.notebook, weight=3)

    def _build_statusbar(self) -> None:
        self.status = tk.StringVar(value="Aucun fichier ouvert.")
        bar = ttk.Frame(self, relief="sunken")
        bar.pack(side="bottom", fill="x")
        ttk.Label(bar, textvariable=self.status, anchor="w").pack(side="left", padx=6, pady=2)

    # fichiers ------------------------------------------------------------ #

    def open_file(self) -> None:
        if not self._confirm_discard():
            return
        path = filedialog.askopenfilename(
            title="Ouvrir un fichier EAD",
            filetypes=[("Fichiers EAD/XML", "*.xml"), ("Tous les fichiers", "*.*")],
        )
        if not path:
            return
        try:
            self.doc = EadDocument.load(path)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Erreur d'ouverture", f"Impossible d'ouvrir le fichier :\n{exc}")
            return
        self.navigator.load(self.doc)
        self._update_title()
        self.status.set(f"Ouvert : {path}")

    def save_file(self) -> None:
        if self.doc is None:
            return
        if self.doc.path is None:
            self.save_file_as()
            return
        self._do_save(self.doc.path)

    def save_file_as(self) -> None:
        if self.doc is None:
            return
        initial = self.doc.path.name if self.doc.path else "ead.xml"
        path = filedialog.asksaveasfilename(
            title="Enregistrer sous", defaultextension=".xml", initialfile=initial,
            filetypes=[("Fichiers EAD/XML", "*.xml")],
        )
        if path:
            self._do_save(Path(path))

    def _do_save(self, path: Path) -> None:
        try:
            self.doc.save(path, backup=True)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Erreur d'enregistrement", str(exc))
            return
        self._update_title()
        self.status.set(f"Enregistré : {path} (sauvegarde .bak créée si écrasement)")

    # événements ---------------------------------------------------------- #

    def _on_component_selected(self, element) -> None:
        self.dao_panel.set_component(self.doc, element)
        self.ca_panel.set_component(self.doc, element)

    def _on_change(self) -> None:
        if self.doc is not None:
            self.doc.mark_dirty()
            self._update_title()

    def open_batch(self) -> None:
        from .batch_dialog import BatchDialog

        BatchDialog(self).show()

    # divers -------------------------------------------------------------- #

    def _update_title(self) -> None:
        name = self.doc.path.name if (self.doc and self.doc.path) else "(aucun fichier)"
        flag = " *" if (self.doc and self.doc.dirty) else ""
        self.title(f"{APP_TITLE} — {name}{flag}")

    def _confirm_discard(self) -> bool:
        if self.doc is None or not self.doc.dirty:
            return True
        answer = messagebox.askyesnocancel(
            "Modifications non enregistrées",
            "Enregistrer les modifications avant de continuer ?",
        )
        if answer is None:
            return False
        if answer:
            self.save_file()
            return not self.doc.dirty
        return True

    def _about(self) -> None:
        messagebox.showinfo(
            "À propos",
            f"{APP_TITLE} v{__version__}\n\n"
            "Édition guidée des numérisations (dao) et de l'indexation\n"
            "(controlaccess) des fichiers EAD de la Bibliothèque\n"
            "numérique de Roubaix.",
        )

    def _on_close(self) -> None:
        if self._confirm_discard():
            self.destroy()


def main() -> None:
    App().mainloop()


if __name__ == "__main__":
    main()
