"""Traitement par lot : applique une transformation à un dossier de fichiers EAD."""

from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from .. import operations as ops
from ..model import CONTROLACCESS_TAGS, EadDocument, is_ead_file


def _op_rename_role(root, p):
    return ops.rename_role(root, p["old"], p["new"])


def _op_replace_href(root, p):
    return ops.replace_in_href(
        root, p["find"], p["repl"],
        regex=bool(p.get("regex")),
        role_filter=p["role"] or None,
    )


def _op_replace_term(root, p):
    return ops.replace_term_text(root, p["tag"] or None, p["old"], p["new"])


def _op_normalize_source(root, p):
    return ops.normalize_source(root, {p["old"]: p["new"]})


# Chaque opération déclare ses champs (clé, libellé, type) et sa fonction.
OPERATIONS: dict[str, dict] = {
    "Renommer un rôle (dao)": {
        "fields": [("old", "Rôle actuel", "text"), ("new", "Nouveau rôle", "text")],
        "run": _op_rename_role,
    },
    "Remplacer dans le lien href (dao)": {
        "fields": [
            ("find", "Rechercher", "text"),
            ("repl", "Remplacer par", "text"),
            ("role", "Filtre rôle (optionnel)", "text"),
            ("regex", "Expression régulière", "check"),
        ],
        "run": _op_replace_href,
    },
    "Remplacer un terme (controlaccess)": {
        "fields": [
            ("tag", "Type (vide = tous)", "tag"),
            ("old", "Terme actuel", "text"),
            ("new", "Nouveau terme", "text"),
        ],
        "run": _op_replace_term,
    },
    "Normaliser une source (controlaccess)": {
        "fields": [
            ("old", "Source actuelle", "text"),
            ("new", "Nouvelle source", "text"),
        ],
        "run": _op_normalize_source,
    },
}


class BatchDialog(tk.Toplevel):
    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent)
        self.title("Traitement par lot")
        self.transient(parent)
        self.geometry("760x620")
        self._folder: Path | None = None
        self._files: list[Path] = []
        self._field_vars: dict[str, tk.Variable] = {}

        self._build()
        self.grab_set()

    # construction -------------------------------------------------------- #

    def _build(self) -> None:
        pad = {"padx": 8, "pady": 4}

        top = ttk.Frame(self)
        top.pack(fill="x", **pad)
        ttk.Button(top, text="Choisir un dossier…", command=self._pick_folder).pack(side="left")
        self.folder_lbl = ttk.Label(top, text="Aucun dossier sélectionné")
        self.folder_lbl.pack(side="left", padx=8)

        opf = ttk.LabelFrame(self, text="Opération")
        opf.pack(fill="x", **pad)
        self.op_var = tk.StringVar(value=next(iter(OPERATIONS)))
        ttk.Combobox(
            opf, textvariable=self.op_var, values=list(OPERATIONS), state="readonly",
        ).pack(fill="x", padx=8, pady=6)
        self.op_var.trace_add("write", lambda *_: self._build_fields())

        self.fields_frame = ttk.Frame(self)
        self.fields_frame.pack(fill="x", **pad)
        self._build_fields()

        optf = ttk.Frame(self)
        optf.pack(fill="x", **pad)
        self.backup_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            optf, text="Créer une sauvegarde .bak avant écrasement", variable=self.backup_var,
        ).pack(side="left")

        btns = ttk.Frame(self)
        btns.pack(fill="x", **pad)
        ttk.Button(btns, text="Aperçu (sans écrire)", command=self._preview).pack(side="left")
        ttk.Button(btns, text="Appliquer", command=self._apply).pack(side="left", padx=6)
        ttk.Button(btns, text="Fermer", command=self.destroy).pack(side="right")

        self.progress = ttk.Progressbar(self, mode="determinate")
        self.progress.pack(fill="x", **pad)

        logf = ttk.LabelFrame(self, text="Résultats")
        logf.pack(fill="both", expand=True, **pad)
        self.log = tk.Text(logf, height=12, wrap="none")
        vsb = ttk.Scrollbar(logf, orient="vertical", command=self.log.yview)
        self.log.configure(yscrollcommand=vsb.set, state="disabled")
        self.log.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

    def _build_fields(self) -> None:
        for child in self.fields_frame.winfo_children():
            child.destroy()
        self._field_vars.clear()
        spec = OPERATIONS[self.op_var.get()]
        for row, (key, label, kind) in enumerate(spec["fields"]):
            if kind == "check":
                var = tk.BooleanVar(value=False)
                ttk.Checkbutton(self.fields_frame, text=label, variable=var).grid(
                    row=row, column=1, sticky="w", pady=2
                )
            else:
                ttk.Label(self.fields_frame, text=f"{label} :").grid(
                    row=row, column=0, sticky="w", pady=2
                )
                var = tk.StringVar()
                if kind == "tag":
                    ttk.Combobox(
                        self.fields_frame, textvariable=var,
                        values=("",) + CONTROLACCESS_TAGS, state="readonly", width=48,
                    ).grid(row=row, column=1, sticky="w", pady=2)
                else:
                    ttk.Entry(self.fields_frame, textvariable=var, width=50).grid(
                        row=row, column=1, sticky="w", pady=2
                    )
            self._field_vars[key] = var

    # actions ------------------------------------------------------------- #

    def _pick_folder(self) -> None:
        folder = filedialog.askdirectory(title="Dossier contenant les fichiers EAD")
        if not folder:
            return
        self._folder = Path(folder)
        self._files = sorted(p for p in self._folder.glob("*.xml") if is_ead_file(p))
        self.folder_lbl.config(text=f"{self._folder}  ({len(self._files)} fichier(s) EAD)")

    def _params(self) -> dict:
        return {k: v.get() for k, v in self._field_vars.items()}

    def _run(self, write: bool) -> None:
        if not self._files:
            messagebox.showwarning("Lot", "Aucun fichier EAD sélectionné.", parent=self)
            return
        spec = OPERATIONS[self.op_var.get()]
        params = self._params()
        self.progress.config(maximum=len(self._files), value=0)
        self._clear_log()
        total_changes = total_files = errors = 0
        for path in self._files:
            try:
                doc = EadDocument.load(path)
                n = spec["run"](doc.root, params)
                if n and write:
                    doc.save(path, backup=self.backup_var.get())
                if n:
                    total_changes += n
                    total_files += 1
                    self._append(f"{'✔' if write else '·'} {path.name} : {n} modif.")
            except Exception as exc:  # noqa: BLE001
                errors += 1
                self._append(f"✖ {path.name} : ERREUR {exc}")
            self.progress.step(1)
            self.update_idletasks()
        verb = "appliquées" if write else "détectées (aperçu)"
        self._append(
            f"\n— {total_changes} modification(s) {verb} sur {total_files} fichier(s)"
            f"{f', {errors} erreur(s)' if errors else ''}."
        )

    def _preview(self) -> None:
        self._run(write=False)

    def _apply(self) -> None:
        if not self._files:
            messagebox.showwarning("Lot", "Aucun fichier EAD sélectionné.", parent=self)
            return
        if not messagebox.askyesno(
            "Appliquer",
            f"Appliquer l'opération à {len(self._files)} fichier(s) ?\n"
            "Les fichiers seront modifiés sur disque.",
            parent=self,
        ):
            return
        self._run(write=True)

    # log ----------------------------------------------------------------- #

    def _clear_log(self) -> None:
        self.log.config(state="normal")
        self.log.delete("1.0", "end")
        self.log.config(state="disabled")

    def _append(self, line: str) -> None:
        self.log.config(state="normal")
        self.log.insert("end", line + "\n")
        self.log.see("end")
        self.log.config(state="disabled")

    def show(self) -> None:
        self.wait_window()
