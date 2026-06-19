"""Panneau d'édition de l'indexation (controlaccess) d'un composant."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable

from .. import operations as ops
from ..model import CONTROLACCESS_TAGS, EadDocument
from .dialogs import TermDialog


class ControlAccessPanel(ttk.Frame):
    def __init__(self, parent: tk.Misc, on_change: Callable[[], None]) -> None:
        super().__init__(parent, padding=6)
        self._on_change = on_change
        self._doc: EadDocument | None = None
        self._component = None
        self._row_to_element: dict[str, object] = {}

        toolbar = ttk.Frame(self)
        toolbar.grid(row=0, column=0, sticky="w", pady=(0, 6))
        ttk.Button(toolbar, text="Ajouter…", command=self._add).grid(row=0, column=0, padx=2)
        ttk.Button(toolbar, text="Modifier…", command=self._edit).grid(row=0, column=1, padx=2)
        ttk.Button(toolbar, text="Supprimer", command=self._remove).grid(row=0, column=2, padx=2)
        ttk.Button(toolbar, text="Dédoublonner", command=self._dedupe).grid(row=0, column=3, padx=2)

        cols = ("tag", "text", "source")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", selectmode="browse")
        for col, title, width in (
            ("tag", "Type", 110),
            ("text", "Terme", 360),
            ("source", "Source", 280),
        ):
            self.tree.heading(col, text=title)
            self.tree.column(col, width=width, anchor="w")
        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.grid(row=1, column=0, sticky="nsew")
        vsb.grid(row=1, column=1, sticky="ns")
        self.rowconfigure(1, weight=1)
        self.columnconfigure(0, weight=1)
        self.tree.bind("<Double-1>", lambda _e: self._edit())

    # API ----------------------------------------------------------------- #

    def set_component(self, doc: EadDocument | None, component) -> None:
        self._doc = doc
        self._component = component
        self.refresh()

    def refresh(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self._row_to_element.clear()
        if self._doc is None or self._component is None:
            return
        for entry in self._doc.term_entries(self._component):
            iid = self.tree.insert("", "end", values=(entry.tag, entry.text, entry.source))
            self._row_to_element[iid] = entry.element

    # actions ------------------------------------------------------------- #

    def _selected(self):
        sel = self.tree.selection()
        return self._row_to_element.get(sel[0]) if sel else None

    def _sources(self) -> list[str]:
        return self._doc.known_sources() if self._doc else []

    def _add(self) -> None:
        if self._doc is None or self._component is None:
            return
        data = TermDialog(
            self, list(CONTROLACCESS_TAGS), self._sources(), title="Ajouter un terme",
        ).show()
        if data is None:
            return
        ca = self._doc.get_or_create_controlaccess(self._component)
        ops.add_term(ca, data["tag"], data["text"], data["source"])
        self._changed()

    def _edit(self) -> None:
        el = self._selected()
        if el is None:
            return
        data = TermDialog(
            self, list(CONTROLACCESS_TAGS), self._sources(),
            tag=el.tag, text=(el.text or ""), source=el.get("source", ""),
            title="Modifier le terme",
        ).show()
        if data is None:
            return
        if ops.edit_term(el, data["tag"], data["text"], data["source"]):
            self._changed()

    def _remove(self) -> None:
        el = self._selected()
        if el is None:
            return
        if not messagebox.askyesno("Supprimer", "Supprimer ce terme ?", parent=self):
            return
        ops.remove_term(el)
        self._changed()

    def _dedupe(self) -> None:
        if self._doc is None or self._component is None:
            return
        ca = self._doc.controlaccess(self._component)
        if ca is None:
            return
        n = ops.dedupe_terms(ca)
        if n:
            messagebox.showinfo("Dédoublonnage", f"{n} doublon(s) supprimé(s).", parent=self)
            self._changed()
        else:
            messagebox.showinfo("Dédoublonnage", "Aucun doublon trouvé.", parent=self)

    def _changed(self) -> None:
        self.refresh()
        self._on_change()
