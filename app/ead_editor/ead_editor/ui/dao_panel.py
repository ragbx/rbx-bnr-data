"""Panneau d'édition des numérisations (dao / daoloc) d'un composant."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable

from .. import operations as ops
from ..model import EadDocument
from .dialogs import DaoDialog


class DaoPanel(ttk.Frame):
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
        ttk.Button(toolbar, text="Dupliquer", command=self._duplicate).grid(row=0, column=2, padx=2)
        ttk.Button(toolbar, text="Supprimer", command=self._remove).grid(row=0, column=3, padx=2)

        cols = ("kind", "href", "role", "audience")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", selectmode="browse")
        for col, title, width in (
            ("kind", "Type", 70),
            ("href", "Lien (href)", 420),
            ("role", "Rôle", 180),
            ("audience", "Audience", 80),
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
        for entry in self._doc.dao_entries(self._component):
            iid = self.tree.insert(
                "", "end",
                values=(entry.kind, entry.href, entry.role, entry.audience),
            )
            self._row_to_element[iid] = entry.element

    # actions ------------------------------------------------------------- #

    def _selected(self):
        sel = self.tree.selection()
        return self._row_to_element.get(sel[0]) if sel else None

    def _roles(self) -> list[str]:
        return self._doc.known_roles() if self._doc else []

    def _add(self) -> None:
        if self._component is None:
            return
        data = DaoDialog(self, self._roles(), title="Ajouter une numérisation").show()
        if data is None:
            return
        into = self._component.find("daogrp") is not None and messagebox.askyesno(
            "Numérisation",
            "Ajouter dans le groupe <daogrp> existant ?\n"
            "(Non = créer un <dao> isolé)",
            parent=self,
        )
        ops.add_dao(self._component, data["href"], data["role"], data["audience"], into_daogrp=into)
        self._changed()

    def _edit(self) -> None:
        el = self._selected()
        if el is None:
            return
        data = DaoDialog(
            self, self._roles(),
            href=el.get("href", ""), role=el.get("role", ""), audience=el.get("audience", ""),
            title="Modifier la numérisation",
        ).show()
        if data is None:
            return
        if ops.set_dao_attr(el, data["href"], data["role"], data["audience"]):
            self._changed()

    def _duplicate(self) -> None:
        el = self._selected()
        if el is None or self._component is None:
            return
        ops.add_dao(
            self._component, el.get("href", ""), el.get("role", ""), el.get("audience", ""),
            into_daogrp=el.tag == "daoloc",
        )
        self._changed()

    def _remove(self) -> None:
        el = self._selected()
        if el is None:
            return
        if not messagebox.askyesno("Supprimer", "Supprimer cette numérisation ?", parent=self):
            return
        ops.remove_dao(el)
        self._changed()

    def _changed(self) -> None:
        self.refresh()
        self._on_change()
