"""Navigateur : arbre des composants (archdesc + c) du document EAD."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Callable

from ..model import EadDocument


class Navigator(ttk.Frame):
    """Affiche la hiérarchie de l'IR ; notifie la sélection d'un composant."""

    def __init__(self, parent: tk.Misc, on_select: Callable[[object], None]) -> None:
        super().__init__(parent)
        self._on_select = on_select
        self._iid_to_element: dict[str, object] = {}

        self.tree = ttk.Treeview(self, show="tree", selectmode="browse")
        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.tree.bind("<<TreeviewSelect>>", self._handle_select)

    def load(self, doc: EadDocument) -> None:
        self.tree.delete(*self.tree.get_children())
        self._iid_to_element.clear()
        element_to_iid: dict[object, str] = {}

        for info in doc.components():
            label = self._label(info)
            parent_el = self._parent_component(info.element)
            parent_iid = element_to_iid.get(parent_el, "")
            iid = self.tree.insert(parent_iid, "end", text=label, open=info.depth < 2)
            self._iid_to_element[iid] = info.element
            element_to_iid[info.element] = iid

        children = self.tree.get_children()
        if children:
            self.tree.selection_set(children[0])

    @staticmethod
    def _parent_component(element) -> object | None:
        node = element.getparent()
        while node is not None:
            if node.tag in ("archdesc", "c"):
                return node
            node = node.getparent()
        return None

    @staticmethod
    def _label(info) -> str:
        bits = [info.unitid or "(sans cote)"]
        if info.unittitle:
            bits.append(info.unittitle)
        prefix = "📁 " if info.tag == "archdesc" else ""
        level = f" [{info.level}]" if info.level else ""
        return f"{prefix}{' — '.join(bits)}{level}"

    def _handle_select(self, _event=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        element = self._iid_to_element.get(sel[0])
        if element is not None:
            self._on_select(element)
