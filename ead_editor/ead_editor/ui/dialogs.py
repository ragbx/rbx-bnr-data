"""Boîtes de dialogue modales réutilisables (édition dao et termes)."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk

AUDIENCE_VALUES = ("", "internal", "external")


class _ModalDialog(tk.Toplevel):
    """Base d'une boîte modale qui renvoie un dict ou None (annulation)."""

    def __init__(self, parent: tk.Misc, title: str) -> None:
        super().__init__(parent)
        self.title(title)
        self.transient(parent)
        self.resizable(False, False)
        self.result: dict | None = None
        self._body = ttk.Frame(self, padding=12)
        self._body.grid(row=0, column=0, sticky="nsew")
        self.build(self._body)
        self._build_buttons()
        self.bind("<Return>", lambda _e: self._on_ok())
        self.bind("<Escape>", lambda _e: self._on_cancel())
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)
        self.update_idletasks()
        self.grab_set()

    # à surcharger -------------------------------------------------------- #

    def build(self, body: ttk.Frame) -> None:  # pragma: no cover - UI
        raise NotImplementedError

    def validate(self) -> dict | None:  # pragma: no cover - UI
        raise NotImplementedError

    # interne ------------------------------------------------------------- #

    def _build_buttons(self) -> None:
        bar = ttk.Frame(self, padding=(12, 0, 12, 12))
        bar.grid(row=1, column=0, sticky="e")
        ttk.Button(bar, text="Annuler", command=self._on_cancel).grid(row=0, column=0, padx=4)
        ttk.Button(bar, text="Valider", command=self._on_ok).grid(row=0, column=1)

    def _on_ok(self) -> None:
        data = self.validate()
        if data is not None:
            self.result = data
            self.destroy()

    def _on_cancel(self) -> None:
        self.result = None
        self.destroy()

    def show(self) -> dict | None:
        self.wait_window()
        return self.result


class DaoDialog(_ModalDialog):
    """Saisie/édition d'une numérisation (href, role, audience)."""

    def __init__(
        self,
        parent: tk.Misc,
        roles: list[str],
        href: str = "",
        role: str = "",
        audience: str = "",
        title: str = "Numérisation",
    ) -> None:
        self._roles = roles
        self._init = (href, role, audience)
        super().__init__(parent, title)

    def build(self, body: ttk.Frame) -> None:
        href, role, audience = self._init
        ttk.Label(body, text="Lien (href) :").grid(row=0, column=0, sticky="w", pady=3)
        self.href = ttk.Entry(body, width=60)
        self.href.insert(0, href)
        self.href.grid(row=0, column=1, pady=3)

        ttk.Label(body, text="Rôle (role) :").grid(row=1, column=0, sticky="w", pady=3)
        self.role = ttk.Combobox(body, width=57, values=self._roles)
        self.role.set(role)
        self.role.grid(row=1, column=1, pady=3)

        ttk.Label(body, text="Audience :").grid(row=2, column=0, sticky="w", pady=3)
        self.audience = ttk.Combobox(body, width=57, values=AUDIENCE_VALUES, state="readonly")
        self.audience.set(audience)
        self.audience.grid(row=2, column=1, pady=3)
        self.href.focus_set()

    def validate(self) -> dict | None:
        href = self.href.get().strip()
        role = self.role.get().strip()
        if not href:
            messagebox.showwarning("Champ requis", "Le lien (href) est obligatoire.", parent=self)
            return None
        if not role:
            messagebox.showwarning("Champ requis", "Le rôle (role) est obligatoire.", parent=self)
            return None
        return {"href": href, "role": role, "audience": self.audience.get().strip()}


class TermDialog(_ModalDialog):
    """Saisie/édition d'un terme d'indexation (tag, texte, source)."""

    def __init__(
        self,
        parent: tk.Misc,
        tags: list[str],
        sources: list[str],
        tag: str = "subject",
        text: str = "",
        source: str = "",
        title: str = "Terme d'indexation",
    ) -> None:
        self._tags = tags
        self._sources = sources
        self._init = (tag, text, source)
        super().__init__(parent, title)

    def build(self, body: ttk.Frame) -> None:
        tag, text, source = self._init
        ttk.Label(body, text="Type :").grid(row=0, column=0, sticky="w", pady=3)
        self.tag = ttk.Combobox(body, width=57, values=self._tags, state="readonly")
        self.tag.set(tag)
        self.tag.grid(row=0, column=1, pady=3)

        ttk.Label(body, text="Terme :").grid(row=1, column=0, sticky="w", pady=3)
        self.text = ttk.Entry(body, width=60)
        self.text.insert(0, text)
        self.text.grid(row=1, column=1, pady=3)

        ttk.Label(body, text="Source :").grid(row=2, column=0, sticky="w", pady=3)
        self.source = ttk.Combobox(body, width=57, values=self._sources)
        self.source.set(source)
        self.source.grid(row=2, column=1, pady=3)
        self.text.focus_set()

    def validate(self) -> dict | None:
        text = self.text.get().strip()
        if not text:
            messagebox.showwarning("Champ requis", "Le terme ne peut pas être vide.", parent=self)
            return None
        return {
            "tag": self.tag.get().strip() or "subject",
            "text": text,
            "source": self.source.get().strip(),
        }
