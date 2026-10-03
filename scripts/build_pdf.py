"""Genera docs/01_documento_diseno.pdf a partir del Markdown, con portada, índice paginado y estilo propio.

Requiere pandoc, pdftotext (poppler) y Google Chrome.
Uso: python scripts/build_pdf.py
"""
from __future__ import annotations

import html
import os
import re
import subprocess
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")
SRC = os.path.join(DOCS, "01_documento_diseno.md")
OUT = os.path.join(DOCS, "01_documento_diseno.pdf")
STYLE = os.path.join(DOCS, "estilo")
CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")


def pandoc_html(path: str) -> None:
    subprocess.run([
        "pandoc", SRC, "-f", "gfm", "-t", "html5", "-s", "--toc", "--toc-depth=3",
        "--template", os.path.join(STYLE, "plantilla.html"),
        "--lua-filter", os.path.join(STYLE, "quitar_encabezado.lua"),
        "--css", os.path.join(STYLE, "documento.css"),
        "--metadata", "pagetitle=Cloud Provider Analytics · Documento de diseño v1",
        "--resource-path", DOCS, "--embed-resources", "-o", path,
    ], check=True, cwd=DOCS)


def print_pdf(html_path: str, pdf_path: str) -> None:
    subprocess.run([CHROME, "--headless", "--disable-gpu", "--no-pdf-header-footer",
                    f"--print-to-pdf={pdf_path}", f"file://{html_path}"],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def pages_text(pdf_path: str) -> list[str]:
    text = subprocess.run(["pdftotext", pdf_path, "-"], check=True, capture_output=True, text=True).stdout
    return [re.sub(r"\s+", " ", p) for p in text.split("\f")]


def toc_entries(doc: str) -> list[tuple[str, str]]:
    nav = doc[doc.index('<nav id="TOC">'):doc.index("</nav>")]
    return [(m.group(1), html.unescape(re.sub(r"<[^>]+>", "", m.group(2))))
            for m in re.finditer(r'<a href="#([^"]+)"[^>]*>(.*?)</a>', nav, re.S)]


def add_page_numbers(doc: str, pages: list[str]) -> str:
    """Busca cada título en orden (desde la página 3: 1 = portada, 2 = índice) y agrega su número al índice."""
    current = 2
    for anchor, title in toc_entries(doc):
        needle = re.sub(r"\s+", " ", title).strip()
        for i in range(current, len(pages)):
            if needle in pages[i]:
                current = i
                break
        page = current + 1
        doc = re.sub(rf'(<a href="#{re.escape(anchor)}"[^>]*>)(.*?)(</a>)',
                     lambda m: f'{m.group(1)}<span class="t">{m.group(2)}</span><span class="pg">{page}</span>{m.group(3)}',
                     doc, count=1, flags=re.S)
    return doc


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        html_path, pdf_path = os.path.join(tmp, "doc.html"), os.path.join(tmp, "pass1.pdf")
        pandoc_html(html_path)
        print_pdf(html_path, pdf_path)                       # pasada 1: ubicar títulos
        with open(html_path) as fh:
            doc = add_page_numbers(fh.read(), pages_text(pdf_path))
        with open(html_path, "w") as fh:
            fh.write(doc)
        print_pdf(html_path, OUT)                            # pasada 2: índice con páginas
    print(f"OK -> {os.path.relpath(OUT, ROOT)} ({len(pages_text(OUT)) - 1} páginas)")


if __name__ == "__main__":
    main()
