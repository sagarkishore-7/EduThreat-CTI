from __future__ import annotations

from typing import Optional

from bs4 import BeautifulSoup


def extract_last_page_from_numbers(
    pagination_root: Optional[BeautifulSoup],
) -> int:
    """
    Given a <ul class="page-numbers"> style block, return the highest numeric page.
    """
    if not pagination_root:
        return 1

    max_page = 1
    for node in pagination_root.select(".page-numbers"):
        text = node.get_text(strip=True)
        if text.isdigit():
            max_page = max(max_page, int(text))
        else:
            href = node.get("href", "")
            if href and href.rstrip("/").split("/")[-1].isdigit():
                max_page = max(max_page, int(href.rstrip("/").split("/")[-1]))
    return max_page


def extract_last_page_from_attr(
    pagination_root: Optional[BeautifulSoup],
    attr_name: str = "aria-label",
) -> int:
    """
    For Algolia-style pagination where anchors contain "Page X" in aria-label.
    """
    if not pagination_root:
        return 1

    max_page = 1
    for node in pagination_root.select("[aria-label]"):
        label = node.get(attr_name, "")
        parts = [int(x) for x in label.split() if x.isdigit()]
        if parts:
            max_page = max(max_page, parts[-1])
    return max_page

