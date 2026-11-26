"""Which spot coin a USDT perpetual stands for.

The match is by name only. A perpetual whose name was reused for a different coin
(LIT, for example) is matched to whatever spot coin carries that name.
"""

from __future__ import annotations

from typing import Iterable, Optional

# Perp base assets that are the spot coin times a multiplier.
MULTIPLIER_PREFIXES = ("1000000", "1000", "1M")

# Perp base asset -> spot base asset, where the names simply differ.
ALIASES = {
    "LUNA2": "LUNA",
    "DODOX": "DODO",
    "RAYSOL": "RAY",
}

# Preferred quote when one coin trades against several: the pair written to venue_symbol.
QUOTE_PRIORITY = ("USDT", "USDC", "FDUSD", "BTC")


def spot_base_candidates(perp_base: str) -> list[str]:
    """Spot base assets to try, most specific first.

    The exact name comes first because some multiplied names are spot coins too
    (1000SATS, 1000CAT, 1000CHEEMS, 1MBABYDOGE).
    """
    out = [ALIASES.get(perp_base, perp_base)]
    for prefix in MULTIPLIER_PREFIXES:
        if perp_base.startswith(prefix) and len(perp_base) > len(prefix):
            out.append(perp_base[len(prefix):])
            break
    return out


def resolve_spot_base(perp_base: str, spot_bases: Iterable[str]) -> Optional[str]:
    known = spot_bases if isinstance(spot_bases, (set, frozenset, dict)) else set(spot_bases)
    for candidate in spot_base_candidates(perp_base):
        if candidate in known:
            return candidate
    return None


def perp_base_of(symbol: str) -> Optional[str]:
    """Base asset of a USDT perpetual symbol; None for anything else (delivery, other quotes)."""
    if "_" in symbol or not symbol.endswith("USDT") or len(symbol) <= 4:
        return None
    return symbol[:-4]


def quote_rank(quote: str) -> int:
    return QUOTE_PRIORITY.index(quote) if quote in QUOTE_PRIORITY else len(QUOTE_PRIORITY)


def pick_pair(pairs: Iterable[tuple[str, str]]) -> str:
    """Pick one (symbol, quote) pair by QUOTE_PRIORITY, then by name."""
    return min(pairs, key=lambda item: (quote_rank(item[1]), item[0]))[0]
