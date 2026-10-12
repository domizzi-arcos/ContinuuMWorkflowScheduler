"""Single Python source of truth for scheduling-policy names, labels and styles.

Mirrors ``internal/algos/algos.go``; ``internal/algos/sync_test.go`` fails if
the lists here and there differ. Every Python script imports from this module
instead of hard-coding policy names.

Palette
-------
Eight validated categorical hues (OKLCH-checked: lightness band, chroma floor,
adjacent-pair CVD separation >= 8 and normal-vision separation >= 15 on a
light surface) are assigned in ``NAMES`` order, so neighbouring bars and
legend entries stay distinguishable. More than eight series cannot all be
told apart by hue, so identity is never carried by colour alone:

* ``random`` is drawn in neutral grey: it is the null baseline, not a hue.
* Ablations share their parent's hue and are marked by ``LINESTYLES`` and a
  hollow marker (``topsis_u0`` -> ``topsis``, ``best_fit_norm`` -> ``best_fit``).
* Every policy has a unique ``MARKERS`` entry. Use markers in every figure
  with more than three policies, and always in scatter plots, where all
  pairs (not just neighbours) must be told apart.
* Three hues (aqua, yellow, magenta) are below 3:1 contrast on white: show
  direct labels, markers or a table alongside them.
"""

# Five v1 policies, in v1 run order.
LEGACY = ["round_robin", "random", "best_fit", "worst_fit", "topsis"]

# v2 default set, in run and plot order.
NAMES = LEGACY + ["min_cost", "min_energy", "eft", "weighted_sum", "topsis_u0"]

# Registered but not run by default.
OPTIONAL = ["best_fit_norm"]

ALL = NAMES + OPTIONAL

LABELS = {
    "round_robin":   "Round Robin",
    "random":        "Random",
    "best_fit":      "Best Fit",
    "worst_fit":     "Worst Fit",
    "topsis":        "TOPSIS",
    "min_cost":      "Min-Cost",
    "min_energy":    "Min-Energy",
    "eft":           "EFT",
    "weighted_sum":  "Weighted Sum",
    "topsis_u0":     "TOPSIS (no util.)",
    "best_fit_norm": "Best Fit (norm.)",
}

COLORS = {
    "round_robin":   "#e34948",  # red
    "random":        "#7a7974",  # neutral grey (null baseline)
    "best_fit":      "#4a3aa7",  # violet
    "worst_fit":     "#008300",  # green
    "topsis":        "#2a78d6",  # blue (focus policy)
    "min_cost":      "#eb6834",  # orange
    "min_energy":    "#1baf7a",  # aqua
    "eft":           "#eda100",  # yellow
    "weighted_sum":  "#e87ba4",  # magenta
    "topsis_u0":     "#2a78d6",  # TOPSIS hue; distinguished by style
    "best_fit_norm": "#4a3aa7",  # Best Fit hue; distinguished by style
}

MARKERS = {
    "round_robin":   "o",
    "random":        "x",
    "best_fit":      "s",
    "worst_fit":     "D",
    "topsis":        "*",
    "min_cost":      "^",
    "min_energy":    "v",
    "eft":           "P",
    "weighted_sum":  "X",
    "topsis_u0":     "*",
    "best_fit_norm": "s",
}

# Short cell labels (heatmaps, tight tables); unique per policy.
ABBREV = {
    "round_robin":   "RR",
    "random":        "RND",
    "best_fit":      "BF",
    "worst_fit":     "WF",
    "topsis":        "TOP",
    "min_cost":      "MC",
    "min_energy":    "ME",
    "eft":           "EFT",
    "weighted_sum":  "WS",
    "topsis_u0":     "T-U0",
    "best_fit_norm": "BFN",
}

# Hollow markers distinguish ablations from their parent.
HOLLOW = {"topsis_u0", "best_fit_norm"}

LINESTYLES = {name: ("--" if name in HOLLOW else "-") for name in ALL}


def label(name: str) -> str:
    """Display label for a policy; unknown names are returned unchanged."""
    return LABELS.get(name, name)


def color(name: str) -> str:
    """Colour for a policy; unknown names get a light grey."""
    return COLORS.get(name, "#b5b4ae")


def ordered(present) -> list:
    """Policies in ``present``, in canonical order, unknown names last (sorted)."""
    present = list(dict.fromkeys(present))
    known = [a for a in ALL if a in present]
    return known + sorted(a for a in present if a not in ALL)
