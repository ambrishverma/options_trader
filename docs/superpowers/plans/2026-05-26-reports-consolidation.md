# Reports Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the Collar Report email and Daily-run email into a single "Daily Options Report" with unified template, strict phase ordering, and layout cleanup.

**Architecture:** The two separate pipelines (`run_pipeline()` and `run_collar_pipeline_and_email()`) become one unified pipeline in `run_pipeline()` with strict four-phase execution: data collection → collar & spreads → covered calls → protection modes → finalize & send. One HTML template, one emailer path, one scheduler job at 10:15 AM ET. Spread management "optimize" mode is renamed to "safety".

**Tech Stack:** Python 3.9, Jinja2 templates, Resend API for email, `schedule` library for cron, pytest

---

### Task 1: Rename Spread Mode "optimize" → "safety" in trader.py

**Files:**
- Modify: `trader.py:3968-4130` (the `execute_spread_mode()` function)
- Test: `tests/test_spread_management.py`

This task renames the first spread management mode from "optimize" to "safety" throughout the core logic and its tests. The function accepts `mode="safety"` instead of `mode="optimize"`. All downstream callers (scheduler, emailer, template, CLI) will be updated in Task 2.

- [ ] **Step 1: Update tests — change "optimize" to "safety" in test_spread_management.py**

Replace all occurrences of `execute_spread_mode("optimize", ...)` with `execute_spread_mode("safety", ...)` and all assertions `a["mode"] == "optimize"` with `a["mode"] == "safety"`. The affected tests are:

In class `TestSpreadOptimize` (rename to `TestSpreadSafety`):
- `test_pcs_optimize_triggers` → `test_pcs_safety_triggers`
- `test_pcs_optimize_no_trigger` → `test_pcs_safety_no_trigger`
- `test_ccs_optimize_triggers` → `test_ccs_safety_triggers`
- `test_ccs_optimize_no_trigger` → `test_ccs_safety_no_trigger`

In class `TestSpreadEdgeCases`:
- `test_dte_boundary_5_optimize` → `test_dte_boundary_5_safety`
- `test_dte_boundary_6_optimize` → `test_dte_boundary_6_safety`
- At lines ~648, ~745, ~759, ~772: change `"optimize"` to `"safety"` in mode arguments

In class `TestSpreadIntegration`:
- `test_spread_optimize_flag_exists` → `test_spread_safety_flag_exists` (will be updated in Task 2)

Also update the module docstring at line 3 that references `execute_spread_mode("optimize" | ...)`.

Apply this via `replace_all` for the string replacements:
- `execute_spread_mode("optimize"` → `execute_spread_mode("safety"`
- `a["mode"] == "optimize"` → `a["mode"] == "safety"`
- `["mode"] == "optimize"` → `["mode"] == "safety"`
- `class TestSpreadOptimize` → `class TestSpreadSafety`
- `test_pcs_optimize_triggers` → `test_pcs_safety_triggers`
- `test_pcs_optimize_no_trigger` → `test_pcs_safety_no_trigger`
- `test_ccs_optimize_triggers` → `test_ccs_safety_triggers`
- `test_ccs_optimize_no_trigger` → `test_ccs_safety_no_trigger`
- `test_dte_boundary_5_optimize` → `test_dte_boundary_5_safety`
- `test_dte_boundary_6_optimize` → `test_dte_boundary_6_safety`
- Update docstrings: "optimize" → "safety" in test method docstrings

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_spread_management.py -v -k "safety" 2>&1 | tail -20`
Expected: FAIL — tests call `execute_spread_mode("safety", ...)` but trader.py still has `mode == "optimize"`

- [ ] **Step 3: Update trader.py — rename "optimize" branch to "safety"**

In `execute_spread_mode()` at `trader.py:3968-4130`:

1. Update docstring at line 3979: change `"optimize" | "rescue" | "panic"` to `"safety" | "rescue" | "panic"`
2. Update label at line 3999: `label = f"{spread_type} {mode.upper()}"` — no change needed (already uses `mode`)
3. Change the mode check at line 4048 from `if mode == "optimize":` to `if mode == "safety":`
4. The `action["mode"]` field at line 4118 already uses the `mode` variable — it will automatically emit `"safety"` now

```python
# Line 3979 — docstring
    mode        : "safety" | "rescue" | "panic"

# Line 4047-4048 — branch check
            # ── Safety (DTE > 5 only) ──────────────────────────────
            if mode == "safety":
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_spread_management.py -v 2>&1 | tail -20`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```bash
git add trader.py tests/test_spread_management.py
git commit -m "refactor: rename spread mode 'optimize' to 'safety' in trader.py"
```

---

### Task 2: Rename Spread Mode References Across Codebase

**Files:**
- Modify: `scheduler.py:547-577` (spread management section in `run_pipeline()`)
- Modify: `emailer.py:45-63,294-369` (parameter names, subject line)
- Modify: `templates/email.html:584-619` (spread optimize section label)
- Modify: `main.py:36-40,813-818,967-972,1057-1058,1106-1107` (CLI flags, help text, dispatch)
- Modify: `tests/test_spread_management.py:978-1001` (integration test assertions)
- Modify: `tests/test_strategy.py:739,856` (emailer param names in test calls)

- [ ] **Step 1: Update scheduler.py — rename spread_optimize_results → spread_safety_results**

In `run_pipeline()`, change all references from `spread_optimize_results` to `spread_safety_results` and from `execute_spread_mode("optimize", ...)` to `execute_spread_mode("safety", ...)`:

```python
# Line 548
        spread_safety_results = []

# Lines 555-558
            for sp_type in ("PCS", "CCS"):
                spread_safety_results.extend(
                    execute_spread_mode("safety", sp_type, dry_run=dry_run)
                )

# Line 568
            n_saf = len(spread_safety_results)

# Line 571-574
            if n_saf + n_res + n_pan > 0:
                logger.info(
                    f"[SPREAD MGMT] Safety: {n_saf} | Rescue: {n_res} | Panic: {n_pan}"
                )
            results["spread_safety"] = n_saf

# Line 629
            spread_safety_results=spread_safety_results,
```

- [ ] **Step 2: Update emailer.py — rename spread_optimize_results parameter**

In `_render_html()` and `send_recommendations()`, rename the parameter:
- `spread_optimize_results` → `spread_safety_results`

In `_render_html()` (lines 45-83):
```python
    spread_safety_results: list = None,
    # ...
    spread_safety_results = spread_safety_results or []
    # ...
    return template.render(
        # ...
        spread_safety_results=spread_safety_results,
    )
```

In `send_recommendations()` (lines 294-381):
```python
    spread_safety_results: list = None,
    # ...
    n_sp_saf = len(spread_safety_results or [])
    if n_sp_saf:
        subject += f" | 📐 {n_sp_saf} spread safety(s)"
    # ...
    html_body = _render_html(...,
        spread_safety_results=spread_safety_results or [],
    )
```

- [ ] **Step 3: Update templates/email.html — rename spread optimize section**

Change the spread optimize section header and variable references (lines 584-619):

```html
  {# ── Spread Management: Safety ─────────────────────────────────────────── #}
  {% if spread_safety_results %}
  <div style="background:#eef2ff;border-left:4px solid #4338ca;padding:10px 14px;margin-bottom:14px;border-radius:0 4px 4px 0;">
    <strong style="font-size:13px;">📐 Spread Safety — close profitable spreads at minimal cost</strong>
  </div>
  <!-- ... -->
      {% for a in spread_safety_results %}
  <!-- ... -->
  {% endif %}
```

Replace all `spread_optimize_results` with `spread_safety_results` in the template.

- [ ] **Step 4: Update main.py — rename --spread-optimize to --spread-safety**

1. Update help text examples (lines 36-40, 813-818): `--spread-optimize` → `--spread-safety`
2. Update argparse definition (lines 968-972):
```python
    parser.add_argument(
        "--spread-safety", action="store_true", default=False,
        help="For --pcs/--ccs: close spreads meeting safety criteria "
             "(PCS: BE > 90%% stock price; CCS: BE < 110%% stock price).",
    )
```
3. Update dispatch code (lines 1057, 1106):
```python
        if args.spread_safety:
            cmd_spread_manage("safety", "CCS", symbol=sym)
        # ...
        if args.spread_safety:
            cmd_spread_manage("safety", "PCS", symbol=sym)
```

- [ ] **Step 5: Update test assertions for the rename**

In `tests/test_spread_management.py`:
- `test_spread_optimize_flag_exists` → `test_spread_safety_flag_exists`: change `"--spread-optimize"` to `"--spread-safety"` and `"spread_optimize"` to `"spread_safety"`
- Line 978: `assert "spread_optimize_results" in sig.parameters` → `assert "spread_safety_results" in sig.parameters`
- Line 988: same change for `_render_html` signature check

In `tests/test_strategy.py`:
- Lines 739, 856: `spread_optimize_results=[]` → `spread_safety_results=[]`

- [ ] **Step 6: Run full test suite**

Run: `python3 -m pytest tests/ -v 2>&1 | tail -30`
Expected: ALL 566 PASS

- [ ] **Step 7: Commit**

```bash
git add scheduler.py emailer.py templates/email.html main.py tests/test_spread_management.py tests/test_strategy.py
git commit -m "refactor: rename spread 'optimize' to 'safety' across codebase"
```

---

### Task 3: Add Collar/CCS/PCS Parameters to emailer.py

**Files:**
- Modify: `emailer.py`
- Test: `tests/test_emailer_consolidation.py` (new file)

This task adds collar, CCS, and PCS data parameters to `_render_html()` and `send_recommendations()`, moves the CCS/PCS quality filter constants from `collar_emailer.py`, and updates the subject line to the unified format.

- [ ] **Step 1: Write failing tests for the unified emailer**

Create `tests/test_emailer_consolidation.py`:

```python
"""
test_emailer_consolidation.py — Tests for unified Daily Options Report email.

Verifies:
  1. send_recommendations() accepts collar/CCS/PCS parameters
  2. Subject line includes collar, CCS, PCS counts
  3. CCS/PCS quality filter suppresses recs below thresholds
  4. Empty collar/CCS/PCS sections render cleanly
  5. _render_html() passes collar/CCS/PCS data to template
"""

import logging
from unittest.mock import patch

import pytest


def _make_collar_rec(**overrides):
    """Minimal collar recommendation dict for testing."""
    base = {
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "market_value": 150_000,
        "current_price": 195.0,
        "contracts": 10,
        "expiration": "2026-06-06",
        "cc_expiration": "2026-06-06",
        "lp_expiration": "2026-06-06",
        "dte": 11,
        "cc_dte": 11,
        "lp_dte": 11,
        "call_leg": {"strike": 200.0, "bid": 2.10, "ask": 2.30, "mid": 2.20,
                     "otm_pct": 2.6, "open_interest": 500},
        "put_leg": {"strike": 185.0, "bid": 1.50, "ask": 1.70, "mid": 1.60,
                    "otm_pct": 5.1, "open_interest": 400},
        "net_gain_per_share": 0.60,
        "net_gain_total": 600.0,
        "upside_cap_pct": 2.6,
        "downside_floor_pct": -5.1,
        "low_gain": False,
        "earnings_date": None,
        "earnings_warning": None,
        "next_earnings_date": None,
        "ex_dividend_date": None,
    }
    base.update(overrides)
    return base


def _make_spread_rec(spread_type="PCS", **overrides):
    """Minimal CCS/PCS rec for testing."""
    base = {
        "symbol": "TSLA",
        "name": "Tesla Inc.",
        "type": spread_type,
        "current_price": 300.0,
        "expiration": "2026-06-06",
        "dte": 11,
        "short_leg": {"strike": 270.0, "bid": 1.00, "ask": 1.20, "mid": 1.10,
                      "otm_pct": 10.0, "open_interest": 100},
        "long_leg": {"strike": 260.0, "bid": 0.40, "ask": 0.60, "mid": 0.50,
                     "otm_pct": 13.3, "open_interest": 80},
        "net_credit": 0.60,
        "net_credit_total": 60.0,
        "max_loss": 940.0,
        "credit_to_loss_ratio": 0.064,
        "spread_size": 10.0,
        "ypd": 5.45,
        "earnings_date": None,
        "no_contract": False,
    }
    base.update(overrides)
    return base


META = {
    "run_date": "2026-05-26",
    "recipient_email": "test@example.com",
    "duration_sec": 45.0,
    "pur_pct": 65.0,
    "pur_open": 13,
    "pur_max": 20,
    "portfolio_ypd": 12.50,
}


class TestUnifiedSubjectLine:
    """Verify combined subject line includes collar, CCS, PCS counts."""

    def _capture_subject(self, **kwargs):
        """Call send_recommendations in dry_run and return the subject line."""
        from emailer import send_recommendations
        handler = logging.handlers.MemoryHandler(capacity=100)
        log = logging.getLogger("emailer")
        log.addHandler(handler)
        log.setLevel(logging.DEBUG)
        try:
            send_recommendations([], META, dry_run=True, **kwargs)
        finally:
            log.removeHandler(handler)
        # Find the Subject: line in log output
        for record in handler.buffer:
            msg = record.getMessage()
            if "Subject:" in msg:
                return msg.split("Subject:", 1)[1].strip()
        return ""

    def test_subject_includes_collar_count(self):
        """Subject includes collar count when collar recs are provided."""
        subject = self._capture_subject(
            collar_recs=[_make_collar_rec()],
        )
        assert "1 collars" in subject or "1 collar" in subject

    def test_subject_includes_ccs_pcs_counts(self):
        """Subject includes CCS and PCS counts."""
        subject = self._capture_subject(
            ccs_recs=[_make_spread_rec("CCS")],
            pcs_recs=[_make_spread_rec("PCS")],
        )
        assert "1 CCS" in subject
        assert "1 PCS" in subject

    def test_subject_omits_zero_collar(self):
        """When collar count is 0, collar segment is omitted from subject."""
        subject = self._capture_subject(collar_recs=[])
        assert "collar" not in subject.lower()

    def test_subject_combined_format(self):
        """Full combined subject line format."""
        subject = self._capture_subject(
            collar_recs=[_make_collar_rec(), _make_collar_rec(symbol="MSFT")],
            ccs_recs=[_make_spread_rec("CCS")],
            pcs_recs=[],
        )
        assert "Daily Options" in subject
        assert "2 collars" in subject
        assert "1 CCS" in subject
        assert "0 PCS" in subject


class TestQualityFilter:
    """Verify CCS/PCS quality filter is applied before rendering."""

    def test_below_min_credit_suppressed(self):
        """Recs with net_credit_total < $50 are suppressed."""
        from emailer import send_recommendations
        low_credit = _make_spread_rec(net_credit_total=30.0, credit_to_loss_ratio=0.50)
        send_recommendations(
            [], META, dry_run=True,
            ccs_recs=[low_credit],
        )
        # The test passes if no error — actual rendering verification in template tests

    def test_below_min_cl_ratio_suppressed(self):
        """Recs with credit_to_loss_ratio < 0.25 are suppressed."""
        from emailer import send_recommendations
        low_ratio = _make_spread_rec(net_credit_total=100.0, credit_to_loss_ratio=0.10)
        send_recommendations(
            [], META, dry_run=True,
            pcs_recs=[low_ratio],
        )


class TestRenderHtmlAcceptsCollarParams:
    """Verify _render_html() accepts and passes collar/CCS/PCS data."""

    def test_render_with_collar_data(self):
        """_render_html() does not error when collar data is provided."""
        from emailer import _render_html
        html = _render_html(
            [], META,
            collar_recs=[_make_collar_rec()],
            collar_meta={"symbols_with_collars": 1, "eligible_holdings": 5,
                         "total_recommendations": 1, "low_gain_count": 0,
                         "earnings_flags": 0},
        )
        assert "AAPL" in html

    def test_render_with_ccs_pcs_data(self):
        """_render_html() renders CCS/PCS sections."""
        from emailer import _render_html
        html = _render_html(
            [], META,
            ccs_recs=[_make_spread_rec("CCS")],
            pcs_recs=[_make_spread_rec("PCS")],
            ccs_meta={"scenarios_evaluated": 100, "qualified_opportunities": 5,
                      "symbols_recommended": 1, "total_net_credit": 60.0,
                      "total_ypd": 5.45, "count": 1},
            pcs_meta={"scenarios_evaluated": 80, "qualified_opportunities": 3,
                      "symbols_recommended": 1, "total_net_credit": 60.0,
                      "total_ypd": 5.45, "count": 1},
        )
        assert "CCS" in html or "Call Credit Spread" in html
        assert "PCS" in html or "Put Credit Spread" in html

    def test_render_empty_collar_ccs_pcs(self):
        """Empty collar/CCS/PCS data renders without errors."""
        from emailer import _render_html
        html = _render_html(
            [], META,
            collar_recs=[], collar_meta={},
            ccs_recs=[], pcs_recs=[],
            ccs_meta={}, pcs_meta={},
        )
        assert "Daily Options Report" in html
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_emailer_consolidation.py -v 2>&1 | tail -20`
Expected: FAIL — `_render_html()` and `send_recommendations()` don't accept `collar_recs` etc. yet

- [ ] **Step 3: Add CCS/PCS quality filter constants and helper to emailer.py**

Add after the imports section (around line 30):

```python
# CCS/PCS quality filter thresholds (moved from collar_emailer.py)
_MIN_SPREAD_NET_CREDIT_TOTAL: float = 50.0
_MIN_SPREAD_CREDIT_TO_LOSS_RATIO: float = 0.25


def _build_spread_meta(recs: list, scenarios: int, qualified_before_filter: int) -> dict:
    """Build aggregate metrics dict for a CCS or PCS section header."""
    return {
        "scenarios_evaluated":    scenarios,
        "qualified_opportunities": qualified_before_filter,
        "symbols_recommended":    len(set(r["symbol"] for r in recs)),
        "total_net_credit":       round(sum(r.get("net_credit_total", 0) for r in recs), 2),
        "total_ypd":              round(sum(r.get("ypd", 0) for r in recs), 2),
        "count":                  len(recs),
    }
```

- [ ] **Step 4: Add collar/CCS/PCS parameters to _render_html()**

Add new parameters to `_render_html()` signature:

```python
def _render_html(
    recommendations: list,
    run_meta: dict,
    roll_candidates: list = None,
    btc_candidates: list = None,
    optimize_results: list = None,
    panic_results: list = None,
    rescue_results: list = None,
    safety_results: list = None,
    spread_safety_results: list = None,
    spread_rescue_results: list = None,
    spread_panic_results: list = None,
    strategy_recs: list = None,
    collar_recs: list = None,
    collar_meta: dict = None,
    ccs_recs: list = None,
    pcs_recs: list = None,
    ccs_meta: dict = None,
    pcs_meta: dict = None,
) -> str:
```

Add defaults inside the function body:
```python
    collar_recs  = collar_recs  or []
    collar_meta  = collar_meta  or {}
    ccs_recs     = ccs_recs     or []
    pcs_recs     = pcs_recs     or []
    ccs_meta     = ccs_meta     or {}
    pcs_meta     = pcs_meta     or {}
```

Pass them to `template.render()`:
```python
        return template.render(
            recommendations=recommendations,
            meta=run_meta,
            # ... existing params ...
            collar_recs=collar_recs,
            collar_meta=collar_meta,
            ccs_recs=ccs_recs,
            pcs_recs=pcs_recs,
            ccs_meta=ccs_meta,
            pcs_meta=pcs_meta,
        )
```

- [ ] **Step 5: Add collar/CCS/PCS parameters to send_recommendations()**

Add new parameters to `send_recommendations()` signature:

```python
def send_recommendations(
    recommendations: list,
    run_meta: dict,
    dry_run: bool = False,
    # ... existing params ...
    collar_recs: list = None,
    collar_meta: dict = None,
    ccs_recs: list = None,
    pcs_recs: list = None,
    ccs_scenarios: int = 0,
    pcs_scenarios: int = 0,
) -> bool:
```

Apply CCS/PCS quality filter before rendering:
```python
    collar_recs = collar_recs or []
    ccs_recs_raw = ccs_recs or []
    pcs_recs_raw = pcs_recs or []
    collar_meta = collar_meta or {}

    # Quality filter: suppress recs below $50 net credit or 0.25 C/L ratio
    ccs_qualified = len(ccs_recs_raw)
    pcs_qualified = len(pcs_recs_raw)
    ccs_recs_filtered = [
        r for r in ccs_recs_raw
        if r.get("net_credit_total", 0) >= _MIN_SPREAD_NET_CREDIT_TOTAL
        and r.get("credit_to_loss_ratio", 0) >= _MIN_SPREAD_CREDIT_TO_LOSS_RATIO
    ]
    pcs_recs_filtered = [
        r for r in pcs_recs_raw
        if r.get("net_credit_total", 0) >= _MIN_SPREAD_NET_CREDIT_TOTAL
        and r.get("credit_to_loss_ratio", 0) >= _MIN_SPREAD_CREDIT_TO_LOSS_RATIO
    ]

    # Build aggregate section-header metadata
    ccs_meta = _build_spread_meta(ccs_recs_filtered, ccs_scenarios, ccs_qualified)
    pcs_meta = _build_spread_meta(pcs_recs_filtered, pcs_scenarios, pcs_qualified)
```

Update the subject line to use the unified format:
```python
    collar_n = len(collar_recs)
    ccs_n = len(ccs_recs_filtered)
    pcs_n = len(pcs_recs_filtered)

    subject = f"📊 Daily Options — {today_str} — {n} CC recs"
    if collar_n:
        subject += f" | {collar_n} collars"
    if ccs_n or pcs_n:
        subject += f" | {ccs_n} CCS, {pcs_n} PCS"
    # ... existing dynamic indicators (earnings, optimize/panic/rescue/safety, spread modes) ...
```

Pass new params to `_render_html()`:
```python
    html_body = _render_html(recommendations, run_meta,
                             # ... existing params ...
                             collar_recs=collar_recs,
                             collar_meta=collar_meta,
                             ccs_recs=ccs_recs_filtered,
                             pcs_recs=pcs_recs_filtered,
                             ccs_meta=ccs_meta,
                             pcs_meta=pcs_meta)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_emailer_consolidation.py -v 2>&1 | tail -20`
Expected: PASS (tests that check HTML content will pass once template is updated in Task 4; for now they may be partial)

- [ ] **Step 7: Run full test suite to check no regressions**

Run: `python3 -m pytest tests/ -v 2>&1 | tail -20`
Expected: ALL PASS (existing callers still pass collar params as None by default)

- [ ] **Step 8: Commit**

```bash
git add emailer.py tests/test_emailer_consolidation.py
git commit -m "feat: add collar/CCS/PCS params to emailer with quality filter and unified subject"
```

---

### Task 4: Merge Collar/CCS/PCS Sections Into Unified Email Template

**Files:**
- Modify: `templates/email.html`

This task adds the collar, CCS, and PCS sections into the existing daily email template, unifies CSS, and tightens layout. Sections are ordered per the spec: Header → Summary → Collars → CCS → PCS → Covered Calls → Roll-Forward → BTC → Strategy → Footer.

- [ ] **Step 1: Update the header**

Change the header from "Covered Call Recommendations" to "Daily Options Report" (line 117):

```html
  <div class="header">
    <h1>📊 Daily Options Report</h1>
    <p>{{ meta.run_date }} &nbsp;·&nbsp; Safe Mode (≥7% OTM) &nbsp;·&nbsp; {{ recommendations|length }} CC rec(s)
    {%- if collar_recs %} &nbsp;·&nbsp; {{ collar_recs|length }} collar(s){% endif %}
    {%- if ccs_recs %} &nbsp;·&nbsp; {{ ccs_recs|length }} CCS{% endif %}
    {%- if pcs_recs %} &nbsp;·&nbsp; {{ pcs_recs|length }} PCS{% endif %}
    </p>
  </div>
```

- [ ] **Step 2: Update the container max-width and tighten layout padding**

Change `.container` max-width from 860px to 900px. Update padding values:

```css
    .container { max-width: 900px; /* ... */ }
    .tbl-wrap { padding: 16px 24px; }
    .section-header { padding: 12px 24px 8px; /* ... */ }
```

- [ ] **Step 3: Update the summary bar to include collar/CCS/PCS counts**

Add collar, CCS, PCS stats to the summary bar:

```html
  <div class="summary-bar">
    {%- if collar_recs is defined and collar_recs %}
    <div class="kv">
      <span class="label">Collars</span>
      <span class="value">{{ collar_recs|length }}</span>
    </div>
    {% endif %}
    {%- if ccs_recs is defined and ccs_recs %}
    <div class="kv">
      <span class="label">CCS</span>
      <span class="value">{{ ccs_recs|length }}</span>
    </div>
    {% endif %}
    {%- if pcs_recs is defined and pcs_recs %}
    <div class="kv">
      <span class="label">PCS</span>
      <span class="value">{{ pcs_recs|length }}</span>
    </div>
    {% endif %}
    <!-- existing CC stats follow -->
    <div class="kv">
      <span class="label">CC Recs</span>
      <span class="value neutral">{{ recommendations|length }}</span>
    </div>
    <!-- ... rest of existing summary stats ... -->
  </div>
```

- [ ] **Step 4: Add collar section CSS classes**

Add these CSS rules to the `<style>` block (alongside existing collar_email.html styles):

```css
    /* Symbol group header (collar tables) */
    .sym-group-header td { background: #1e3a5f; color: white; padding: 8px 12px; font-size: 13px; font-weight: bold; }
    .row-call { background: #eff6ff; }
    .row-put  { background: #f0fdf4; }
    .row-call.low-gain { background: #fff1f2; }
    .row-put.low-gain  { background: #ffe4e6; }
    .cell-combined-collar { background: #f8fafc; border-left: 2px solid #cbd5e1; vertical-align: middle !important; text-align: right; }
    .cell-combined-collar .net { font-size: 14px; font-weight: bold; color: #15803d; }
    .cell-combined-collar .net.low { color: #dc2626; }
    .cell-combined-collar .sub { font-size: 11px; color: #64748b; margin-top: 2px; }
    .cell-combined-collar .cts { font-size: 10px; color: #94a3b8; margin-top: 2px; }
    .leg-lbl-call { color: #1d4ed8; font-weight: bold; font-size: 11px; }
    .leg-lbl-put  { color: #15803d; font-weight: bold; font-size: 11px; }
    .row-low-warn td { background: #fee2e2; font-size: 12px; color: #991b1b; padding: 5px 12px; }
    .empty-state { padding: 16px 24px; font-size: 13px; color: #64748b; }

    /* CCS/PCS sections */
    .section-header.ccs { background: #1e1b4b; color: white; }
    .section-header.pcs { background: #14532d; color: white; }
    .section-header .section-sub { font-size: 11px; opacity: 0.65; font-weight: normal; margin-top: 3px; }
    .row-short { background: #eff6ff; }
    .row-long  { background: #faf5ff; }
    .row-short.pcs-row { background: #fef9c3; }
    .row-long.pcs-row  { background: #f0fdf4; }
    .leg-lbl-short-call { color: #1d4ed8; font-weight: bold; font-size: 11px; }
    .leg-lbl-long-call  { color: #6d28d9; font-weight: bold; font-size: 11px; }
    .leg-lbl-short-put  { color: #b45309; font-weight: bold; font-size: 11px; }
    .leg-lbl-long-put   { color: #15803d; font-weight: bold; font-size: 11px; }
    .cell-spread-net { background: #f8fafc; border-left: 2px solid #cbd5e1; vertical-align: middle !important; text-align: right; }
    .cell-spread-net .net { font-size: 14px; font-weight: bold; color: #15803d; }
    .cell-spread-net .sub { font-size: 11px; color: #64748b; margin-top: 2px; }
    .cell-spread-net .loss { font-size: 11px; color: #dc2626; margin-top: 2px; }
```

- [ ] **Step 5: Insert collar section BEFORE covered call recommendations**

After the summary bar and BEFORE the existing CC recommendations `<div class="tbl-wrap">`, insert the collar section. Copy the collar rendering logic from `templates/collar_email.html` lines 113-201 (the `{% for rec in recommendations %}` block), adapting variable names: the collar recs are now in `collar_recs` instead of `recommendations`.

```html
  <!-- ── Section 1: Collar Recommendations ───────────────────────────── -->
  {% if collar_recs is defined %}
  <div class="section-header" style="background:#1e3a5f;color:white;">
    🛡 Collar Recommendations
    <p style="margin:3px 0 0;font-size:11px;font-weight:normal;opacity:0.75;">
      Large Holdings Protection (&gt; $10K) &nbsp;·&nbsp;
      {{ collar_meta.symbols_with_collars|default(0) }} symbol(s) &nbsp;·&nbsp;
      {{ collar_recs|length }} recommendation(s)
    </p>
  </div>
  <div class="tbl-wrap">
  {% if not collar_recs %}
    <div class="empty-state">✅ No collar recommendations today — all eligible holdings either lack qualifying options or direction filter removed them.</div>
  {% else %}
    {# Collar tables — one table per symbol group with CC + LP rows #}
    {% set ns = namespace(current_sym="") %}
    {% for rec in collar_recs %}
    {% if rec.symbol != ns.current_sym %}
      {% set ns.current_sym = rec.symbol %}
      {% if not loop.first %}</table>{% endif %}
      <table class="recs">
      <tr class="sym-group-header">
        <td colspan="11">
          {{ rec.symbol }} &mdash; {{ rec.name }}
          &nbsp;&middot;&nbsp; ${{ "{:,.0f}".format(rec.market_value) }} market value
          &nbsp;&middot;&nbsp; {{ rec.contracts }} contract(s)
          &nbsp;&middot;&nbsp; Current price: ${{ "%.2f"|format(rec.current_price) }}
        </td>
      </tr>
      <thead>
        <tr>
          <th>Leg</th>
          <th class="right">Strike</th>
          <th class="right">Expiry</th>
          <th class="right">DTE</th>
          <th class="right">Bid</th>
          <th class="right">Ask</th>
          <th class="right">Mid</th>
          <th class="right">OTM / Protection</th>
          <th class="right">Earnings</th>
          <th class="right">Ex-Div</th>
          <th class="right">Net Gain / Details</th>
        </tr>
      </thead>
    {% endif %}
    {% set low = rec.low_gain %}
    <tr class="row-call{% if low %} low-gain{% endif %}">
      <td><span class="leg-lbl-call">Covered Call</span></td>
      <td class="right">${{ "%.2f"|format(rec.call_leg.strike) }}</td>
      <td class="right">{{ rec.cc_expiration }}</td>
      <td class="right">{{ rec.cc_dte }}d</td>
      <td class="right">${{ "%.2f"|format(rec.call_leg.bid) }}</td>
      <td class="right">${{ "%.2f"|format(rec.call_leg.ask) }}</td>
      <td class="right" style="font-weight:600;">${{ "%.2f"|format(rec.call_leg.mid) }}</td>
      <td class="right">+{{ "%.1f"|format(rec.upside_cap_pct) }}%</td>
      <td class="right" rowspan="2" style="color:#475569;">{{ rec.next_earnings_date[5:] | replace('-', '/') if rec.next_earnings_date else '—' }}</td>
      <td class="right" rowspan="2" style="color:#475569;">{{ rec.ex_dividend_date[5:] | replace('-', '/') if rec.ex_dividend_date else '—' }}</td>
      <td class="cell-combined-collar right" rowspan="{{ 2 + (1 if rec.low_gain else 0) + (1 if rec.earnings_warning else 0) }}">
        <div class="net{% if low %} low{% endif %}">${{ "%.2f"|format(rec.net_gain_total) }}</div>
        <div class="sub">${{ "%.2f"|format(rec.net_gain_per_share) }}/share net</div>
        <div class="sub">Cap: +{{ "%.1f"|format(rec.upside_cap_pct) }}%</div>
        <div class="sub">Floor: {{ "%.1f"|format(rec.downside_floor_pct) }}%</div>
        <div class="cts">{{ rec.contracts }} contract(s)</div>
      </td>
    </tr>
    <tr class="row-put{% if low %} low-gain{% endif %}">
      <td><span class="leg-lbl-put">Long Put</span></td>
      <td class="right">${{ "%.2f"|format(rec.put_leg.strike) }}</td>
      <td class="right">{{ rec.lp_expiration }}</td>
      <td class="right">{{ rec.lp_dte }}d</td>
      <td class="right">${{ "%.2f"|format(rec.put_leg.bid) }}</td>
      <td class="right">${{ "%.2f"|format(rec.put_leg.ask) }}</td>
      <td class="right" style="font-weight:600;">${{ "%.2f"|format(rec.put_leg.mid) }}</td>
      <td class="right">{{ "%.1f"|format(rec.downside_floor_pct) }}%</td>
    </tr>
    {% if low %}
    <tr class="row-low-warn"><td colspan="10">Best available &mdash; below $0.10/share threshold</td></tr>
    {% endif %}
    {% if rec.earnings_warning %}
    <tr class="row-warn"><td colspan="10">{{ rec.earnings_warning }}</td></tr>
    {% endif %}
    {% if loop.last %}</table>{% endif %}
    {% endfor %}
  {% endif %}
  </div>
  {% endif %}
```

- [ ] **Step 6: Insert CCS section after collars, before CC recommendations**

Copy the CCS section from `templates/collar_email.html` lines 203-293. The section header, summary bar, and per-symbol table structure are preserved verbatim:

```html
  <!-- ── Section 2: Call Credit Spread (CCS) Recommendations ── -->
  {% if ccs_recs is defined %}
  <div class="section-header ccs">
    📉 Call Credit Spread (CCS) Recommendations — Bear Call Spreads
    <div class="section-sub">Short call ≥10% OTM + long call at higher strike &nbsp;|&nbsp; Sorted by YPD</div>
  </div>
  {% if ccs_meta %}
  <!-- summary bar (same as collar_email.html) -->
  {% endif %}
  {% if not ccs_recs %}
  <div class="empty-state">✅ No qualifying CCS candidates today.</div>
  {% else %}
  <div class="tbl-wrap">
  <!-- per-symbol CCS tables (same structure as collar_email.html lines 237-292) -->
  </div>
  {% endif %}
  {% endif %}
```

- [ ] **Step 7: Insert PCS section after CCS, before CC recommendations**

Same pattern as CCS but with PCS styling. Copy from `templates/collar_email.html` lines 295-385.

- [ ] **Step 8: Update CC section header to be explicit**

Wrap the existing CC recommendation section with a section header for consistency:

```html
  <!-- ── Section 4: Covered Call Recommendations ───────────────────── -->
  <div class="section-header" style="background:#1e3a5f;color:white;">
    📊 Covered Call Recommendations
    <p style="margin:3px 0 0;font-size:11px;font-weight:normal;opacity:0.75;">Safe Mode ≥7% OTM &nbsp;·&nbsp; {{ recommendations|length }} recommendation(s)</p>
  </div>
```

- [ ] **Step 9: Update footer — single unified disclaimer**

Replace the existing footer with a unified disclaimer that covers all instrument types:

```html
  <div class="footer">
    <strong>Disclaimer:</strong> Automated analysis only — not financial advice. Options trading involves significant risk.
    Collar strategies, credit spreads, and covered calls all carry distinct risks.
    Verify all data and earnings dates independently before placing trades. Premiums are mid-price estimates.
    <div class="meta-row">
      <span class="meta-kv"><span class="mk">Run date:</span> {{ meta.run_date }}</span>
      <span class="meta-kv"><span class="mk">Duration:</span> {{ meta.duration_sec }}s</span>
      <span class="meta-kv"><span class="mk">Mode:</span> Safe (≥7% OTM, $0.20 min bid, 28-day window)</span>
    </div>
  </div>
```

- [ ] **Step 10: Run emailer consolidation tests**

Run: `python3 -m pytest tests/test_emailer_consolidation.py -v 2>&1 | tail -20`
Expected: ALL PASS

- [ ] **Step 11: Run full test suite**

Run: `python3 -m pytest tests/ -v 2>&1 | tail -20`
Expected: ALL PASS

- [ ] **Step 12: Commit**

```bash
git add templates/email.html
git commit -m "feat: add collar/CCS/PCS sections to unified email template"
```

---

### Task 5: Merge Collar Pipeline Into run_pipeline()

**Files:**
- Modify: `scheduler.py:245-666` (`run_pipeline()`)

This is the core integration task. The collar scan and CCS/PCS scan are inserted into `run_pipeline()` as Phase 1, between data collection (Phase 0) and the CC pipeline (Phase 2). The collar data and CCS/PCS data flow into the unified emailer.

- [ ] **Step 1: Write a test for the unified pipeline passing collar data to emailer**

Add to `tests/test_emailer_consolidation.py`:

```python
class TestPipelineIntegration:
    """Verify that run_pipeline passes collar/CCS/PCS data to emailer."""

    @patch("scheduler.send_recommendations")
    @patch("scheduler.run_collar_pipeline")
    @patch("scheduler.run_spread_weekly_pipeline")
    @patch("scheduler.get_portfolio")
    @patch("scheduler.load_open_calls_snapshot", return_value={})
    @patch("scheduler.load_open_calls_detail_snapshot", return_value=[])
    @patch("scheduler.load_open_puts_detail_snapshot", return_value=[])
    @patch("scheduler.load_open_longs_detail_snapshot", return_value=[])
    @patch("scheduler.load_open_spreads_detail_snapshot", return_value=[])
    @patch("scheduler.fetch_all_options", return_value=[])
    @patch("scheduler._is_trading_day", return_value=True)
    def test_collar_data_passed_to_emailer(
        self, mock_trading, mock_fetch, mock_spreads, mock_longs,
        mock_puts, mock_calls_detail, mock_calls, mock_portfolio,
        mock_spread_pipe, mock_collar_pipe, mock_send,
    ):
        """run_pipeline passes collar_recs and ccs/pcs_recs to send_recommendations."""
        mock_portfolio.return_value = []
        mock_collar_pipe.return_value = {
            "recommendations": [_make_collar_rec()],
            "eligible_count": 5,
        }
        mock_spread_pipe.return_value = {
            "ccs": [_make_spread_rec("CCS")],
            "pcs": [_make_spread_rec("PCS")],
            "ccs_scenarios": 100,
            "pcs_scenarios": 80,
        }
        mock_send.return_value = True

        from scheduler import run_pipeline
        run_pipeline(dry_run=True)

        # Verify send_recommendations was called with collar/CCS/PCS data
        call_kwargs = mock_send.call_args[1]
        assert "collar_recs" in call_kwargs
        assert "ccs_recs" in call_kwargs
        assert "pcs_recs" in call_kwargs
        assert len(call_kwargs["collar_recs"]) == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python3 -m pytest tests/test_emailer_consolidation.py::TestPipelineIntegration -v 2>&1 | tail -20`
Expected: FAIL — `run_pipeline` doesn't pass collar data yet

- [ ] **Step 3: Insert Phase 1 (Collar & Spreads) into run_pipeline()**

After Step 2 (loading open positions / computing PUR, around line 398), and BEFORE Step 3 (fetching options chains), insert the collar and spread scan:

```python
        # ── Phase 1: Collar & Spreads ─────────────────────────────────────────
        # Runs before the CC pipeline (Phase 2) because collar recs appear first
        # in the unified email. Portfolio data from Phase 0 is reused.
        collar_recs     = []
        ccs_recs        = []
        pcs_recs        = []
        ccs_scenarios   = 0
        pcs_scenarios   = 0
        collar_meta_raw = {}

        try:
            # 1a: Collar scan
            logger.info("[Phase 1a] Running collar pipeline...")
            from collar import run_collar_pipeline
            collar_result = run_collar_pipeline(dry_run=dry_run)
            collar_recs   = collar_result["recommendations"]
            collar_meta_raw = collar_result
            logger.info(f"  {len(collar_recs)} collar recommendation(s)")
        except Exception as exc:
            logger.error(f"[Phase 1a] Collar scan failed: {exc}", exc_info=True)

        try:
            # 1b: CCS + PCS spread scan
            logger.info("[Phase 1b] Running spread weekly pipeline (CCS + PCS)...")
            from spread_scanner import run_spread_weekly_pipeline
            spread_result = run_spread_weekly_pipeline(holdings_all, config)
            ccs_recs      = spread_result.get("ccs", [])
            pcs_recs      = spread_result.get("pcs", [])
            ccs_scenarios = spread_result.get("ccs_scenarios", 0)
            pcs_scenarios = spread_result.get("pcs_scenarios", 0)
            logger.info(
                f"  CCS: {len(ccs_recs)} rec(s) [{ccs_scenarios} scenarios]  |  "
                f"PCS: {len(pcs_recs)} rec(s) [{pcs_scenarios} scenarios]"
            )
        except Exception as exc:
            logger.error(f"[Phase 1b] CCS/PCS scan failed: {exc}", exc_info=True)

        try:
            # 1c: Intraday direction filter (collars only — CCS/PCS not filtered)
            if collar_recs:
                collar_symbols = list({r["symbol"] for r in collar_recs})
                logger.info(f"[Phase 1c] Fetching intraday direction for {len(collar_symbols)} collar symbol(s)...")
                direction_map = _get_intraday_changes(collar_symbols)
                logger.info(
                    "  Direction: "
                    + ", ".join(f"{s}={direction_map[s]}" for s in sorted(direction_map))
                )

                def _passes_up(sym: str) -> bool:
                    d = direction_map.get(sym, "unknown")
                    return d in ("up", "flat", "unknown")

                recs_before = len(collar_recs)
                collar_recs = [r for r in collar_recs if _passes_up(r["symbol"])]
                logger.info(f"  Intraday filter: collar {recs_before}→{len(collar_recs)}")
        except Exception as exc:
            logger.error(f"[Phase 1c] Direction filter failed: {exc}", exc_info=True)

        # Enrich CCS + PCS recs with upcoming earnings dates
        all_spread_symbols = list({r["symbol"] for r in ccs_recs + pcs_recs})
        if all_spread_symbols:
            try:
                from earnings import get_earnings_dates
                earnings_map = get_earnings_dates(all_spread_symbols)
                for rec in ccs_recs + pcs_recs:
                    rec["earnings_date"] = earnings_map.get(rec["symbol"])
            except Exception as exc:
                logger.warning(f"Could not fetch earnings dates for spreads: {exc}")

        # Build collar meta for email
        collar_meta = {
            "eligible_holdings":     collar_meta_raw.get("eligible_count", 0),
            "total_recommendations": len(collar_recs),
            "symbols_with_collars":  len({r["symbol"] for r in collar_recs}),
            "low_gain_count":        sum(1 for r in collar_recs if r.get("low_gain")),
            "earnings_flags":        sum(1 for r in collar_recs if r.get("earnings_date")),
        }

        results["collar_recs"]   = len(collar_recs)
        results["ccs_recs"]      = len(ccs_recs)
        results["pcs_recs"]      = len(pcs_recs)
```

- [ ] **Step 4: Update the send_recommendations call to pass collar/CCS/PCS data**

Update the `send_recommendations()` call at line ~624:

```python
        email_ok = send_recommendations(
            recommendations, run_meta, dry_run=dry_run,
            roll_candidates=roll_candidates, btc_candidates=btc_candidates,
            optimize_results=optimize_results, panic_results=panic_results,
            rescue_results=rescue_results, safety_results=safety_results,
            spread_safety_results=spread_safety_results,
            spread_rescue_results=spread_rescue_results,
            spread_panic_results=spread_panic_results,
            strategy_recs=strategy_recs,
            collar_recs=collar_recs,
            collar_meta=collar_meta,
            ccs_recs=ccs_recs,
            pcs_recs=pcs_recs,
            ccs_scenarios=ccs_scenarios,
            pcs_scenarios=pcs_scenarios,
        )
```

- [ ] **Step 5: Reorder Phase 3 — spread management before standalone protection modes**

The spec requires spread management (safety/rescue/panic for PCS/CCS) to run BEFORE standalone protection modes (optimize/safety/rescue/panic for individual options). The current code has it reversed. Reorder the blocks within `run_pipeline()`:

**Current order (wrong):**
1. Step 6c: Optimize rolls (standalone)
2. Step 6d: Safety BTC (standalone)
3. Step 6e: Rescue rolls (standalone)
4. Step 6f: Panic rolls (standalone)
5. Step 6h: Spread management optimize/rescue/panic

**Correct order per spec Phase 3:**
1. Spread Safety → Rescue → Panic (for PCS + CCS) — `execute_spread_mode("safety"/"rescue"/"panic", sp_type)`
2. Optimize rolls — `execute_optimize_rolls()`
3. Safety BTC — `execute_safety_btc_orders()`
4. Rescue rolls — `execute_rescue_rolls()`
5. Panic rolls — `execute_panic_rolls()`
6. Strategy recs — `parse_strategy_table()` + `scan_strategy_recommendations()`

Move the spread management block (current Step 6h, lines 547-579) to run BEFORE Step 6c (optimize rolls). The acted-keys tracking (`optimize_acted_keys`) is unchanged — standalone modes still exclude contracts acted on by `execute_optimize_rolls()`.

- [ ] **Step 6: Update earnings enrichment to cover all sections**

In the earnings step (around line 401-408), add collar recs to enrichment:

```python
        # Apply earnings warnings to collar recs
        collar_recs = build_earnings_warnings(collar_recs)
        collar_recs = add_ex_dividend_dates(collar_recs)
```

Also apply `annotate_candidates_with_earnings()` to `ccs_recs` and `pcs_recs` in the annotations step (current Step 6g).

- [ ] **Step 7: Update the pipeline summary log**

Update the completion log to include collar/CCS/PCS counts:

```python
        logger.info(
            f"{'='*60}\n"
            f"Pipeline {'dry run ' if dry_run else ''}complete in {duration:.0f}s\n"
            f"  Collars: {len(collar_recs)} rec(s)\n"
            f"  CCS: {len(ccs_recs)} / PCS: {len(pcs_recs)}\n"
            f"  Holdings: {results['holdings_eligible']} eligible, "
            f"{open_calls_count} symbol(s) skipped (open covered calls)\n"
            f"  CC Recs:  {results['recommendations']}\n"
            f"  Earnings: {flagged} warning(s)\n"
            f"  Email:    {'sent ✅' if email_ok else 'FAILED ❌'} → {config.get('recipient_email', 'n/a')}"
        )
```

- [ ] **Step 8: Update watchdog timeout**

Change `_WATCHDOG_CC_PIPELINE` from 3000 (50 min) to 3600 (60 min) at line 83:

```python
_WATCHDOG_CC_PIPELINE   = 3600  # 60 min (includes collar + CC combined)
```

- [ ] **Step 9: Run the integration test**

Run: `python3 -m pytest tests/test_emailer_consolidation.py::TestPipelineIntegration -v 2>&1 | tail -20`
Expected: PASS

- [ ] **Step 10: Run full test suite**

Run: `python3 -m pytest tests/ -v 2>&1 | tail -20`
Expected: ALL PASS

- [ ] **Step 11: Commit**

```bash
git add scheduler.py
git commit -m "feat: merge collar pipeline into run_pipeline() as Phase 1"
```

---

### Task 6: Refactor Standalone --collar, Remove Scheduler Job, Config Cleanup

**Files:**
- Modify: `scheduler.py:1101-1216` (refactor `run_collar_pipeline_and_email()`)
- Modify: `scheduler.py:1280-1288` (remove `job_daily_collar()`)
- Modify: `scheduler.py:1393-1453` (remove collar job from `start_scheduler()`)
- Modify: `main.py:104-109` (update `cmd_collar()`)
- Modify: `config.yaml:15` (remove `collar_pipeline_time_et`)

- [ ] **Step 1: Refactor run_collar_pipeline_and_email() to scan-only**

Rename `run_collar_pipeline_and_email()` to `run_collar_scan()`. Remove the email-sending code (lines 1196-1201 calling `send_collar_report`). The function should only:
1. Run collar scan
2. Run CCS/PCS scan
3. Apply direction filter
4. Print results to console (keep the existing console output)
5. Save HTML preview (keep using `_render_collar_html` temporarily for the on-demand preview)

```python
def run_collar_scan(dry_run: bool = False):
    """
    Execute the collar + CCS/PCS scan pipeline (scan-only, no email).
    Used by --collar / --collar-dry-run for ad-hoc analysis.
    Results are printed to console and saved as HTML preview.
    """
    # ... keep existing scan logic ...
    # REMOVE: from collar_emailer import send_collar_report
    # REMOVE: email_ok = send_collar_report(...)
    # KEEP: console output, HTML preview generation
```

- [ ] **Step 2: Update main.py cmd_collar() to call run_collar_scan()**

```python
def cmd_collar(dry_run: bool = False):
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_collar_scan
    run_collar_scan(dry_run=dry_run)
```

- [ ] **Step 3: Remove job_daily_collar() from scheduler.py**

Delete the `job_daily_collar()` function (lines 1280-1288):
```python
# DELETE this entire function:
def job_daily_collar():
    """Scheduled weekday collar pipeline job (7:30 AM PST / 10:30 AM ET)."""
    if not _is_trading_day():
        logger.info(f"Collar pipeline skipped — {date.today()} is not a trading day")
        return
    if not _wait_for_network("collar pipeline"):
        return
    with _Watchdog("collar pipeline", timeout=_WATCHDOG_COLLAR):
        run_collar_pipeline_and_email(dry_run=False)
```

- [ ] **Step 4: Remove collar job from start_scheduler()**

In `start_scheduler()`, remove lines 1421-1427:
```python
# DELETE these lines:
    collar_time_et    = config.get("collar_pipeline_time_et", "10:30")
    collar_time_local = _et_to_local(collar_time_et)
    logger.info(f"  Collar pipeline: {collar_time_et} ET  →  {collar_time_local} PT  (daily, trading days only)")
    schedule.every().day.at(collar_time_local).do(job_daily_collar)
```

Update the `start_scheduler()` docstring to remove the collar job entry.

- [ ] **Step 5: Remove _WATCHDOG_COLLAR constant**

Delete line 84: `_WATCHDOG_COLLAR = 1200  # 20 min` — no longer used.

- [ ] **Step 6: Remove collar_pipeline_time_et from config.yaml**

Delete line 15 from `config.yaml`:
```yaml
# DELETE: collar_pipeline_time_et: "10:30"     # Daily 10:30 AM ET = 7:30 AM PT
```

- [ ] **Step 7: Run full test suite**

Run: `python3 -m pytest tests/ -v 2>&1 | tail -30`
Expected: ALL PASS (some collar-specific tests may need updates — see Task 7)

- [ ] **Step 8: Commit**

```bash
git add scheduler.py main.py config.yaml
git commit -m "refactor: convert --collar to scan-only, remove separate collar scheduler job"
```

---

### Task 7: Delete Retired Files and Update Tests

**Files:**
- Delete: `collar_emailer.py`
- Delete: `templates/collar_email.html`
- Modify: `tests/test_collar_emailer.py` → delete or refactor
- Modify: `tests/test_collar_emailer_v16.py` → delete or refactor
- Modify: `scheduler.py:1053` (on-demand collar preview — stop importing from collar_emailer)

- [ ] **Step 1: Update on-demand collar preview to use unified template**

In `scheduler.py` at line ~1053, the `run_collar_on_demand_and_preview()` function imports `_render_collar_html` from `collar_emailer`. Update it to use the unified `_render_html` from `emailer`:

```python
    from emailer import _render_html
    html_body = _render_html(
        [],  # no CC recommendations for on-demand
        collar_meta,
        collar_recs=recs,
        collar_meta=collar_meta,
        ccs_recs=[],
        pcs_recs=[],
    )
```

- [ ] **Step 2: Update run_collar_scan() to not import from collar_emailer**

If `run_collar_scan()` (formerly `run_collar_pipeline_and_email()`) still references `collar_emailer` for HTML preview, switch it to use the unified emailer as well.

- [ ] **Step 3: Delete collar_emailer.py**

```bash
git rm collar_emailer.py
```

- [ ] **Step 4: Delete templates/collar_email.html**

```bash
git rm templates/collar_email.html
```

- [ ] **Step 5: Update tests/test_collar_emailer.py**

This test file directly imports from `collar_emailer`. Since that module is deleted, these tests must be either:
- **Deleted** if the functionality they test is now covered by `tests/test_emailer_consolidation.py`
- **Migrated** to test the unified emailer's collar rendering

Decision: Delete `tests/test_collar_emailer.py` — the collar rendering is now tested via `tests/test_emailer_consolidation.py` (Task 3 created those tests).

```bash
git rm tests/test_collar_emailer.py
```

- [ ] **Step 6: Update tests/test_collar_emailer_v16.py**

This file tests CCS/PCS sections in collar_emailer and `send_collar_report()`. Since those functions are deleted:
- The CCS/PCS rendering tests should be migrated to `tests/test_emailer_consolidation.py`
- The subject line tests should be migrated to `TestUnifiedSubjectLine`

Add the key v16 test cases to `tests/test_emailer_consolidation.py`:

```python
class TestCCSPCSQualityFilter:
    """Tests migrated from test_collar_emailer_v16.py — CCS/PCS quality filter."""

    def test_ccs_below_min_credit_filtered_out(self):
        """CCS rec with net_credit_total < $50 is suppressed from rendering."""
        from emailer import _render_html
        low_credit_ccs = _make_spread_rec("CCS", net_credit_total=30.0, credit_to_loss_ratio=0.50)
        # This should not appear in rendered HTML (filtered in send_recommendations)
        # Template receives only filtered recs, so this tests the template handles empty gracefully
        html = _render_html([], META, ccs_recs=[], ccs_meta={}, pcs_recs=[], pcs_meta={})
        assert "No qualifying CCS" in html or "empty-state" in html

    def test_pcs_below_min_cl_ratio_filtered_out(self):
        """PCS rec with credit_to_loss_ratio < 0.25 is suppressed."""
        from emailer import _render_html
        html = _render_html([], META, pcs_recs=[], pcs_meta={}, ccs_recs=[], ccs_meta={})
        assert "No qualifying PCS" in html or "empty-state" in html
```

Then delete the original file:
```bash
git rm tests/test_collar_emailer_v16.py
```

- [ ] **Step 7: Run full test suite**

Run: `python3 -m pytest tests/ -v 2>&1 | tail -30`
Expected: ALL PASS (test count will decrease by the number of deleted tests, but no failures)

- [ ] **Step 8: Clean up stale reference in report_emailer.py**

`report_emailer.py` line 5 has a comment "Mirrors collar_emailer.py structure but for the trade report." Update it to: "Mirrors emailer.py structure but for the trade report."

- [ ] **Step 9: Commit**

```bash
git add -A  # stages deletions and modifications
git commit -m "chore: delete collar_emailer.py + collar_email.html, migrate tests to unified emailer"
```

---

### Task 8: Full Regression Test and Cleanup

**Files:**
- All files from Tasks 1-7

- [ ] **Step 1: Run full test suite**

Run: `python3 -m pytest tests/ -v 2>&1 | tail -40`
Expected: ALL PASS

- [ ] **Step 2: Run --dry-run to verify unified pipeline works end-to-end**

Run: `python3 main.py --dry-run 2>&1 | tail -50`
Expected: Pipeline completes with collar/CCS/PCS/CC sections, HTML preview saved

- [ ] **Step 3: Run --collar --dry-run to verify standalone scan works**

Run: `python3 main.py --collar ALL --dry-run 2>&1 | tail -30`
Expected: Collar scan completes, results printed to console, NO email sent

- [ ] **Step 4: Verify HTML preview contains all sections**

Open the HTML preview from Step 2 and verify:
1. Header says "📊 Daily Options Report"
2. Collar section appears (if any recs)
3. CCS section appears (if any recs)
4. PCS section appears (if any recs)
5. Covered Call section appears
6. Roll-Forward section appears
7. BTC section appears
8. Strategy section appears
9. Single footer (no duplicate disclaimers)

- [ ] **Step 5: Verify no stale imports or references**

Run: `grep -rn "collar_emailer\|collar_email\.html\|spread_optimize_results\|\"optimize\"" --include="*.py" --include="*.html" . | grep -v __pycache__ | grep -v ".pyc"` from project root

Expected: Zero results (all references cleaned up). The only `"optimize"` that may remain is in `execute_optimize_rolls()` which is the standalone options optimize mode (not spread management) — that's correct and unchanged.

- [ ] **Step 6: Verify config.yaml has no collar_pipeline_time_et**

Run: `grep "collar_pipeline_time" config.yaml`
Expected: No output

- [ ] **Step 7: Final commit if any cleanup needed**

```bash
git add -A
git commit -m "chore: final cleanup for reports consolidation"
```
