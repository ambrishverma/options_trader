# Income Generator — Design Spec

**Date:** 2026-05-26  
**Version:** 1.8  
**Status:** Draft

## Overview

The Income Generator is an autonomous spread-purchasing system that reads the daily
strategy briefing, calculates how many contracts to buy per symbol using a config-driven
formula, checks for duplicates against the live portfolio, and places spread orders
via Robinhood. It is invoked manually through CLI commands; there is no scheduled
pipeline integration in this version.

## Architecture

### Data Flow

```
Daily Briefing (markdown)
    │
    ▼
strategy.py: parse_strategy_table()
    │  → [{symbol, spread_type, action, strike, raw_text}, ...]
    ▼
strategy.py: scan_strategy_recommendations()
    │  → [{symbol, type, short_leg, long_leg, expiration, net_credit,
    │      credit_to_loss_ratio, ypd, score, ...}, ...]
    ▼
income_generator.py: generate_income()
    │  1. Load open_spreads_detail snapshot (portfolio.py)
    │  2. For each qualifying contract:
    │     a. Skip if duplicate (symbol + spread_type + expiration already open)
    │     b. Calculate quantity via formula
    │     c. Place order via trader.py (or print dry-run summary)
    ▼
CLI output: per-symbol summary of actions taken
```

### Module Boundaries

| Module | Responsibility | Changes |
|--------|---------------|---------|
| `income_generator.py` | **New.** Orchestrates the income generation workflow: quantity formula, duplicate detection, order loop. | New file |
| `strategy.py` | Parse daily briefing, scan for contracts. | None — used as-is |
| `spread_scanner.py` | Find best PCS/CCS contracts. | None — called by strategy.py |
| `portfolio.py` | Load portfolio snapshots. | None — used as-is |
| `trader.py` | Place spread orders via Robinhood. | Parameterize `place_spread_order()` to accept `quantity` (currently hardcoded to 1) |
| `main.py` | CLI entry point. | Add `--generate-income`, `--income-config`, `--income-config-set` commands |
| `config.yaml` | Configuration. | Add `income_generator` section |

## Configuration

New keys added to the flat `config.yaml` under a comment header:

```yaml
# Income Generator (v1.8)
ig_min_cl_ratio: 0.10          # Minimum credit/loss ratio to consider a spread
ig_risk_factor: 1.0            # Multiplier on the quantity formula (0.5 = conservative, 1.0 = normal, 2.0 = aggressive)
ig_max_contracts_per_equity: 5 # Hard cap on contracts per symbol per run
ig_enabled: true               # Master switch (false = dry-run only, ignores --generate-income)
```

### Config Management CLI

```
--income-config                         Show current income generator configuration
--income-config ig_risk_factor=0.5      Update a single config key
```

`--income-config` with no argument prints a formatted table of current values with
descriptions. With a `KEY=VALUE` argument, it validates the key exists and the value
type is correct, updates `config.yaml`, and confirms the change.

## Contract Quantity Formula

For each qualifying spread contract returned by the scanner:

```
quantity = floor( (credit_to_loss_ratio / ig_min_cl_ratio) × ig_risk_factor )
quantity = min(quantity, ig_max_contracts_per_equity)
quantity = max(quantity, 0)
```

Where:
- `credit_to_loss_ratio` is from the scanner result: `net_credit_total / max_loss`
- `ig_min_cl_ratio` is the config threshold (default 0.10 = 10%)
- `ig_risk_factor` scales the quantity up or down (default 1.0)
- `ig_max_contracts_per_equity` is the hard cap (default 5)

**Behavior at the threshold:** A spread with `credit_to_loss_ratio` exactly equal to
`ig_min_cl_ratio` produces `floor(1.0 × risk_factor)` = 1 contract (at default
risk_factor). Below the threshold, the formula produces 0 → skip.

**Examples (ig_min_cl_ratio=0.10, ig_risk_factor=1.0, max=5):**

| C/L Ratio | Raw Calc | Floor | Capped | Result |
|-----------|----------|-------|--------|--------|
| 0.05      | 0.50     | 0     | 0      | Skip   |
| 0.10      | 1.00     | 1     | 1      | 1 contract |
| 0.15      | 1.50     | 1     | 1      | 1 contract |
| 0.25      | 2.50     | 2     | 2      | 2 contracts |
| 0.50      | 5.00     | 5     | 5      | 5 contracts |
| 0.80      | 8.00     | 8     | 5      | 5 contracts (capped) |

## Duplicate Detection

Before placing an order, check the latest `open_spreads_detail` snapshot for an
existing position matching **all three** of:

1. **Symbol** (exact match)
2. **Spread type** (PCS or CCS)
3. **Expiration** (same expiration date)

If a match exists, skip the symbol with a log message:
```
[IG] SKIP NVDA CCS — duplicate spread already open (exp 2026-06-20)
```

### Why expiration is included

The same symbol can have spreads at different expirations (e.g., a June PCS and a
July PCS). Including expiration prevents blocking legitimate new positions while
still catching true duplicates within the same expiration cycle.

### Snapshot freshness

The function loads the most recent snapshot via `load_open_spreads_detail_snapshot()`.
If the snapshot is more than 24 hours old, log a warning but proceed (the user may
have just pulled portfolio manually). If no snapshot exists at all, log a warning
and proceed with no duplicate filtering (first-run scenario).

## Order Placement

### Modifications to `place_spread_order()`

Current signature:
```python
def place_spread_order(symbol, rec, spread_type, prompt=True) -> bool
```

New signature:
```python
def place_spread_order(symbol, rec, spread_type, prompt=True, quantity=1, dry_run=False) -> bool
```

Changes:
- `quantity` parameter replaces the hardcoded `1` at the `order_option_spread` call
- `dry_run` parameter: when True, print the order summary but skip the Robinhood API call and return True
- Both parameters default to backward-compatible values

### Order flow in income_generator

```python
for contract in qualifying_contracts:
    if is_duplicate(contract, open_spreads):
        log skip
        continue
    qty = calculate_quantity(contract, config)
    if qty == 0:
        log skip (below threshold)
        continue
    success = place_spread_order(
        symbol=contract["symbol"],
        rec=contract,
        spread_type=contract["type"],
        prompt=False,          # autonomous — no interactive prompt
        quantity=qty,
        dry_run=dry_run,
    )
    results.append({...})
```

**No interactive prompt** when `--add` is passed — the user explicitly opted in to
autonomous execution. Without `--add`, everything runs in preview mode (dry-run).

## CLI Interface

### New commands in the primary mutex group

```
--generate-income [SYMBOL]              Preview income plan (dry-run by default)
--generate-income [SYMBOL] --add        Execute: actually place orders
--income-config [KEY=VALUE]             Show or update income generator config
```

**Safety-first design:** `--generate-income` defaults to preview mode (no orders
placed). The `--add` flag (already in argparse, outside the mutex group) must be
explicitly passed to place real orders. This mirrors the existing
`--pcs TSLA` (find) vs `--pcs TSLA --add` (place) pattern.

Note: `--dry-run` is a separate entry in the mutex group (for the CC pipeline) and
cannot be combined with `--generate-income`. Instead, preview-by-default achieves
the same safety without needing a second flag.

### Usage examples

```bash
# Preview — show what would be purchased (default, no orders placed)
python main.py --generate-income

# Preview for a single symbol
python main.py --generate-income NVDA

# Actually place orders for all recommended symbols
python main.py --generate-income --add

# Place orders for one symbol only
python main.py --generate-income NVDA --add

# View current config
python main.py --income-config

# Adjust risk factor
python main.py --income-config ig_risk_factor=0.5
```

### `--generate-income` output format

**Preview mode** (default, no `--add`):
```
============================================================
  Income Generator — 2026-05-26  [PREVIEW MODE]
============================================================
  Strategy hints: 6 parsed from daily briefing
  Contracts found: 4 of 6 symbols had qualifying spreads

  NVDA  CCS  2026-06-20 (25d)
        Short $180.00 / Long $190.00  |  Credit $1.45/sh
        C/L: 0.15  →  qty: 1  |  Total credit: $145.00
        📋 Would place order (preview)

  GOOG  PCS  2026-06-20 (25d)
        Short $170.00 / Long $160.00  |  Credit $0.85/sh
        C/L: 0.09  →  qty: 0  |  SKIP (below min C/L 0.10)

  AMD   PCS  2026-06-20 (25d)
        SKIP — duplicate spread already open (exp 2026-06-20)

  TSLA  CCS  — no qualifying contract found

============================================================
  Summary: 1 would place, 1 below threshold, 1 duplicate, 1 no contract
  → Re-run with --add to execute orders
============================================================
```

**Live mode** (`--add`):
Same format but with `[LIVE MODE]` header and order confirmations:
```
        ✅ Order placed (id=abc123, state=confirmed)
```

### `--income-config` output format

```
============================================================
  Income Generator Configuration
============================================================
  ig_min_cl_ratio .............. 0.10    Min credit/loss ratio threshold
  ig_risk_factor ............... 1.0     Quantity multiplier (0.5=conservative, 2.0=aggressive)
  ig_max_contracts_per_equity .. 5       Max contracts per symbol per run
  ig_enabled ................... true    Master switch (false = dry-run only)
============================================================
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| No daily briefing file for today | Print warning, exit cleanly with message |
| No strategy recommendations parsed | Print "no PCS/CCS strategies found", exit cleanly |
| Scanner finds no qualifying contract | Log "no contract" for that symbol, continue to next |
| Quantity calculates to 0 | Log "below threshold", continue to next |
| Duplicate detected | Log "duplicate", continue to next |
| `ig_enabled` is false | Print warning, force preview mode even with `--add` |
| Robinhood order fails | Log error, print failure message, continue to next symbol |
| Robinhood auth fails | Abort entire run with error message |
| No portfolio snapshot | Warn about missing snapshot, proceed without duplicate filtering |
| Stale portfolio snapshot (>24h) | Warn about staleness, proceed with available data |

All errors for individual symbols are non-fatal — the loop continues to the next
symbol. Only authentication failure aborts the entire run.

## Testing Strategy

### Unit tests (`tests/test_income_generator.py`)

1. **Quantity formula tests:**
   - Below threshold → 0
   - At threshold → 1
   - Above threshold → correct floor value
   - Capped at max → correct cap
   - Risk factor scaling → correct multiplication
   - Edge cases: C/L ratio = 0, negative values

2. **Duplicate detection tests:**
   - Exact match (symbol + type + expiration) → skip
   - Same symbol, different type → allow
   - Same symbol, same type, different expiration → allow
   - No snapshot available → allow all
   - Empty spreads list → allow all

3. **Orchestration tests (mocked scanner + trader):**
   - Full happy path: parse → scan → calculate → place
   - Mixed results: some placed, some skipped, some failed
   - Preview mode (no `--add`): no orders placed, all logged
   - `ig_enabled=false` forces preview even with `--add`
   - Single-symbol filter (`--generate-income NVDA`)

4. **Config management tests:**
   - `--income-config` (no arg) displays all keys
   - `--income-config KEY=VALUE` updates valid key
   - `--income-config` rejects invalid key name
   - `--income-config` rejects invalid value type

### Integration test pattern

Tests mock `robin_stocks.robinhood` and the Robinhood auth layer, but use real
`parse_strategy_table` output fixtures and real `calculate_quantity` logic.

## File Changes Summary

| File | Type | Description |
|------|------|-------------|
| `income_generator.py` | New | Orchestrator: quantity formula, duplicate check, order loop |
| `trader.py` | Modify | Add `quantity` and `dry_run` params to `place_spread_order()` |
| `main.py` | Modify | Add `--generate-income` and `--income-config` CLI commands |
| `config.yaml` | Modify | Add 4 new `ig_*` keys |
| `tests/test_income_generator.py` | New | Unit + integration tests |

## Out of Scope (v1.8)

- Scheduled/pipeline integration (future: add to daily pipeline after manual validation period)
- Email notification of income generation results
- Position sizing based on account buying power (future: query Robinhood buying power)
- Multi-day strategy lookback (only uses today's briefing)
- Collar-based income generation (only PCS/CCS spreads)
