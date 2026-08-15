# scripts/

Operator tooling. Nothing here is imported by the application; these are run by
hand from a terminal.

## trader-ctl

Starts or stops **this laptop** as the authoritative options-trader host.

```
trader-ctl start        # make this laptop the trading machine
trader-ctl stop         # stand this laptop down
trader-ctl status       # what is this laptop doing?
trader-ctl pin REF      # always start from a tag/commit instead of main
trader-ctl pin --clear  # go back to tracking main
```

### Why it exists

The duplicate-instance lock in `scheduler.py` is an `flock` on a file under
`DATA_DIR`. It reliably prevents two schedulers on **one** machine, and cannot
see another machine at all. Two laptops both running `main.py --schedule` both
evaluate `_force_dry_run` to `False` and both place real orders against the same
Robinhood account.

Until the container work lands — where `TRADER_ALLOW_LIVE` makes an instance
opt in to trading and solves this properly — switching hosts is a manual,
strictly ordered operation. This script makes that operation checkable instead
of remembered.

### What `start` does

1. **Refuses** if a scheduler is already running locally.
2. **Updates the code** — `git pull --ff-only`, or checks out the pinned ref.
   Uncommitted changes to *tracked* files abort the run rather than being
   stashed: `config.yaml` holds the live-order master switches (`ig_enabled`,
   `auto_income`, `auto_defense`), and silently reverting a threshold someone
   tuned mid-incident is worse than not starting. Untracked files do not block.
   A failed fetch warns and continues on the code already on disk — `--ff-only`
   cannot leave a half-merged tree, so that code is exactly what ran before.
3. **Syncs dependencies** if `requirements.lock` changed, into the interpreter
   named in the launchd plist — the same binary launchd will run.
4. **Preflights**, after the update so it validates the code that will actually
   run: interpreter exists, that interpreter can import the app, `.env` present,
   and the trading gate evaluates to `False`. Any failure aborts before
   `launchctl` is touched.
5. **Asks for confirmation** that the other laptop is stopped (`-y` skips).
6. **Bootstraps and verifies** — exactly one process, correct startup line.

### What it deliberately does not do

- **`verify-daily-pipeline`** cannot be toggled from a script. Claude-managed
  schedules live in the scheduling service, not on disk — a task folder can sit
  in `~/.claude/scheduled-tasks/` while nothing is registered. `start` and `stop`
  each print the sentence to give Claude on that laptop.
- **`daily-stocks-briefing`** is left alone by design. It is harmless on both
  laptops and its CSV output is per-machine.
- **It cannot detect the other laptop.** The confirmation prompt is a human
  check, not a technical one.

### Flags

| Flag | Effect |
| --- | --- |
| `-y` | skip the confirmation prompt on `start` |
| `--no-pull` | start without updating at all (offline, or to freeze what is on disk) |
| `--ref REF` | update to `REF` just this once, ignoring any saved pin |

### Pinning

`trader-ctl pin v1.2.3` records a ref in `~/.config/trader-ctl/pin` and `start`
checks that out instead of tracking `main`. The pin is per-laptop and lives
outside the repo, so it survives checkouts and is never itself pulled. Use it
when you want a switchover to land on a known-good revision rather than
whatever happens to be on `main`.

### Environment

`TRADER_REPO` overrides the repo location (default `~/Code/options_trader`).
Used by the tests; also useful for a second checkout.
