# scripts/

Operator tooling. Nothing here is imported by the application; these are run by
hand from a terminal. All three are safe on a live machine — `trader-check` and
`trader-schedules` are strictly read-only, and `trader-ctl` is the only one that
changes anything.

| Script | Purpose |
| --- | --- |
| `trader-ctl` | start / stop this laptop as the trading host |
| `trader-check` | verify this laptop is correctly configured to trade |
| `trader-schedules` | enumerate every scheduling system on this laptop |

### Environment overrides

| Variable | Effect |
| --- | --- |
| `TRADER_REPO` | repo location (default `~/Code/options_trader`) |
| `TRADER_PYTHON` | interpreter to use instead of the one named in the launchd plist |

Set **both** when testing. `TRADER_REPO` alone redirects the repo but *not* the
interpreter, and `trader-ctl`'s dependency sync installs into whatever
interpreter the plist names — so a test against a throwaway clone would
pip-install into the production environment.

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

See the overrides table at the top. When exercising `start` against a scratch
checkout, set `TRADER_PYTHON` as well — otherwise the dependency sync targets
the real interpreter regardless of `TRADER_REPO`.

## trader-check

Read-only. Verifies this laptop is correctly set up to trade, in eight areas:

1. **launchd** — job loaded, running, exit status, and that exactly *one*
   scheduler process exists
2. **Interpreter** — the plist's python exists *and* can import the app. A pip
   install into a different interpreter than the plist names is the classic
   failure: it succeeds, then launchd crash-loops the job on `ImportError`
   every 30 seconds
3. **Trading gate** — `_force_dry_run` must be `False` on a laptop, and
   `TRADER_DATA_DIR` must be unset. If that variable is set, `is_container()`
   becomes true, the gate arms, and trading stops silently while the daemon
   still looks healthy
4. **Startup** — the `Signal handlers installed` line, jobs registered, no
   "not authoritative" skips in the log
5. **State** — newest run marker and `ig_ledger`, plus counts and `.env`
   size/mode. These are the files that prevent a duplicate pipeline and
   duplicate live spread orders
6. **Briefing output** — a recent `Strategy-Purchase-*.csv` in `snapshots/`,
   which is how the strategy hints reach the pipeline
7. **Absence checks** — no retired copy job in cron or as a task folder
8. **verify-daily-pipeline** — SKILL.md present, and any interpreter it names
   exists *on this machine*

Exits after a summary. Registration of Claude-managed tasks cannot be checked
from a shell, so it reports that as a WARN with the question to ask instead.

## trader-schedules

Read-only. Four independent systems schedule work on these machines and none of
them lists the others — which is how a redundant job survived in two places
unnoticed:

| System | Where | Inspect with |
| --- | --- | --- |
| launchd | `~/Library/LaunchAgents/` | `launchctl list` |
| cron | user crontab | `crontab -l` |
| Claude Code tasks | `~/.claude/scheduled-tasks/` | ask Claude: "list my scheduled tasks" |
| Claude Desktop tasks | `~/Documents/Claude/Scheduled/` | the Desktop app's own UI |

The script covers all four and ends with a short map of what the trading
pipeline actually depends on.

**Important limitation, which it states rather than hides:** for both Claude
systems a folder on disk is *not* proof anything is scheduled. A deleted task
leaves its folder behind, so the folder listing can show jobs that will never
run. Only Claude itself can report what is actually registered.
