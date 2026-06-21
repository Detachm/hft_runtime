# PM5M Research Log Policy

Research logs are append-only evidence. Every non-trivial experiment must leave a durable entry
before the result is used in discussion or follow-up work.

## Location

Daily logs live under:

```text
docs/research_logs/YYYY-MM-DD.md
```

Use the Asia/Shanghai calendar date for the file name unless an experiment explicitly states a
different operational timezone.

## Experiment IDs

Each experiment entry must use this ID format:

```text
YYYY-MM-DD-EXP-NNN
```

Examples:

```text
2026-06-20-EXP-001
2026-06-20-EXP-002
```

Within one daily file, IDs increase monotonically. Do not reuse an ID.

## Append-Only Rule

After an entry is committed or used as evidence, do not edit or delete it.

If an old entry is wrong or incomplete, append a new entry with:

- `Amends`: old experiment ID.
- `Correction`: what changed.
- `Reason`: why the correction is needed.

This preserves the research trail and prevents later agents from unknowingly rewriting history.

## Required Entry Shape

Use this structure for every experiment:

```md
## YYYY-MM-DD-EXP-NNN - Short Title

- Time: YYYY-MM-DD HH:MM CST
- Objective:
- Hypothesis:
- Data:
- Code/config:
- Commands:
- Parameters:
- Outputs:
- Metrics:
- Interpretation:
- Next:
```

Keep entries concise, but include enough paths, parameters, and hashes/metrics for another agent to
reproduce or challenge the result.

## Scope

Log at least:

- backtests and parameter sweeps;
- data alignment or latency-model changes;
- live-vs-backtest reconciliation;
- production-readiness decisions;
- any result that may influence live trading.

Do not log routine formatting, compile-only checks, or unrelated code cleanup unless it affects a
research result.
