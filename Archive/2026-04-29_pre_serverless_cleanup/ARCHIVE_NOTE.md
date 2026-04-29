# Archive event: pre-serverless cleanup

**Date:** 2026-04-29
**Archived by:** Nino Motskobili (with assistance from Claude / Emily)

## What was archived

11 files from across the project root and `egd_analysis/`, all retired in favor of newer replacements committed earlier the same week.

| Archived file | Replaced by | Reason |
|---|---|---|
| `Final Table.md` | `README.md` and module-level docs | Old planning doc; content superseded |
| `create_startup_shortcuts.ps1` | `scripts/_register_s4u.ps1` | Old startup-shortcut helper; new script registers a single S4U scheduled task instead |
| `register_tasks.ps1` | `scripts/_register_s4u.ps1` | Same — replaced by S4U registration |
| `setup_task_scheduler.ps1` | `scripts/_register_s4u.ps1` | Same |
| `start_daily_flow.bat` | `scripts/run_daily_flow.bat` | Moved into `scripts/` and renamed |
| `start_prefect_server.bat` | Prefect serverless mode (commit `857de2a`) | Server/worker no longer required |
| `start_prefect_worker.bat` | Prefect serverless mode (commit `857de2a`) | Same |
| `egd_analysis/01_investigation_results.sql` | `medallion_pg/sql/31_gold_egd_dimension.sql` | Investigation work productionized |
| `egd_analysis/02_dim_egd_position.sql` | `medallion_pg/sql/31_gold_egd_dimension.sql` | Same |
| `egd_analysis/03_bridge_egd_position_machine_history.sql` | `medallion_pg/sql/31_gold_egd_dimension.sql` | Same |
| `egd_analysis/04_fact_derived_machine_periods.sql` | `medallion_pg/sql/31_gold_egd_dimension.sql` | Same |

## Why

These files were originally staged for deletion (visible as `git status` " D" entries on `main`). Per the project's archive convention (`Archive/README.md`), files with historical reference value should be archived rather than deleted.

Verification before archiving:
- `git grep` confirmed none of these files are referenced by any tracked code on `main` *except* the three `start_*.bat` scripts, which are referenced only by `create_startup_shortcuts.ps1` (also archived). No live code path depends on them.
- All replacements are confirmed present and committed.

## How to restore

```bash
git mv Archive/2026-04-29_pre_serverless_cleanup/<original-path> <original-path>
```

Update this note with a "Restored" entry below if any file is brought back.
