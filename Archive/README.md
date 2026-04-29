# Archive

This folder holds **superseded files** from across the project — old scripts, retired SQL, replaced documentation, deprecated configuration. Anything that was once part of the working project but is no longer used.

## Why archive instead of delete?

- Recovery: a deleted file requires `git log` archaeology to recover; an archived file is one `mv` away
- Context: an archived file with a dated subfolder shows *when* a decision changed and *what* it replaced
- Audit: the team can see the full history of architectural choices in one place

## Layout

```
Archive/
├── README.md                          <- this file
├── 2026-04-29_task_scheduler/         <- one folder per archive event
│   ├── ARCHIVE_NOTE.md                <- why these were archived; what replaced them
│   └── <archived files preserving original paths>
└── ...
```

Each archive event lives in its own dated folder, named `YYYY-MM-DD_short-topic`. Inside, an `ARCHIVE_NOTE.md` answers three questions:

1. **What was archived** — the file list
2. **Why** — what replaced it, what decision drove the change
3. **Who** — who made the call (often a slack thread or ticket reference)

## Module-level archives

This top-level `Archive/` is for project-wide changes. Individual modules MAY have their own `Archive/` subfolder for module-scoped retirements:

- `medallion_pg/Archive/` — DW pipeline-scoped archives
- `scripts/Archive/` — automation script archives
- `docs/<topic>/Archive/` — documentation-event archives

Use the most local archive that fits. If a change touches multiple modules, archive at this top level.

## When to archive

| Situation | Action |
|---|---|
| A script has a clear successor; old version still useful as reference | **Archive** |
| A SQL file was replaced by a refactored version | **Archive** with note linking to the replacement |
| A doc describes a deprecated approach that someone might still reference | **Archive** |
| A file is auto-generated, transient, or trivially recreatable | **Delete** (don't pollute Archive) |
| A file was never used, was a bad idea from the start, has no historical value | **Delete** |

## Restoring from archive

```bash
git mv Archive/<dated-folder>/<original-path> <original-path>
```

Then update the `ARCHIVE_NOTE.md` in the dated folder to record the restore (or move the whole dated folder into a `restored/` subdir).

## Convention enforced going forward

When in doubt: **archive, don't delete**. Storage is cheap; lost context is expensive. Reviewers should reject PRs that delete files without either an `ARCHIVE_NOTE.md` entry or a clear comment explaining why deletion (not archiving) is correct.
