# Incremental update from a `.hyper` payload file

*A standalone example: incrementally update an existing Tableau `.hyper` extract
with data from a separate `.hyper` data source.*

> This is a self-contained sub-example of the `update_hyper` project. It has no
> dependency on the split/union/publish pipeline beyond needing an existing
> extract (`Finished_Merged.hyper`) to update. See the main `README.md` for the
> wider project.

## What it demonstrates

**Can you update an existing Hyper extract in place, without rebuilding it?**
Yes — 
Here the new payload lives in its own `Updates.hyper` file; the update script attaches 
both that new file and the existing extract, and applies the update with a single SQL
`UPDATE ... FROM`, executed inside Hyper's engine.

This mirrors the "keep the data in the engine" approach the rest of the project
uses (`union_hyper_files.py`, `split_by_year.py`): attach files under aliases,
then run one `execute_command`.

## Files

| File | Role |
|------|------|
| `generate_updates.py` | Creates `Updates.hyper` from a Python list of `[Chain Id, New Metres Gained]` rows. Run once (or whenever you change the payload). |
| `Updates.hyper` | The externalised payload: one table `"public"."Updates"` with columns `Chain Id` (TEXT) and `New Metres Gained` (BIG_INT), one row per update. Committed, but fully regenerable. |
| `incremental_update.py` | Copies `Finished_Merged.hyper` → `Example_Update.hyper`, attaches both the copy and `Updates.hyper`, and applies every update in one `UPDATE ... FROM`. |
| `Example_Update.hyper` | The disposable output — a copy of the extract with the updates applied. Regenerated every run; never the real extract. |

Inputs it reads but never modifies: `Finished_Merged.hyper` (the extract to
update from).

## Requirements

The project virtualenv with `tableauhyperapi` installed (see the main
`README.md` §3 for setup):

```
source .venv/bin/activate
```

## Running it

```
python3 generate_updates.py        # Optional. To (re)create Updates.hyper
python3 incremental_update.py   # apply the payload to a disposable copy
```

Expected output:

```
Wrote 5 update row(s) to Updates.hyper (table "public"."Updates").

Copied Finished_Merged.hyper -> Example_Update.hyper (only the copy will be modified)
--- Applying updates from Updates.hyper ---
Before (5 row(s)):
    ['110023590_001', 110023590, 'Wallabies', 'England', Date(2021, 7, 11), 10]
    ['110023590_002', 110023590, 'Wallabies', 'England', Date(2021, 7, 11), 4]
    ...
UPDATE affected 5 row(s) (expected: 5, one per row in Updates.hyper)
After (5 row(s)):
    ['110023590_001', 110023590, 'Wallabies', 'England', Date(2021, 7, 11), 25]
    ['110023590_002', 110023590, 'Wallabies', 'England', Date(2021, 7, 11), 15]
    ...
Done. Only Example_Update.hyper was modified ...
```

## How it works

1. **`generate_updates.py`** opens `Updates.hyper` with `CreateMode.CREATE_AND_REPLACE`
   (fresh every run), creates the `"public"."Updates"` table from an explicit
   `TableDefinition`, and bulk-inserts the `UPDATE_ROWS` list with an `Inserter`.
2. **`incremental_update.py`** copies the extract to a throwaway file, then opens
   a single Hyper connection and attaches **both** files under aliases —
   `target` (the copy) and `updates` (the payload) — so one SQL engine sees both,
   addressable as `"alias"."schema"."table"`.
3. It applies everything in one statement:

   ```sql
   UPDATE "target"."public"."Extract" AS e
   SET "Metres Gained" = u."New Metres Gained"
   FROM "updates"."public"."Updates" AS u
   WHERE e."Chain Id" = u."Chain Id"
   ```

   Because both tables are attached into the same engine, the join and the writes
   happen natively inside Hyper — the Python layer only issues the command string.

### Why attach *both* files (rather than open the copy directly)?

Only because we're using Hyper for our source file.
Opening a connection directly against one `.hyper` file and then attaching a
second changes how the unqualified `"public"."Extract"` name resolves, and the
update fails with `schema "public" does not exist`. Attaching *both* under
explicit aliases and using fully-qualified three-part names avoids the ambiguity —
and matches how `union_hyper_files.py` and `split_by_year.py` already work.

## Customising the update

Editing *which* rows change is a **data-only** change — no code edits:

1. Edit the `UPDATE_ROWS` list in `generate_updates.py`. The Chain Ids must be
   real keys present in the extract.
2. Re-run `python3 generate_updates.py`, then `python3 incremental_update.py`.

If a `Chain Id` in the payload matches no row in the extract, the update simply
affects fewer rows and the script prints a one-line `WARNING` naming the
shortfall — it does not fail.

## Safety

`Finished_Merged.hyper` (and the live Tableau Cloud data source it backs) is
never touched. Every run works on a fresh `Example_Update.hyper` copy, so both
scripts are safe to re-run any number of times.
