# GitHub Upload Bundle

This folder is prepared for direct upload to a GitHub repository.

## Included Scope
- Playbook document
- End-to-end runner
- Sink profile abstraction
- Pipeline definitions
- SQL generators
- CDC validation and test scripts

## Recent Runner Changes
- Inline FLINK-36741 probe path and related env params have been removed for simpler operation.
- Main-flow FLINK-36741 mode is retained via `FLINK36741_MAIN_TRANSFORM=1` (enabled automatically in aggressive runs).
- Aggressive defaults were tuned for speed stability in long runs.

## Quick Run

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc

AGGRESSIVE_BUG_TRIGGER=1 \
FLINK36741_MAIN_TRANSFORM=1 \
WAIT_SYNC=1 PRINT_SCHEMA_SNAPSHOT=0 \
./run_sqlancer_cdc_e2e.sh
```

## Latest Validation Snapshot
- Main-flow decimal mapping check passes through and reproduces FLINK-36741 pattern:
	- `source.c4=decimal(10,2)`
	- `sink.deposits=DECIMAL(19, 0)`
- After tuning, one aggressive run completed in about `91s` in this environment.

## pull_images Updates
- `pull_images.py` now follows CDC-style Compose naming by default.
	- Generated `docker-compose.yaml` includes `name: 'cdcup'`.
	- Container names become `cdcup-jobmanager-1` and `cdcup-taskmanager-1`.
- Added configurable Compose project name:
	- CLI argument: `--project-name <name>`
	- Default value: `cdcup`
- Improved interactive UX:
	- Step sections: `基础镜像` / `CDC 版本` / `Source / Sink` / `产物与配置`
	- Added banner / progress bar / glossary hints for key terms
	- Execution summary shown before confirmation (source/sink, CDC version, project name, selected actions)
	- Runtime precheck warnings for missing connector/JDBC/Hadoop jars when skipping dependency download.

### Path & Help Notes
- Output default has been aligned for e2e integration:
	- Running `python3 ./playbook_bundle/sqlancer_cdc/pull_images.py` from repo root now writes files to `playbook_bundle/sqlancer_cdc/` by default.
	- This matches `run_sqlancer_cdc_e2e.sh` default `PIPELINE_YAML=pipeline-definition.yaml` lookup.
- Short options were added:
	- `-h/--help`: show full help with examples
	- `-o/--output-dir`: change generated `docker-compose.yaml` and `pipeline-definition.yaml` location
	- `-t/--target-dir`: set CDC runtime directory
	- `-p/--project-name`: set compose project name
	- `-n/--dry-run`: print actions without executing
- Permission troubleshooting:
	- If output directory is not writable, script now prints a clear hint to use `-o /your/writable/path`.

### E2E Integration Notes
- Added `playbook_bundle/sqlancer_cdc/cdcup.sh` wrapper to delegate to repo-root `cdcup.sh`.
	- This fixes `run_sqlancer_cdc_e2e.sh` local `./cdcup.sh` invocation assumptions.
- Verified compatibility run (mysql -> doris) succeeds end-to-end with pull_images-generated pipeline.
	- Example validation command:

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc
DML_COUNT=5 DDL_COUNT=2 MIXED_COUNT=5 WAIT_SYNC=1 WAIT_TABLE_TIMEOUT=40 DDL_SYNC_TIMEOUT=20 ENABLE_STATUS_LOG=0 ./run_sqlancer_cdc_e2e.sh
```
	- Observed outcome in this environment: pipeline submit success, sink table ready, DML/DDL/mixed phases complete, final Flink job state `RUNNING`.

### About `tables: '\\.*.\\.*'`
- Current default in generated mysql source uses a broad regex style (`\\.*.\\.*`), which means "match database.table patterns broadly".
- Why pipeline can still submit/run:
	- Submission stage focuses on building/deploying the job.
	- Runtime matching happens after job starts.
	- Our e2e flow creates `database0.t0` before submission, so runtime finds matched table quickly.
- Recommendation for production-like usage:
	- Prefer explicit scope to avoid accidental over-capture.
	- Single table: `database0.t0`
	- All tables in one db: `database0.\\.*`

### Example

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc
python3 pull_images.py --output-dir /tmp/pull_images_ui_run --target-dir /tmp/pull_images_ui_cdc
```

Verify naming from generated compose:

```bash
cd /tmp/pull_images_ui_run
docker compose -f docker-compose.yaml up -d
docker compose -f docker-compose.yaml ps
docker compose -f docker-compose.yaml down -v
```

## Suggested Git Commands

```bash
# From repository root
cd /home/wyh/flink-cdc/tools/cdcup

git add playbook_bundle/github_upload_bundle
git commit -m "chore: add github upload bundle for cdc playbook and scripts"
git push origin <your-branch>
```

## Notes
- Review files before commit to avoid pushing secrets.
- Keep single file size below 100MB for standard GitHub push.
- Prefer PR workflow if default branch is protected.
