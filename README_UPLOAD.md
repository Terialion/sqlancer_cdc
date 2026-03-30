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

Non-interactive (CI-friendly) E2E run:

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc

./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--sink-type doris \
	--base-seed 111 \
	--dml-count 120 \
	--ddl-count 20 \
	--mixed-count 80
```

Check all options:

```bash
./run_sqlancer_cdc_e2e.sh -h
```

Enable PQS absent->present probe inside E2E:

```bash
./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--sink-type doris \
	--enable-pqs-presence-probe
```

When enabled, E2E runs `pqs_transform_pivot_probe.sh` at workflow end and writes result to report.

Random transform + random DML/DDL + pivot detection (integrated):

```bash
./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--sink-type doris \
	--base-seed 981 \
	--dml-count 20 \
	--ddl-count 10 \
	--mixed-count 20 \
	--random-transform \
	--enable-pqs-presence-probe
```

Tip:
- `--random-transform-seed` can be used to replay a specific random transform template.
- Each run writes reproducible metadata to `REPORT_DIR/experiment_archive.txt`.
- Random transform defaults are now parser-safe (no CAST by default) for better stability on near-3.1.x stacks.

Focus-based stress (choose test direction):

```bash
# Transform-focused (FLINK-36741-like path)
./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--sink-type doris \
	--test-focus transform

# Timezone-focused (best with paimon/starrocks sinks)
./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--sink-type starrocks \
	--test-focus timezone \
	--focus-time-zone Asia/Shanghai

# Combine focuses
./run_sqlancer_cdc_e2e.sh --test-focus transform,timezone,schema
```

Focus behavior summary:
- `transform`: enables transform-heavy path and auto-enables PQS probe.
- `timezone`: injects `pipeline.local-time-zone` and adds timestamp/datetime source columns.
- `schema`: raises schema churn intensity (higher DDL pressure).

Extensibility hooks for future transform generation:

```bash
# 1) Built-in transform shortcut (works across sinks by injecting transform into base pipeline yaml)
./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--sink-type doris \
	--transform-projection "c0, c1, c4 as deposits"

# 2) Generic custom patch hook (recommended for future complex transform generation)
# patch script signature: patch.sh <input_yaml> <output_yaml>
./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--pipeline-patch-script ./your_pipeline_patch.sh
```

## Latest Validation Snapshot
- Main-flow decimal mapping check reproduces precision-loss pattern in normal E2E runs (not only special bug mode):
	- `source.c4=decimal(10,2)`
	- `sink.c4 (or transformed c4)=DECIMAL(19, 0)`
- Meaning:
	- decimal scale can be dropped in sink (`10,2 -> 19,0`), causing factual value mismatch risk.
- Practical signal in report:
	- `Main-flow decimal precision loss detected: YES`
- After tuning, one aggressive run completed in about `91s` in this environment.

Quick check command (normal script usage path):

```bash
./run_sqlancer_cdc_e2e.sh \
	--pipeline-yaml pipeline-definition-doris.yaml \
	--sink-type doris \
	--rounds 1 \
	--dml-count 5 --ddl-count 2 --mixed-count 5 \
	--random-transform --enable-pqs-presence-probe
```

## pull_images Updates
- `pull_images.py` now follows CDC-style Compose naming by default.
	- Generated `docker-compose.yaml` includes `name: 'cdcup'`.
	- Container names become `cdcup-jobmanager-1` and `cdcup-taskmanager-1`.
- Added configurable Compose project name:
	- CLI argument: `--project-name <name>`
	- Default value: `cdcup`
- Improved interactive UX:
	- Step sections: `基础镜像` / `CDC 版本` / `Source / Sink` / `产物与配置`
	- Execution summary shown before confirmation (source/sink, CDC version, project name, selected actions)
	- Runtime precheck warnings for missing connector/JDBC/Hadoop jars when skipping dependency download.

### Example

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc
python3 pull_images.py --output-dir /tmp/pull_images_ui_run --target-dir /tmp/pull_images_ui_cdc
```

Batch mode example (no interactive prompt):

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc

python3 pull_images.py \
	--batch \
	--mode quick \
	--source-type mysql \
	--sink-type doris \
	--cdc-version 3.2.1 \
	--project-name cdcup \
	--output-dir /tmp/pull_images_batch_run \
	--target-dir /tmp/pull_images_batch_cdc
```

Tip:
- Add `--skip-image-pull` when only generating configs or downloading jars.
- Add `--skip-download-cdc` when your `cdc/lib` is already prepared.

Fast stack profile (near FLINK-35852 / CDC 3.1.1):

```bash
python3 pull_images.py \
	--batch \
	--stack-profile flink35852-near311 \
	--project-name cdcup311 \
	--output-dir /tmp/cdc311_env \
	--target-dir /home/wyh/flink-cdc/tools/cdcup/cdc
```

## CI Smoke

Use the one-command smoke runner:

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc
./run_ci_smoke.sh
```

Optional knobs:
- `BASE_SEED=900 ./run_ci_smoke.sh`
- `RUN_PAIMON=0 ./run_ci_smoke.sh` (only run pull_images + doris smoke)

## PQS Transform Probe

Run the productized feasible-scope PQS probe:

```bash
cd /home/wyh/flink-cdc/tools/cdcup/playbook_bundle/sqlancer_cdc
./pqs_transform_pivot_probe.sh
```

Current verified scope:
- PASS: transform filter supports pivot row from absent to present in sink.
- PASS: value-level comparison is built-in for pivot key (source/sink row values are checked after pivot appears).

Performance defaults (for faster E2E):
- `CLEAN_RESTART=0` (default): skip full container restart in PQS stage.
- `WAIT_SECONDS=1`, `WAIT_RETRIES=30` (default): faster probe completion while keeping correctness checks.

Failure visibility:
- If PQS fails in E2E, report now prints `PQS failure line: ...` (first `ERROR:` line) before tail logs.

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
