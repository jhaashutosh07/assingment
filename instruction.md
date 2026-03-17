# Fix the Data Pipeline

The pipeline at `/app/` is broken. Run it with `cd /app && python3 orchestrator.py` and fix all bugs so it produces correct output at `/app/output/sales_report.json`.

The pipeline reads `pipeline_config.yaml`, resolves dependencies, and runs tasks in order. It should resolve `${VAR}` references, handle diamond dependencies correctly, execute tasks after their deps, keep parallel branches isolated, and skip dependents of failed tasks.

Files: `/app/orchestrator.py`, `/app/dag.py`, `/app/config.py`, `/app/transforms.py`, `/app/validators.py`, `/app/pipeline_config.yaml`, `/app/data/sales.csv`
