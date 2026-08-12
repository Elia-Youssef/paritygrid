# ParityGrid

ParityGrid is a local-first data reconciliation and observable I/O execution showcase built with Python, FastAPI, SQLite, DuckDB, React, and TypeScript.

The project demonstrates how heterogeneous and unreliable data sources can be normalized, compared, repaired, and verified while the same workload is executed sequentially, with bounded threads, or with structured asynchronous concurrency.

## Confidentiality statement

This public repository is an independently designed technical demonstration inspired by classes of data reconciliation and automation problems encountered in prior professional work. The underlying production system, client details, data, business rules, implementation, and operational metrics remain confidential under NDA and are not reproduced here. All code, scenarios, names, and datasets in this repository are synthetic.

## Current status

The repository has completed its architecture baseline and initial toolchain foundation. The Python package, FastAPI operational routes, command-line smoke path, React operations shell, strict static checks, and cross-platform pull-request workflow are implemented. The pure domain foundation is the next delivery phase.

The authoritative instruction set begins at [Documentation index](docs/INDEX.md).

## Intended final experience

```powershell
uv sync
uv run paritygrid demo
```

The packaged demonstration will start the synthetic source systems, initialize local embedded databases, serve the operations console, execute the canonical reconciliation scenario, and verify the final state without requiring an external database service.

## Technology direction

- Python 3.14 and FastAPI
- SQLite, SQLAlchemy, and Alembic for durable operational state
- DuckDB and Parquet for analytical reconciliation
- React, TypeScript, Vite, Tailwind CSS, and owned shadcn/ui components
- React Flow for the pipeline studio
- TanStack Query and TanStack Table for data interaction
- Apache ECharts for execution and comparison visualizations
- pytest, Hypothesis, Vitest, and Playwright for verification

## License

A license will be selected before the first public release. Until then, no permission to copy, modify, or redistribute the source is granted.
