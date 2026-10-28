# BagelQuant Data agent entrance

Read [CONTRIBUTING.md](CONTRIBUTING.md), [README.md](README.md) and
[pyproject.toml](pyproject.toml), then the affected technical contracts routed
by the contributor guide. [Owner boundaries](docs/en/engineering/development.md) remain authoritative.

Data owns neutral Raw/DataItem datasets, PIT, coverage, versions, frozen inputs and recovery. It has no BagelQuant package dependencies. Keep provider adapters at the edge and retain one authority for committed input history.

Inspect tracked and untracked Git changes before editing. Preserve unrelated
work, Git metadata and historical evidence. Use Python 3.13 and uv; tests use
isolated temporary roots and synthetic inputs or fake providers.
Run the applicable owner checks in CONTRIBUTING.md and report actual results.
Publication, commits, provider calls and real-data/service changes require
explicit authorization. Keep generated references owned by their generators.
