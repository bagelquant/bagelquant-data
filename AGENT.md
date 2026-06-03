# AGENT.md

## Purpose

bagelquant-data is the unified data layer of the BagelQuant ecosystem.

It owns:

- provider ingestion
- unified data access
- metadata and data contracts
- loader and transformation orchestration
- data lake interfaces
- standardized outputs for downstream systems

It does not own:

- Panel internals
- factor research
- portfolio construction
- graph execution
- backtesting
- analytics

## Boundary Rules

- Do not import downstream repositories from package code.
- In particular, do not import `bagelquant_core`.
- Communicate with `bagelquant-core` through `PanelInputAgreement` payloads.
- Keep provider data immutable at the raw layer.
- Keep transformations stateless.

## Tooling

Use `uv`, `ruff`, `pyright`, `pytest`, `mkdocs`, and `pre-commit`.
Do not use Poetry.
