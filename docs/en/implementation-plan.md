# Implementation Plan

This page summarizes the design contract that guides BagelQuant Data.

## Objective

Build `bagelquant-data` as a high-performance, extensible, source-agnostic data framework for quantitative research.

The system must:

- use Polars as the primary dataframe engine
- use Parquet as the canonical analytical storage format
- use SQLite for mutable metadata and operational state
- prioritize incremental-update efficiency and query performance
- support multiple external data sources
- implement Tushare as the first source adapter
- preserve point-in-time correctness
- provide a clear and stable Python API
- provide complete documentation and executable examples
- store source data without precomputing vendor-specific financial ratios
- provide generic financial transformations that users compose into indicators
- return non-reference research data as a single-value long panel

## Non-Goals

The project does not implement:

- legacy migration
- backward compatibility
- old-layout compatibility
- a reset command
- permanent one-file-per-asset storage
- permanent one-file-per-API-call storage
- hardcoded indicator functions such as `eps_ttm()` or `roe_ttm()`

## Core Principles

Source-specific code belongs under `bagelquant_data.sources`.

Dataset behavior belongs in declarative specs and registries.

Canonical storage remains row-oriented.

Research extraction returns one field at a time.

Financial processing is generic and point-in-time safe.

Operational metadata belongs in SQLite rather than thousands of metadata Parquet files.

## Roadmap Themes

The current implementation establishes the new public API, package layout, storage zones, canonical writes, manifests, query API, finance primitives, and Tushare source adapter.

Future work can deepen:

- optimized initial builds
- micro-batched partition merges
- asset-level content hash state
- concurrent partition locking
- manifest rebuild operations
- compaction and repair workflows
- richer dataset validators
- more source adapters
