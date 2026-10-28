# Data ownership and engineering contracts

- Target ownership: Raw and neutral generic derived datasets, their storage,
  queries, versions, PIT selection and frozen input evidence belong to Data.
  Version 0.7 implements catalog/raw/items/integrity/inputs and pure exploration.
  DataMetaStore owns the sole caller-configured data_meta_path SQLite; lake_path
  is also explicit. Old facades, aliases, schemas and monthly SQLite journals
  are removed. Live database/service cutover remains a later authorized stage.
- Return neutral frames plus schema, availability and immutable identity evidence;
  never expose Core types or import Core. Core owns generic Domain/Panel
  conversion and numerical artifacts; BT owns account/evaluation artifacts.
  Do not add a second canonical store for either package's results.
- Data freezes selected input versions/evidence. Workbench freezes submitted
  research definitions, China semantics and backend receipt references; it owns
  app metadata/governance/task orchestration, not input bytes or generic proofs.
- Workbench owns global scheduler/runtime/resource policy. Data defaults to
  serial execution and enforces explicit local worker/in-flight/buffer ceilings
  through one pool and serial publication; it never probes RAM/CPU for admission.
