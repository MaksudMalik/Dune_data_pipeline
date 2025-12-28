# Dune data pipeline

[![Daily Dune Sync](https://github.com/MaksudMalik/presale_data_dashboard/actions/workflows/daily_sync.yaml/badge.svg)](https://github.com/MaksudMalik/presale_data_dashboard/actions/workflows/daily_sync.yaml)

Dune dashboard automation pipeline.

The pipeline runs every 24 hours, fetches data from external APIs, applies minimal processing, and uploads the resulting dataset to Dune.

**Dune profile:** [Connor_Kenway3](https://dune.com/connor_kenway3)


## Purpose

- Collect data from external APIs
- Perform light transformation where required
- Upload refreshed datasets to Dune
- Keep dashboards updated on a fixed schedule

---

## Scope

- Designed to scale to multiple dashboards within the same repo
- Not intended for real-time or streaming data

