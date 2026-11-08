# Wind Turbine ETL

A PySpark pipeline (Databricks notebook) that loads one month of hourly wind turbine readings, checks data quality, produces a daily power summary for each turbine, and flags turbines that behave unusually compared with the rest of the fleet.

- **Fleet:** 15 turbines, three CSV files of 5 turbines each
- **Period:** 1–31 March 2022, hourly readings (744 per turbine, 11,160 rows in total)
- **Columns:** `timestamp`, `turbine_id`, `wind_speed`, `wind_direction`, `power_output` (MW)

## Repository contents

- `01_Wind_Turbine_ETL.ipynb` – the notebook (load, check, summarise, flag anomalies, save)
- `data/` – the three input CSV files
- `README.md` – this file

## How to run

1. Create a free-tier Databricks workspace (the notebook was run on Serverless compute, Spark 4.2.0).
2. Create a Unity Catalog volume and upload `data_group_1.csv`, `data_group_2.csv` and `data_group_3.csv` to it. The notebook expects:
   `/Volumes/workspace/colibri_assessment/wind_turbine_data/`
   (change `volume_path` in the first cell if yours is different).
3. Import `01_Wind_Turbine_ETL.ipynb` and choose **Run all**.
4. Outputs are written to the same volume (see [Outputs](#outputs)).

## What the notebook does

1. **Load** – each file is read with an explicit schema, with every column as a string so raw values are preserved, then combined with `unionByName`.
2. **Check and clean** – data quality checks run on the raw strings first. Values are then converted to proper types (`timestamp`, `int`, `double`).
3. **Summarise** – minimum, maximum and average `power_output` for each turbine for each day (15 turbines × 31 days = 465 rows).
4. **Flag anomalies** – for each day, take the 15 turbine averages, calculate their mean and standard deviation, and flag any turbine more than 2 standard deviations from the mean.
5. **Save** – the cleaned hourly data and the daily summary (with fleet statistics and anomaly flag) are written out and read back to check the row counts.

## Problems found in the data and what I did

I checked for each of the problems named in the brief (missing, duplicated and physically impossible values), plus malformed values and gaps in time coverage.

| Check | Result | Handling |
|---|---|---|
| Missing values (null, empty, whitespace) in any column | 0 | Nothing to fix |
| Exact duplicate rows | 0 | Nothing to fix |
| Duplicate turbine/timestamp pairs | 0 | Nothing to fix |
| Values that cannot be converted to numbers | 0 | Nothing to fix |
| Timestamps that cannot be parsed | 0 | Nothing to fix |
| Readings not on the hour | 0 | Nothing to fix |
| Missing hours (744 expected per turbine) | None, 744 for every turbine | Nothing to fix |
| Physically impossible values (see limits below) | 0 | Nothing to fix |

**Honest summary:** the data supplied to me is clean, so no records were removed, filled or altered. All 11,160 rows are kept.

The observed ranges are wind speed 9.0–15.0, wind direction 0–359°, and power output 1.5–4.5 MW.

### How I would handle problems if they were present

The notebook only contains logic for what was actually in the data, but these are the decisions I would make:

| Problem | Decision | Why |
|---|---|---|
| Missing power reading | Leave as null and flag it | Filling in power output would invent data and distort the daily averages. Averages ignore nulls. |
| Missing wind speed or direction | Leave as null | These are not used in the summary, so no need to guess them. |
| Exact duplicate rows | Remove | They add no information and would bias the average. |
| Same turbine and hour with different values | Keep one, and log which was dropped | Needs a rule agreed with the operations team. |
| Impossible values (negative power or speed, direction outside 0–360°) | Set to null and flag | A sensor fault gives no reliable value, but the original is kept for investigation. |

## Flagged turbines

20 of the 465 turbine-days (4.3%) were flagged. **15 were below** the fleet mean and **5 above**.

| Date | Turbine | Avg power (MW) | Fleet mean (MW) | Fleet SD | Std devs from mean | Direction |
|---|---|---|---|---|---|---|
| 2022-03-01 | 15 | 3.37 | 3.00 | 0.16 | +2.27 | High |
| 2022-03-02 | 2 | 2.47 | 3.02 | 0.22 | -2.57 | Low |
| 2022-03-03 | 12 | 2.67 | 3.03 | 0.15 | -2.44 | Low |
| 2022-03-04 | 8 | 2.53 | 2.96 | 0.16 | -2.72 | Low |
| 2022-03-06 | 3 | 3.30 | 3.01 | 0.13 | +2.24 | High |
| 2022-03-07 | 1 | 3.28 | 3.00 | 0.14 | +2.08 | High |
| 2022-03-08 | 11 | 2.58 | 3.07 | 0.21 | -2.31 | Low |
| 2022-03-11 | 4 | 2.48 | 2.97 | 0.20 | -2.47 | Low |
| 2022-03-12 | 12 | 2.64 | 2.99 | 0.14 | -2.50 | Low |
| 2022-03-13 | 7 | 2.73 | 2.98 | 0.12 | -2.18 | Low |
| 2022-03-15 | 9 | 3.47 | 3.08 | 0.16 | +2.45 | High |
| 2022-03-18 | 2 | 2.74 | 3.00 | 0.13 | -2.06 | Low |
| 2022-03-18 | 15 | 3.30 | 3.00 | 0.13 | +2.25 | High |
| 2022-03-20 | 4 | 2.50 | 2.98 | 0.23 | -2.09 | Low |
| 2022-03-22 | 11 | 2.55 | 2.96 | 0.17 | -2.32 | Low |
| 2022-03-24 | 13 | 2.66 | 3.04 | 0.19 | -2.03 | Low |
| 2022-03-25 | 2 | 2.73 | 3.06 | 0.17 | -2.02 | Low |
| 2022-03-26 | 4 | 2.47 | 2.98 | 0.21 | -2.44 | Low |
| 2022-03-28 | 3 | 2.69 | 2.98 | 0.13 | -2.21 | Low |
| 2022-03-29 | 9 | 2.57 | 2.97 | 0.19 | -2.08 | Low |

**By turbine:** turbines 2 and 4 were each flagged three times (turbine 2 on 2, 18 and 25 March; turbine 4 on 11, 20 and 26 March, always low). Turbines 3, 9, 11, 12 and 15 were flagged twice, turbines 1, 7, 8 and 13 once, and turbines 5, 6, 10 and 14 never.

**How to read this:** with 15 turbines and a 2-standard-deviation rule, roughly 4–5% of turbine-days would be flagged even if every turbine behaved identically (about 4.6% for normally distributed values). The observed 4.3% is in line with that, and no turbine is flagged on many days, so on this evidence I would **not** treat any turbine as faulty. This is an informal comparison, not a statistical test. Turbines 2 and 4 would be the first to look at if more data showed the pattern continuing.

## Assumptions

- **Dates:** the day is the calendar date of the timestamp. Timestamps are used as given, with no time zone conversion.
- **Daily average:** the simple mean of that turbine's 24 hourly readings. Every turbine has all 24 readings every day, so the averages are directly comparable.
- **Physical limits:** wind speed ≥ 0, wind direction between 0 and 360°, power output ≥ 0 MW. No upper limits were applied because no turbine specifications (rated power, cut-out wind speed) were provided.
- **Anomaly rule:** population standard deviation (`stddev_pop`) of the 15 turbine averages for that day, a strict "more than 2 standard deviations" cut-off, applied in both directions (unusually high or low).
- **Flagged records are kept**, not removed. The flag is for investigation, not deletion.
- **Turbine IDs** are kept as strings during checks and converted to integers when the data is cleaned.

## Outputs

Written to the same volume as the input files:

| Folder | Contents | Rows |
|---|---|---|
| `cleaned_turbine_readings/` | Cleaned hourly readings with correct data types | 11,160 |
| `daily_turbine_summary/` | Daily min, max and average power per turbine, plus fleet mean, fleet SD and `is_anomaly` flag | 465 (20 flagged) |

Both are written as CSV with a header. Spark writes each output as a **folder of part-files**, not a single file, and reads the whole folder back as one dataset. Reading the outputs back gives the same row counts (11,160 and 465), and the same 20 flagged turbine-days.

## What I would do next with more time

- **Add upper limits** from the manufacturer's data (rated power, cut-out wind speed) to the physical limits check.
- **Compare against the power curve, not just the fleet.** Power depends on wind speed, so a turbine in a calmer spot may look low without any fault. Comparing power for similar wind speeds, or against its own history, would be a fairer test.
- **Use a longer baseline.** One month is short. Turbines flagged repeatedly across several months would be much stronger evidence than single days.
- **Write the outputs as Delta tables** to keep data types and make reruns safe and easy to query.
- **Add automated tests and a scheduled job** (Databricks Jobs) so the checks run on every new file, with a check that fails the run if unexpected values appear.
- **Make the threshold a setting** (for example 2 or 3 standard deviations) so the sensitivity can be tuned without editing the code.
