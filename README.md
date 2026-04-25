<div align="center">

# Telecom ETL Pipeline
### Built with SQL Server Integration Services (SSIS)

<br/>

[![SQL Server](https://img.shields.io/badge/SQL%20Server-2022-CC2927?style=for-the-badge&logo=microsoftsqlserver&logoColor=white)](https://www.microsoft.com/sql-server)
[![SSIS](https://img.shields.io/badge/SSIS-Visual%20Studio%202022-5C2D91?style=for-the-badge&logo=visualstudio&logoColor=white)](https://docs.microsoft.com/sql/integration-services)
[![Windows](https://img.shields.io/badge/Platform-Windows-0078D6?style=for-the-badge&logo=windows&logoColor=white)](https://www.microsoft.com/windows)
[![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=for-the-badge)](.)

<br/>

> A production-grade ETL pipeline that ingests raw telecom transaction CSV files every 5 minutes,
> validates and enriches the data, tracks audit metrics, and archives processed files —
> all built end-to-end with SSIS.

<br/>

*Inspired by the [Garage Education](https://www.youtube.com/@GarageEducation) video series by Eng. Mustafa Alaa — originally implemented in Scala, fully re-built here using SSIS.*

</div>

---

## What This Project Does

A telecom company's system generates a **pipe-delimited CSV file every 5 minutes** containing customer activity events. This SSIS package picks up each file and runs it through a full ETL pipeline:

- **Validates** all records against business rules (null checks, type checks, timestamp format)
- **Enriches** records by joining against a subscriber reference table
- **Decomposes** the IMEI field into TAC and SNR components
- **Loads** clean records into a SQL Server fact table
- **Captures** rejected records into dedicated error tables with source file tracking
- **Audits** every run with record counts (total / stored / rejected)
- **Archives** the processed CSV file to a separate folder

---

## Table of Contents

- [Architecture](#architecture)
- [Source Data Schema](#source-data-schema)
- [Transformation Rules](#transformation-rules)
- [Database Schema](#database-schema)
- [Prerequisites](#prerequisites)
- [Setup Guide](#setup-guide)
- [How to Run](#how-to-run)
- [Data Flow Details](#data-flow-details)
- [Error Handling](#error-handling)
- [Project Structure](#project-structure)
- [Credits](#credits)

---

## Architecture

### Control Flow

```
┌──────────────────────────┐
│   SQL Insert Audit Log   │  ← Creates a new audit_log record, captures audit_id
└────────────┬─────────────┘
             │ (success)
             ▼
┌──────────────────────────┐
│     Data Flow Task       │  ← Full ETL pipeline (see Data Flow below)
└────────────┬─────────────┘
             │ (success)
             ▼
┌──────────────────────────┐
│   SQL Update Audit Log   │  ← Updates record counts and marks status = COMPLETE
└────────────┬─────────────┘
             │ (success)
             ▼
┌──────────────────────────┐
│  FST Archive Source File │  ← Moves processed CSV to archive folder
└──────────────────────────┘
```

### Data Flow

```
                    ┌─────────────────────────┐
                    │   FF SRC Read Transaction│  Reads pipe-delimited CSV
                    └────┬────────────────┬────┘
           (error output)│                │ (main output)
                         ▼                ▼
              ┌──────────────┐   ┌──────────────────┐
              │CNT Rejected  │   │ CNT Total Records │  ← counts all rows
              │   Source     │   └────────┬─────────┘
              └──────┬───────┘            │
                     ▼                    ▼
         ┌───────────────────┐   ┌──────────────────┐
         │DER Add Source File│   │ LKB Subscriber ID │  Full-cache lookup
         │      (SRC)        │   └────────┬──────────┘
         └───────┬───────────┘            │ (match + no-match via IgnoreFailure)
                 ▼                        ▼
      ┌──────────────────┐      ┌─────────────────────┐
      │ OLE DB Destination│      │ DER Get Subscriber ID│  NULL → -99999
      │ err_source_output │      └──────────┬──────────┘
      └──────────────────┘                  │
                                            ▼
                                 ┌──────────────────────┐
                                 │DER Calculate TAC & SNR│  IMEI → TAC + SNR
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                 ┌──────────────────────┐
                                 │   CNT Stored Records  │  ← counts clean rows
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                 ┌──────────────────────┐
                                 │   DER Add Audit ID    │  injects audit_id
                                 └──────────┬───────────┘
                                            │
                                            ▼
                                 ┌──────────────────────┐
                                 │OLEDB DEST             │  Fast load into
                                 │Load Transactions      │  dbo.fact_transaction
                                 └────────┬──────────────┘
                          (error output)  │
                                          ▼
                                 ┌──────────────────────┐
                                 │  CNT Rejected Dest   │  ← counts failed inserts
                                 └──────────┬───────────┘
                                            ▼
                                 ┌──────────────────────┐
                                 │ DER Add Source File  │
                                 │       (DEST)         │
                                 └──────────┬───────────┘
                                            ▼
                                 ┌──────────────────────┐
                                 │OLEDB DEST Error Dest │  → dbo.err_destination_output
                                 └──────────────────────┘
```

---

## Source Data Schema

| Column | Type | Length | Nullable | Example |
|:---|:---:|:---:|:---:|:---|
| `ID` | Int | — | No | `156` |
| `IMSI` | String | 9 | No | `310120265` |
| `IMEI` | String | 15 | Yes | `490154203237518` |
| `CELL` | Int | — | No | `123` |
| `LAC` | Int | — | No | `12` |
| `EVENT_TYPE` | String | 1 | Yes | `1` |
| `EVENT_TS` | Timestamp | — | No | `2020-01-16 07:45:43` |

**Sample CSV (pipe-delimited):**
```
id|imsi|imei|cell|lac|event_type|event_ts
156|310120265|490154203237518|123|12|1|2020-01-16 07:45:43
157|310120266||124|13|2|2020-01-16 07:50:43
```

---

## Transformation Rules

| Source Column | Business Rule | Target Column |
|:---|:---|:---|
| `ID` | As-is | `transaction_id` |
| `IMSI` | As-is · **reject record if null** | `IMSI` |
| `IMSI` | Lookup in `dim_imsi_reference` · use **-99999** if not found | `subscriber_id` |
| `IMEI` | `SUBSTRING(imei, 1, 8)` · use **"-99999"** if null or length < 15 | `tac` |
| `IMEI` | `SUBSTRING(imei, 9, 6)` · use **"-99999"** if null or length < 15 | `snr` |
| `IMEI` | As-is | `IMEI` |
| `CELL` | As-is · **reject record if null** | `CELL` |
| `LAC` | As-is · **reject record if null** | `LAC` |
| `EVENT_TYPE` | As-is | `EVENT_TYPE` |
| `EVENT_TS` | Must be valid `YYYY-MM-DD HH:MM:SS` · **reject record if null** | `EVENT_TS` |

---

## Database Schema

Run all scripts below against your SQL Server instance before executing the package.

### Create Database

```sql
CREATE DATABASE EO_Telecom_GrgEdu;
GO
USE EO_Telecom_GrgEdu;
GO
```

### dim_imsi_reference — Subscriber Lookup Table

```sql
CREATE TABLE dbo.dim_imsi_reference (
    id            INT        NOT NULL IDENTITY(1,1) PRIMARY KEY,
    imsi          VARCHAR(9) NOT NULL,
    subscriber_id INT        NOT NULL
);
GO

INSERT INTO dbo.dim_imsi_reference (imsi, subscriber_id) VALUES
    ('310120265', 1001),
    ('310120266', 1002),
    ('310120267', 1003);
GO
```

### fact_transaction — Main Fact Table

```sql
CREATE TABLE dbo.fact_transaction (
    id             INT          IDENTITY(1,1) PRIMARY KEY,
    transaction_id INT          NOT NULL,
    IMSI           VARCHAR(9)   NULL,
    subscriber_id  INT          NULL,
    tac            VARCHAR(8)   NULL,
    snr            VARCHAR(8)   NULL,
    IMEI           VARCHAR(50)  NULL,
    CELL           INT          NULL,
    LAC            INT          NULL,
    EVENT_TYPE     VARCHAR(2)   NULL,
    EVENT_TS       DATETIME     NULL,
    audit_id       INT          NULL DEFAULT -1
);
GO
```

### audit_log — Run Metrics Table

```sql
CREATE TABLE dbo.audit_log (
    audit_id         INT          IDENTITY(1,1) PRIMARY KEY,
    source_file      VARCHAR(500) NOT NULL,
    total_records    INT          NULL,
    stored_records   INT          NULL,
    rejected_records INT          NULL,
    load_start       DATETIME     DEFAULT GETDATE(),
    load_end         DATETIME     NULL,
    status           VARCHAR(20)  DEFAULT 'RUNNING'
);
GO
```

### err_source_output — Source-Level Errors

Captures rows that fail type conversion at the Flat File Source (bad integers, unparseable dates, etc.)

```sql
CREATE TABLE dbo.err_source_output (
    id                                   INT   IDENTITY(1,1) PRIMARY KEY,
    [Flat File Source Error Output Column] NTEXT NULL,
    ErrorCode                            INT   NULL,
    ErrorColumn                          INT   NULL,
    audit_id                             INT   NULL,
    source_file                          VARCHAR(500) NULL
);
GO
```

### err_destination_output — Transformation-Level Errors

Captures fully-transformed rows that fail when inserting into `fact_transaction`.

```sql
CREATE TABLE dbo.err_destination_output (
    id            INT          IDENTITY(1,1) PRIMARY KEY,
    IMSI          VARCHAR(50)  NULL,
    IMEI          VARCHAR(50)  NULL,
    subscriber_id INT          NULL,
    CELL          INT          NULL,
    LAC           INT          NULL,
    EVENT_TYPE    VARCHAR(2)   NULL,
    EVENT_TS      DATETIME     NULL,
    tac           VARCHAR(8)   NULL,
    snr           VARCHAR(8)   NULL,
    ErrorCode     INT          NULL,
    ErrorColumn   INT          NULL,
    source_file   VARCHAR(500) NULL,
    audit_id      INT          NULL
);
GO
```

---

## Prerequisites

| Tool | Version |
|:---|:---|
| Windows OS | 10 / 11 |
| SQL Server | 2019 or 2022 (Express or higher) |
| SQL Server Management Studio (SSMS) | 19+ |
| Visual Studio | 2019 or 2022 |
| SSIS Extension | SQL Server Integration Services Projects (latest) |

**Install the SSIS Extension:**
1. Open Visual Studio → **Extensions** → **Manage Extensions**
2. Search `SQL Server Integration Services Projects` → Install → Restart VS

---

## Setup Guide

### 1 — Run the database scripts

Open SSMS, connect to your instance, and execute all scripts from the [Database Schema](#database-schema) section above in order.

### 2 — Open the solution

Open `Telecom Test Cases Project.sln` in Visual Studio 2022.

### 3 — Update the database connection

1. In **Solution Explorer**, right-click the connection manager `DESKTOP-RBKS76A\SQLEXPRESS.EO_Telecom_GrgEdu`
2. Click **Edit** → change the Server name to your instance (e.g. `.\SQLEXPRESS`)
3. Click **Test Connection** → OK

### 4 — Update source file path variables

Open `Load Data.dtsx` → **SSIS menu → Variables** and set:

| Variable | What it controls | Example |
|:---|:---|:---|
| `User::ff_src_folder_path` | Folder containing the incoming CSV | `C:\telecom\source\batch 0\` |
| `User::ff_src_file_name` | File name without extension | `01_clean_data` |
| `User::ff_src_file_extension` | File extension | `.csv` |

> `User::ff_src_full_path` is computed automatically:
> `@[User::ff_src_folder_path] + @[User::ff_src_file_name] + @[User::ff_src_file_extension]`

### 5 — Configure the archive folder

Open the `FST Archive Source File` task → set the destination to your archive folder:

```
C:\telecom\archive\
```

Make sure the archive folder **exists on disk** before running.

---

## How to Run

### Development (Visual Studio)

1. Place your CSV file in the source folder configured in Step 4
2. Right-click `Load Data.dtsx` → **Execute Package**
3. Watch the **Progress** tab — green = success, red X = failure with details

### Production (SQL Server Agent)

1. Deploy `bin\Development\Telecom Test Cases Project.ispac` via the Integration Services Deployment Wizard
2. In SSMS → SQL Server Agent → Jobs → New Job → add a step of type **SQL Server Integration Services Package**
3. Schedule it to match the CSV drop interval (every 5 minutes)

### Command Line

```cmd
dtexec /ISServer "\SSISDB\YourFolder\Telecom Test Cases Project\Load Data" ^
       /Server ".\SQLEXPRESS"
```

---

## Data Flow Details

| Step | Component | What happens |
|:---:|:---|:---|
| 1 | **FF SRC Read Transaction** | Reads the CSV, parses all 7 columns. Conversion failures (bad int, bad date) go to `err_source_output`. |
| 2 | **CNT Total Records** | Counts every row passing through → stored in `var_total_records`. |
| 3 | **LKB Subscriber ID** | Full-cache lookup against `dim_imsi_reference`. Unmatched rows flow through with `subscriber_id = NULL` via IgnoreFailure. |
| 4 | **DER Get Subscriber ID** | `ISNULL(subscriber_id) ? -99999 : subscriber_id` |
| 5 | **DER Calculate TAC & SNR** | Splits IMEI: TAC = `SUBSTRING(imei,1,8)`, SNR = `SUBSTRING(imei,9,6)`. Returns `"-99999"` if IMEI is null or shorter than 15 chars. |
| 6 | **CNT Stored Records** | Counts clean rows → stored in `var_stored_records`. |
| 7 | **DER Add Audit ID** | Injects the current run's `audit_id` into each row before loading. |
| 8 | **OLEDB DEST Load Transactions** | Fast-loads (`TABLOCK, CHECK_CONSTRAINTS`) into `fact_transaction`. Insert failures go to `err_destination_output`. |
| 9 | **CNT Rejected Dest** | Counts failed-insert rows → stored in `var_rejected_dest`. |

---

## Error Handling

Every rejected record is captured with full context — no data is silently dropped.

```
Incoming CSV Row
       │
       ├─── Type conversion fails ──────────► err_source_output
       │         (null int, bad date)              raw text · ErrorCode · ErrorColumn
       │                                           source_file · audit_id
       │
       └─── Passes conversion
                   │
                   ▼   (lookup → derive → transform)
                   │
                   ├─── Insert fails ───────────► err_destination_output
                   │      (constraint, overflow)       full row · ErrorCode · ErrorColumn
                   │                                   source_file · audit_id
                   │
                   └─── Insert succeeds ────────► fact_transaction ✓
```

| Table | Captures | Key Columns |
|:---|:---|:---|
| `err_source_output` | Source read / conversion errors | raw row text, ErrorCode, ErrorColumn, source_file, audit_id |
| `err_destination_output` | Transformation / insert errors | full row, ErrorCode, ErrorColumn, source_file, audit_id |
| `audit_log` | Per-run execution metrics | total_records, stored_records, rejected_records, status, load_start, load_end |

---

## Project Structure

```
Telecom Test Cases Project/
│
├── README.md
├── .gitignore
├── Telecom Test Cases Project.sln          ← Visual Studio solution
│
├── sql_setup/
│   └── issues_4_5_db_changes.sql           ← DB migration scripts
│
└── Telecom Test Cases Project/             ← SSIS project
    ├── Load Data.dtsx                      ← Main ETL package
    ├── Project.params
    ├── Telecom Test Cases Project.dtproj
    ├── Telecom Test Cases Project.database
    │
    └── bin/
        └── Development/
            └── Telecom Test Cases Project.ispac   ← Deployable package
```

---

## Credits

<div align="center">

| Role | Detail |
|:---|:---|
| Original concept | [Garage Education](https://www.youtube.com/@GarageEducation) — Eng. Mustafa Alaa (Scala) |
| SSIS implementation | Osama El-Zhery — ETL Developer |
| Tools | SQL Server 2022 · SSIS · Visual Studio 2022 · SSMS 19 |

</div>
