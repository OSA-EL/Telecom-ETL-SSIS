-- ============================================================
-- Issues 4 & 5 — Database Changes
-- Run this against EO_Telecom_GrgEdu before opening VS
-- ============================================================

USE EO_Telecom_GrgEdu;
GO

-- ============================================================
-- ISSUE 4 — Add source_file column to both error tables
-- Tracks which CSV file produced each rejected record
-- ============================================================

ALTER TABLE dbo.err_source_output
    ADD source_file VARCHAR(500) NULL;
GO

ALTER TABLE dbo.err_destination_output
    ADD source_file VARCHAR(500) NULL;
GO

-- ============================================================
-- ISSUE 5 — Create the audit log table
-- One row per package execution, tracks record counts
-- ============================================================

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

-- ============================================================
-- ISSUE 5 — Add audit_id FK column to fact_transaction
-- (column already exists with default -1, just add the constraint
--  once audit_log is in place — optional)
-- ============================================================
-- If audit_id column does NOT yet exist in your fact_transaction, run this:
-- ALTER TABLE dbo.fact_transaction ADD audit_id INT NULL DEFAULT -1;
-- GO

PRINT 'DB changes for Issues 4 & 5 applied successfully.';
