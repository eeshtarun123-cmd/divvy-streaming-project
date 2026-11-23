CREATE OR REPLACE TABLE mart.mart_pipeline_health AS
WITH latest_run AS (
    SELECT
        run_id,
        MAX(started_at_utc) AS latest_started_at_utc
    FROM ops.pipeline_run_log
    GROUP BY run_id
    ORDER BY latest_started_at_utc DESC
    LIMIT 1
),
step_status AS (
    SELECT
        log.run_id,
        log.step_name,
        MIN(log.started_at_utc) AS pipeline_started_at_utc,
        MAX(log.ended_at_utc) AS pipeline_ended_at_utc,
        MAX(COALESCE(log.duration_seconds, 0)) AS step_duration_seconds,
        CASE
            WHEN COUNT_IF(log.status = 'FAILED') > 0 THEN 'FAILED'
            WHEN COUNT_IF(log.status = 'SUCCESS') > 0 THEN 'SUCCESS'
            ELSE 'STARTED'
        END AS step_status
    FROM ops.pipeline_run_log AS log
    INNER JOIN latest_run
        ON log.run_id = latest_run.run_id
    GROUP BY log.run_id, log.step_name
),
step_summary AS (
    SELECT
        run_id,
        COUNT(*) AS total_steps,
        COUNT_IF(step_status = 'SUCCESS') AS successful_steps,
        COUNT_IF(step_status = 'FAILED') AS failed_steps,
        ROUND(SUM(step_duration_seconds), 2) AS total_duration_seconds,
        MIN(pipeline_started_at_utc) AS pipeline_started_at_utc,
        MAX(pipeline_ended_at_utc) AS pipeline_ended_at_utc
    FROM step_status
    GROUP BY run_id
),
validation_summary AS (
    SELECT
        results.run_id,
        COUNT(*) AS total_validation_checks,
        SUM(CASE WHEN results.status = 'PASS' THEN 1 ELSE 0 END) AS passed_validation_checks,
        SUM(CASE WHEN results.status = 'FAIL' AND results.severity = 'ERROR' THEN 1 ELSE 0 END) AS error_validation_failures,
        SUM(CASE WHEN results.status = 'WARNING' THEN 1 ELSE 0 END) AS warning_validation_failures
    FROM ops.validation_results AS results
    INNER JOIN latest_run
        ON results.run_id = latest_run.run_id
    GROUP BY results.run_id
),
manifest_summary AS (
    SELECT
        COUNT(*) AS total_manifest_files,
        SUM(CASE WHEN processed_status = 'PROCESSED' THEN 1 ELSE 0 END) AS processed_files,
        SUM(CASE WHEN processed_status LIKE 'SKIPPED%' THEN 1 ELSE 0 END) AS skipped_files,
        MAX(processed_at_utc) AS latest_file_processed_at_utc
    FROM ops.source_file_manifest
),
watermark_summary AS (
    SELECT
        COUNT(*) AS total_watermarks,
        MAX(updated_at_utc) AS latest_watermark_updated_at_utc
    FROM ops.pipeline_watermarks
)
SELECT
    step_summary.run_id,
    step_summary.pipeline_started_at_utc,
    step_summary.pipeline_ended_at_utc,
    step_summary.total_duration_seconds,
    step_summary.total_steps,
    step_summary.successful_steps,
    step_summary.failed_steps,
    COALESCE(validation_summary.total_validation_checks, 0) AS total_validation_checks,
    COALESCE(validation_summary.passed_validation_checks, 0) AS passed_validation_checks,
    COALESCE(validation_summary.error_validation_failures, 0) AS error_validation_failures,
    COALESCE(validation_summary.warning_validation_failures, 0) AS warning_validation_failures,
    manifest_summary.total_manifest_files,
    manifest_summary.processed_files,
    manifest_summary.skipped_files,
    manifest_summary.latest_file_processed_at_utc,
    watermark_summary.total_watermarks,
    watermark_summary.latest_watermark_updated_at_utc,
    CASE
        WHEN step_summary.failed_steps > 0 THEN 'FAILED'
        WHEN COALESCE(validation_summary.error_validation_failures, 0) > 0 THEN 'FAILED_VALIDATION'
        WHEN COALESCE(validation_summary.warning_validation_failures, 0) > 0 THEN 'SUCCESS_WITH_WARNINGS'
        ELSE 'SUCCESS'
    END AS pipeline_health_status
FROM step_summary
LEFT JOIN validation_summary
    ON step_summary.run_id = validation_summary.run_id
CROSS JOIN manifest_summary
CROSS JOIN watermark_summary;
