-- Temporary crosswalk stub for SQL Server bring-up. Replace with a real mapping as needed.
-- Columns: assessment_identifier, namespace, score_name, academic_subject, score_result (optional)

{{ config(materialized='ephemeral') }}

select
    cast(null as nvarchar(255)) as assessment_identifier,
    cast(null as nvarchar(255)) as namespace,
    cast(null as nvarchar(255)) as score_name,
    cast(null as nvarchar(255)) as academic_subject,
    cast(null as nvarchar(255)) as score_result
where 1 = 0
