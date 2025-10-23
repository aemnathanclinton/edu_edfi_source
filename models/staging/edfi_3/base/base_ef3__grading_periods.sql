with grading_periods as (
    {{ source_edfi3('grading_periods') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        __last_modified_timestamp as last_modified_timestamp,
        file_row_number,
        filename,
        __is_deleted as is_deleted,
        {{ jget('v:id::string') }} as record_guid,
        ods_version,
        data_model_version,
        {{ extract_descriptor('v:gradingPeriodDescriptor::string') }} as grading_period,
        {{ jget('v:gradingPeriodName::string') }}               as grading_period_name,
        {{ jget('v:periodSequence::int') }}                     as period_sequence,
        {{ jget('v:schoolReference:schoolId::int') }}           as school_id,
        {{ jget('v:schoolYearTypeReference:schoolYear::int') }} as school_year,
        {{ jget('v:beginDate::date') }}                         as begin_date,
        {{ jget('v:endDate::date') }}                           as end_date,
        {{ jget('v:totalInstructionalDays::float') }}           as total_instructional_days,
        -- references
        {{ jget('v:schoolReference') }} as school_reference,

        -- edfi extensions
        {{ jget('v:_ext::string') }} as v_ext
    from grading_periods
)
select * from renamed