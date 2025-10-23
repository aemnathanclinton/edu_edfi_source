with sessions as (
    {{ source_edfi3('sessions') }}
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
        {{ jget("v:id::string") }}                                  as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:schoolReference:schoolId::integer") }}           as school_id,
        {{ jget("v:schoolYearTypeReference:schoolYear::integer") }} as school_year,
        {{ jget("v:beginDate::date") }}                             as session_begin_date,
        {{ jget("v:endDate::date") }}                               as session_end_date,
        {{ jget("v:sessionName::string") }}                         as session_name,
        {{ jget("v:totalInstructionalDays::float") }}               as total_instructional_days,
        {{ extract_descriptor('v:termDescriptor::string') }} as academic_term,
        -- references
        {{ jget("v:schoolReference") }} as school_reference,
        -- lists
        {{ jget("v:academicWeeks::string") }} as v_academic_weeks,
        {{ jget("v:gradingPeriods::string") }} as v_grading_periods,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from sessions
)
select * from renamed