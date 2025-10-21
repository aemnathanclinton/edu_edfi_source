with class_periods as (
    {{ source_edfi3('class_periods') }}
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
        {{ jget("v:id::string") }}                        as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:schoolReference:schoolId::integer") }} as school_id,
        {{ jget("v:classPeriodName::string") }}           as class_period_name,
        {{ jget("v:officialAttendancePeriod::boolean") }} as is_official_attendance_period,
        {{ jget("v:schoolReference") }}                   as school_reference,
        {{ jget("v:meetingTimes") }}                      as v_meeting_times,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from class_periods
)
select * from renamed