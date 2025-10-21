with bell_schedules as (
    {{ source_edfi3('bell_schedules') }}
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
        {{ jget("v:id::string") }}                              as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:bellScheduleName::string") }}                as bell_schedule_name,
        {{ jget("v:schoolReference:schoolId::int") }}           as school_id,
        {{ jget("v:alternateDayName::string") }}                as alternate_day_name,
        {{ jget("v:startTime::string") }}                       as start_time,
        {{ jget("v:endTime::string") }}                         as end_time,
        {{ jget("v:totalInstructionalTime::float") }}           as total_instructional_time,
        -- references
        {{ jget("v:schoolReference") }}                         as school_reference,
        -- unflattened lists
        {{ jget("v:classPeriods") }}                            as v_class_periods,
        {{ jget("v:dates") }}                                   as v_dates,
        {{ jget("v:gradeLevels") }}                             as v_grade_levels,
        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from bell_schedules
)
select * from renamed
