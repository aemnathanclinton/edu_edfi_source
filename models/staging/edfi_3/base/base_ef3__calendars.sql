with calendars as (
    {{ source_edfi3('calendars') }}
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
        {{ jget("v:calendarCode::string") }}                        as calendar_code,
        {{ jget("v:schoolReference:schoolId::integer") }}           as school_id,
        {{ jget("v:schoolYearTypeReference:schoolYear::integer") }} as school_year,
        {{ extract_descriptor('v:calendarTypeDescriptor::string') }} as calendar_type,
        {{ jget("v:schoolReference") }} as school_reference,
        {{ jget("v:gradeLevels") }}     as v_grade_levels,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from calendars
)
select * from renamed