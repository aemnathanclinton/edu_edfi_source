with staff_school as (
    {{ source_edfi3('staff_school_associations') }}
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
        {{ jget("v:staffReference:staffUniqueId::string") }}    as staff_unique_id,
        {{ jget("v:schoolReference:schoolId::int") }}           as school_id,
        {{ jget("v:calendarReference:calendarCode::string") }}  as calendar_code,
        {{ jget("v:calendarReference:schoolId::int") }}         as calendar_school_id,
        {{ jget("v:calendarReference:schoolYear::int") }}       as calendar_school_year,
        {{ jget("v:schoolYearTypeReference:schoolYear::int") }} as school_year,
        -- descriptors
        {{ extract_descriptor('v:programAssignmentDescriptor::string') }} as program_assignment,
        -- references
        {{ jget("v:calendarReference") }} as calendar_reference,
        {{ jget("v:schoolReference") }}   as school_reference,
        {{ jget("v:staffReference") }}    as staff_reference,
        -- lists
        {{ jget("v:academicSubjects::string") }} as v_academic_subjects,
        {{ jget("v:gradeLevels::string") }}       as v_grade_levels,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from staff_school
)
select * from renamed
