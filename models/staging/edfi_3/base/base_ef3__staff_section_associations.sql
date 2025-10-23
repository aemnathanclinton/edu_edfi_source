with staff_section as (
    {{ source_edfi3('staff_section_associations') }}
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
        {{ jget("v:id::string") }}                                 as record_guid,
        ods_version,
        data_model_version,
        -- section key
        {{ jget("v:sectionReference:localCourseCode::string") }}   as local_course_code,
        {{ jget("v:sectionReference:schoolId::int") }}             as school_id,
        {{ jget("v:sectionReference:schoolYear::int") }}           as school_year,
        {{ jget("v:sectionReference:sectionIdentifier::string") }} as section_id,
        {{ jget("v:sectionReference:sessionName::string") }}       as session_name,
        -- staff key
        {{ jget("v:staffReference:staffUniqueId::string") }}       as staff_unique_id,
        -- fields
        {{ jget("v:beginDate::date") }}                            as begin_date,
        {{ jget("v:endDate::date") }}                              as end_date,
        {{ jget("v:highlyQualifiedTeacher::boolean") }}            as is_highly_qualified_teacher,
        {{ jget("v:percentageContribution::float") }}              as percentage_contribution,
        {{ jget("v:teacherStudentDataLinkExclusion::boolean") }}   as teacher_student_data_link_exclusion,
        -- descriptors
        {{ extract_descriptor('v:classroomPositionDescriptor::string') }} as classroom_position,
        -- references
        {{ jget("v:sectionReference") }} as section_reference,
        {{ jget("v:staffReference") }}   as staff_reference,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from staff_section
)
select * from renamed