with course_offerings as (
    {{ source_edfi3('course_offerings') }}
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
        {{ jget('v:id::string') }}                                   as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:courseReference:educationOrganizationId::int') }} as ed_org_id,
        {{ jget('v:courseReference:courseCode::string') }}           as course_code,
        {{ jget('v:schoolReference:schoolId::int') }}                as school_id,
        {{ jget('v:sessionReference:schoolId::int') }}               as session_school_id,
        {{ jget('v:sessionReference:schoolYear::int') }}             as school_year,
        {{ jget('v:sessionReference:sessionName::string') }}         as session_name,
        {{ jget('v:localCourseCode::string') }}                      as local_course_code,
        {{ jget('v:localCourseTitle::string') }}                     as local_course_title,
        {{ jget('v:instructionalTimePlanned::float') }}              as instructional_time_planned,
        {{ jget('v:courseReference') }}  as course_reference,
        {{ jget('v:schoolReference') }}  as school_reference,
        {{ jget('v:sessionReference') }} as session_reference,
        {{ jget('v:courseLevelCharacteristics') }} as v_course_level_characteristics,
        {{ jget('v:curriculumUseds') }}            as v_curriculum_used,
        {{ jget('v:offeredGradeLevels') }}         as v_offered_grade_levels,

        -- edfi extensions
        {{ jget('v:_ext') }} as v_ext
    from course_offerings
)
select * from renamed