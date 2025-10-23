with source_stu_program as (
    {{ source_edfi3('student_school_food_service_program_associations') }}
), 

renamed as (
    select 
        -- generic columns
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,

        {{ jget('v:id::string') }}                                                      as record_guid, 
        ods_version::string,
        data_model_version,
        {{ jget('v:studentReference:studentUniqueId::string') }}                    as student_unique_id,
        {{ jget('v:educationOrganizationReference:educationOrganizationId::int') }} as ed_org_id,
        {{ jget('v:educationOrganizationReference:link:rel::string') }}             as ed_org_type,
        {{ jget('v:programReference:educationOrganizationId::int') }}               as program_ed_org_id, 
        {{ jget('v:beginDate::date') }}                                             as program_enroll_begin_date,
        {{ jget('v:endDate::date') }}                                               as program_enroll_end_date,
        {{ jget('v:programReference:programName::string') }}                        as program_name,

        {{ jget('v:directCertification::boolean') }}                                as direct_certification,
        {{ jget('v:servedOutsideOfRegularSession::boolean') }}                      as served_outside_of_regular_session,

        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }}        as program_type,
        {{ extract_descriptor('v:reasonExitedDescriptor') }}                        as reason_exited,

        -- references
        {{ jget('v:educationOrganizationReference') }}                              as education_organization_reference,
        {{ jget('v:programReference') }}                                            as program_reference,
        {{ jget('v:studentReference') }}                                            as student_reference,

        -- lists
        {{ jget('v:programParticipationStatuses::string') }}                                as v_program_participation_statuses,
        {{ jget('v:schoolFoodServiceProgramServices::string') }}                            as v_school_food_service_program_services,

        -- edfi extensions
        {{ jget('v:_ext::string') }}                                                        as v_ext

    from source_stu_program
)

select * from renamed