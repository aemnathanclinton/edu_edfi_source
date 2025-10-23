with source_stu_programs as (
    {{ source_edfi3('student_program_associations') }}
),

renamed as (
    select
        -- generic columns
        tenant_code,
        api_year,
        pull_timestamp,
        __last_modified_timestamp as last_modified_timestamp,
        file_row_number,
        filename,
        __is_deleted as is_deleted,

        {{ jget("v:id::string") }}                                                      as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:studentReference:studentUniqueId::string") }}                        as student_unique_id,
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }}      as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}                  as ed_org_type,
        {{ jget("v:programReference:educationOrganizationId::int") }}                    as program_ed_org_id,
        {{ jget("v:beginDate::date") }}                                                  as program_enroll_begin_date,
        {{ jget("v:endDate::date") }}                                                    as program_enroll_end_date,
        {{ jget("v:programReference:programName::string") }}                             as program_name,
        {{ jget("v:servedOutsideOfRegularSession::boolean") }}                           as is_served_outside_regular_session,
        {{ jget("v:participationStatus:designatedBy::string") }}                         as participation_status_designated_by,
        {{ jget("v:participationStatus:statusBeginDate::date") }}                        as participation_status_begin_date,
        {{ jget("v:participationStatus:statusEndDate::date") }}                          as participation_status_end_date,

        -- descriptors
        {{ extract_descriptor('v:participationStatus:participationStatusDescriptor::string') }} as participation_status,
        {{ extract_descriptor('v:programReference:programTypeDescriptor::string') }}            as program_type,
        {{ extract_descriptor('v:reasonExitedDescriptor::string') }}                            as reason_exited,

        -- references
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        {{ jget("v:programReference") }}               as program_reference,
        {{ jget("v:studentReference") }}               as student_reference,

        -- lists
        {{ jget("v:programParticipationStatuses::string") }} as v_program_participation_statuses,
        {{ jget("v:services::string") }}                     as v_services,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext

    from source_stu_programs
)

select * from renamed
