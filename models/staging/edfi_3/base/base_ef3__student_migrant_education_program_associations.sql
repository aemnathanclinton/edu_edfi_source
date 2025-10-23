with source_stu_programs as (
    {{ source_edfi3('student_migrant_education_program_associations') }}
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

        {{ jget('v:id::string') }}                                                               as record_guid, 
        ods_version, 
        data_model_version, 
        {{ jget('v:studentReference:studentUniqueId::string') }}                                  as student_unique_id, 
        {{ jget('v:educationOrganizationReference:educationOrganizationId::int') }}               as ed_org_id,
        {{ jget('v:educationOrganizationReference:link:rel::string') }}                           as ed_org_type,
        {{ jget('v:beginDate::date') }}                                                           as program_enroll_begin_date,
        {{ jget('v:endDate::date') }}                                                             as program_enroll_end_date, 
        {{ jget('v:programReference:programName::string') }}                                      as program_name, 

        {{ jget('v:priorityForServices::boolean') }}                                              as priority_for_service, 

        {{ jget('v:lastQualifyingMove::date') }}                                                  as last_qualifying_move, 
        {{ jget('v:usInitialEntry::date') }}                                                      as us_initial_entry,
        {{ jget('v:usMostRecentEntry::date') }}                                                   as us_most_recent_entry,
        {{ jget('v:USInitialSchoolEntry::date') }}                                                as us_initial_school_entry,
        {{ jget('v:qualifyingArrivalDate::date') }}                                               as qualifying_arrival_date,
        {{ jget('v:stateResidencyDate::date') }}                                                  as state_residency_date,
        {{ jget('v:eligibilityExpirationDate::date') }}                                           as eligibility_expiration_date,
        -- descriptors
        {{ extract_descriptor('v:programReference:programTypeDescriptor') }}                        as program_type,
        {{ extract_descriptor('v:continuationOfServicesReasonDescriptor') }}                        as continuation_of_services_reason,

        -- references
        {{ jget('v:educationOrganizationReference') }}                                            as education_organization_reference,
        {{ jget('v:programReference') }}                                                          as program_reference, 
        {{ jget('v:studentReference') }}                                                          as student_reference,

        -- lists
        {{ jget('v:migrantEducationProgramServices::string') }}                                           as v_migrant_education_program_services,
        {{ jget('v:programParticipationStatuses::string') }}                                              as v_program_participation_statuses,

        -- edfi extensions
        {{ jget('v:_ext::string') }}                                                                      as v_ext
    from source_stu_programs
)

select * from renamed

