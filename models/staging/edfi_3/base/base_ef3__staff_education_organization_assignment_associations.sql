with staff_ed_org_assign as (
    {{ source_edfi3('staff_education_organization_assignment_associations') }}
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
        {{ jget("v:id::string") }} as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }} as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}             as ed_org_type,
        {{ jget("v:staffReference:staffUniqueId::string") }}                        as staff_unique_id,
        {{ jget("v:positionTitle::string") }}                                       as position_title,
        {{ jget("v:beginDate::date") }}                                             as begin_date,
        {{ jget("v:endDate::date") }}                                               as end_date,
        {{ jget("v:fullTimeEquivalency::float") }}                                  as full_time_equivalency,
        {{ jget("v:orderOfAssignment::float") }}                                    as order_of_assignment,
        {{ jget("v:credentialReference:credentialIdentifier::string") }}            as credential_identifier,
        -- descriptors
        {{ extract_descriptor('v:credentialReference:stateOfIssueStateAbbreviationDescriptor::string') }} as credential_state,
        {{ extract_descriptor('v:staffClassificationDescriptor::string') }}                               as staff_classification,
        -- references
        {{ jget("v:credentialReference") }}            as credential_reference,
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        {{ jget("v:staffReference") }}                 as staff_reference,
        {{ jget("v:employmentStaffEducationOrganizationEmploymentAssociationReference") }} as staff_ed_org_employment_reference,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from staff_ed_org_assign
)
select * from renamed