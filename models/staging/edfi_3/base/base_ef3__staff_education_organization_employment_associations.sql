with staff_ed_org_employ as (
    {{ source_edfi3('staff_education_organization_employment_associations') }}
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
        {{ jget("v:credentialReference:credentialIdentifier::string") }}            as credential_identifier,
        {{ jget("v:department::string") }}                                          as department,
        {{ jget("v:hireDate::date") }}                                              as hire_date,
        {{ jget("v:endDate::date") }}                                               as end_date,
        {{ jget("v:fullTimeEquivalency::float") }}                                  as full_time_equivalency,
        {{ jget("v:hourlyWage::float") }}                                           as hourly_wage,
        {{ jget("v:annualWage::float") }}                                           as annual_wage,
        {{ jget("v:offerDate::date") }}                                             as offer_date,
        -- descriptors
        {{ extract_descriptor('v:employmentStatusDescriptor::string') }}                                  as employment_status,
        {{ extract_descriptor('v:credentialReference:stateOfIssueStateAbbreviationDescriptor::string') }} as credential_state,
        {{ extract_descriptor('v:separationDescriptor::string') }}                                        as separation,
        {{ extract_descriptor('v:separationReasonDescriptor::string') }}                                  as separation_reason,
        -- references
        {{ jget("v:credentialReference") }}            as credential_reference,
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        {{ jget("v:staffReference") }}                 as staff_reference,
        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from staff_ed_org_employ
)
select * from renamed