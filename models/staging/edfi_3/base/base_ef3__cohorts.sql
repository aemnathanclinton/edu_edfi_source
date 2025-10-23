with cohorts as (
    {{ source_edfi3('cohorts') }}
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
        {{ jget('v:id::string') }}                                                  as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:educationOrganizationReference:educationOrganizationId::int') }} as ed_org_id,
        {{ jget('v:educationOrganizationReference:link:rel::string') }}             as ed_org_type,
        {{ jget('v:cohortDescription::string') }}                                   as cohort_description,
        {{ jget('v:cohortIdentifier::string') }}                                    as cohort_id,
        -- descriptors
        {{ extract_descriptor('v:cohortScopeDescriptor::string') }} as cohort_scope,
        {{ extract_descriptor('v:cohortTypeDescriptor::string') }}  as cohort_type,
        -- references
        {{ jget('v:educationOrganizationReference') }} as education_organization_reference,
        -- lists
        {{ jget('v:programs::string') }} as v_programs,
        
        -- edfi extensions
        {{ jget('v:_ext::string') }} as v_ext
    from cohorts
)
select * from renamed