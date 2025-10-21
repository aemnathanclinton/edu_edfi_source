with survey_response_education_organization_target_associations as (
    {{ source_edfi3('survey_response_education_organization_target_associations') }}
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
        -- identity components
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }} as ed_org_id,
        {{ jget("v:surveyResponseReference:namespace::string") }}                   as namespace,
        {{ jget("v:surveyResponseReference:surveyIdentifier::string") }}            as survey_id,
        {{ jget("v:surveyResponseReference:surveyResponseIdentifier::string") }}    as survey_response_id,
        -- non-identity components
        {{ jget("v:educationOrganizationReference:link:rel::string") }} as ed_org_type,
        -- references
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        {{ jget("v:surveyResponseReference") }}        as survey_response_reference
    from survey_response_education_organization_target_associations
)
select * from renamed
