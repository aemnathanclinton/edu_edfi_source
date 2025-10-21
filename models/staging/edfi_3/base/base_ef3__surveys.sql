with surveys as (
    {{ source_edfi3('surveys') }}
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

        {{ jget("v:id::string") }}                                                  as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:namespace::string") }}                                           as namespace,
        {{ jget("v:surveyIdentifier::string") }}                                    as survey_id,
        {{ jget("v:schoolYearTypeReference:schoolYear::string") }}                  as school_year,
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }} as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}             as ed_org_type,
        {{ jget("v:surveyTitle::string") }}                                         as survey_title,
        {{ jget("v:sessionReference:school_id::string") }}                          as school_id,
        {{ jget("v:sessionReference:session_name::string") }}                       as session_name,
        {{ jget("v:numberAdministered::int") }}                                     as number_administered,
        -- descriptors
        {{ extract_descriptor('v:surveyCategoryDescriptor::string') }} as survey_category,
        --references
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        {{ jget("v:schoolYearTypeReference") }}        as school_year_reference,
        {{ jget("v:sessionReference") }}               as session_reference,
        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from surveys
)
select * from renamed
