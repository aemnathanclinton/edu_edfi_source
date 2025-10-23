with programs as (
    {{ source_edfi3('programs') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        __last_modified_timestamp as last_modified_timestamp,
        filename,
        file_row_number,
        __is_deleted as is_deleted,
        {{ jget("v:id::string") }}                                                  as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }} as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}             as ed_org_type,
        {{ jget("v:programId::string") }}                                           as program_id,
        {{ jget("v:programName::string") }}                                         as program_name,
        -- descriptors
        {{ extract_descriptor('v:programTypeDescriptor::string') }} as program_type,
        -- references
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        -- lists
        {{ jget("v:characteristics::string") }}    as v_characteristics,
        {{ jget("v:learningObjectives::string") }} as v_learning_objectives,
        {{ jget("v:learningStandards::string") }}  as v_learning_standards,
        {{ jget("v:services::string") }}           as v_services,
        {{ jget("v:sponsors::string") }}           as v_sponsors,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from programs
)
select * from renamed