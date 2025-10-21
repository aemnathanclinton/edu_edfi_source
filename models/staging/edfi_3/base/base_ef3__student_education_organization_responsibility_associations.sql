with source_stu_responsibility as (
    {{ source_edfi3('student_education_organization_responsibility_associations') }}
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
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }}     as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}                 as ed_org_type,
        {{ jget("v:beginDate::date") }}                                                 as begin_date,
        {{ jget("v:endDate::date") }}                                                   as end_date,

        -- descriptors
        {{ extract_descriptor('v:responsibilityDescriptor::string') }} as responsibility,

        -- references
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        {{ jget("v:studentReference") }}               as student_reference,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext

    from source_stu_responsibility
)

select * from renamed
