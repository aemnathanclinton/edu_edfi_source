with staff_ed_org_contact_assoc as (
    {{ source_edfi3('staff_education_organization_contact_associations') }}
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
        {{ jget('v:contactTitle::string') }}                                        as contact_title,
        {{ jget('v:educationOrganizationReference:educationOrganizationId::int') }} as ed_org_id,
        lower({{ jget('v:electronicMailAddress::string') }})                        as email_address,
        {{ jget('v:staffReference:staffUniqueId::int') }}                           as staff_unique_id,
        -- arrays
        {{ jget('v:telephones::string') }}                                                  as v_telephones,
        -- references
        {{ jget('v:educationOrganizationReference') }}                              as education_organization_reference,
        {{ jget('v:staffReference') }}                                              as staff_reference,
        -- edfi extensions
        {{ jget('v:_ext::string') }}                                                        as v_ext

    from staff_ed_org_contact_assoc
)
select * from renamed
