with network_associations as (
    {{ source_edfi3('education_organization_network_associations') }}
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
        {{ jget('v:id::string') }}                            as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:educationOrganizationNetworkReference:educationOrganizationNetworkId::int') }} as network_id,
        {{ jget('v:memberEducationOrganizationReference:educationOrganizationId::int') }}         as ed_org_id,
        {{ jget('v:beginDate::date') }}                       as begin_date,
        {{ jget('v:endDate::date') }}                         as end_date,
        -- references
        {{ jget('v:educationOrganizationNetworkReference') }} as network_reference,
        {{ jget('v:memberEducationOrganizationReference') }}  as education_organization_reference,
        -- edfi extensions
        {{ jget('v:_ext::string') }} as v_ext

    from network_associations
)
select * from renamed
