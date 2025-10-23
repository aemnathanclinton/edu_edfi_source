with networks as (
    {{ source_edfi3('education_organization_networks') }}
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
        {{ jget('v:id::string') }}                              as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:educationOrganizationNetworkId::int') }}     as network_id,
        {{ jget('v:nameOfInstitution::string') }}               as network_name,
        {{ jget('v:shortNameOfInstitution::string') }}          as network_short_name,
        {{ jget('v:webSite::string') }}                         as website,
        -- descriptors
        {{ extract_descriptor('v:networkPurposeDescriptor::string') }}    as network_purpose,
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }} as operational_status,
        -- unflattened lists
        {{ jget('v:categories::string') }}                              as v_categories,
        {{ jget('v:addresses::string') }}                               as v_addresses,
        {{ jget('v:identificationCodes::string') }}                     as v_identification_codes,
        {{ jget('v:indicators::string') }}                              as v_indicators,
        {{ jget('v:institutionTelephones::string') }}                   as v_institution_telephones,
        {{ jget('v:internationalAddresses::string') }}                  as v_international_addresses,
        -- edfi extensions
        {{ jget('v:_ext::string') }} as v_ext
    from networks
)
select * from renamed
