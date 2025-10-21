with seas as (
    {{ source_edfi3('state_education_agencies') }}
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
        -- fields
        {{ jget("v:id::string") }}                     as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:stateEducationAgencyId::int") }}    as sea_id,
        {{ jget("v:nameOfInstitution::string") }}      as sea_name,
        {{ jget("v:shortNameOfInstitution::string") }} as sea_short_name,
        {{ jget("v:webSite::string") }}                as website,
        -- descriptors
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }} as operational_status,
        -- lists
        {{ jget("v:accountabilities") }}       as v_accountabilities,
        {{ jget("v:addresses") }}              as v_addresses,
        {{ jget("v:categories") }}             as v_categories,
        {{ jget("v:federalFunds") }}           as v_federal_funds,
        {{ jget("v:identificationCodes") }}    as v_identification_codes,
        {{ jget("v:indicators") }}             as v_indicators,
        {{ jget("v:institutionTelephones") }}  as v_institution_telephones,
        {{ jget("v:internationalAddresses") }} as v_international_addresses,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from seas
) 
select * from renamed