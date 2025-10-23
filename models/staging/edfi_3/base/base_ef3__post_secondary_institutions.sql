with post_secondary_institutions as (
    {{ source_edfi3('post_secondary_institutions') }}
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
        {{ jget("v:postSecondaryInstitutionId::int") }} as post_secondary_institution_id,
        -- non-identity components
        {{ jget("v:nameOfInstitution::string") }}       as name_of_institution,
        {{ jget("v:shortNameOfInstitution::string") }}  as short_name_of_institution,
        {{ jget("v:webSite::string") }}                 as web_site,
        -- descriptors
        {{ extract_descriptor('v:postSecondaryInstitutionLevelDescriptor::string')}} as post_secondary_institution_level,
        {{ extract_descriptor('v:administrativeFundingControlDescriptor::string')}}  as administrative_funding_control,
        {{ extract_descriptor('v:federalLocaleCodeDescriptor::string')}}             as federal_locale_code,
        {{ extract_descriptor('v:operationalStatusDescriptor::string')}}             as operational_status_descriptor,
        -- unflattened lists
        {{ jget("v:categories::string") }}             as v_categories,
        {{ jget("v:addresses::string") }}              as v_addresses,
        {{ jget("v:identificationCodes::string") }}    as v_identification_codes,
        {{ jget("v:indicators::string") }}             as v_indicators,
        {{ jget("v:institutionTelephones::string") }}  as v_institution_telephones,
        {{ jget("v:internationalAddresses::string") }} as v_international_addresses,
        {{ jget("v:mediumOfInstructions::string") }}   as v_medium_of_instructions
    from post_secondary_institutions
)
select * from renamed
