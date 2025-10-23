with service_centers as (
    {{ source_edfi3('education_service_centers') }}
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
        {{ jget('v:id::string') }}                                            as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:educationServiceCenterId::string') }}                      as esc_id,
        {{ jget('v:nameOfInstitution::string') }}                             as esc_name,
        {{ jget('v:shortNameOfInstitution::string') }}                        as esc_short_name,
        {{ jget('v:stateEducationAgencyReference:stateEducationAgencyId') }}  as sea_id,
        {{ jget('v:webSite::string') }}                                       as website,
        -- descriptors
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }} as operational_status,
        -- unflattened lists
        {{ jget('v:addresses::string') }}                         as v_addresses,
        {{ jget('v:categories::string') }}                        as v_categories,
        {{ jget('v:identificationCodes::string') }}               as v_identification_codes,
        {{ jget('v:indicators::string') }}                        as v_indicators,
        {{ jget('v:institutionTelephones::string') }}             as v_institution_telephones,
        {{ jget('v:internationalAddresses::string') }}            as v_international_addresses,
        -- references
        {{ jget('v:stateEducationAgencyReference') }}     as state_education_agency_reference,
        -- edfi extensions
        {{ jget('v:_ext::string') }} as v_ext
    from service_centers
)
select * from renamed