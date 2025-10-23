with leas as (
    {{ source_edfi3('local_education_agencies') }}
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
        {{ jget('v:id::string') }}                      as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:localEducationAgencyId::int') }}     as lea_id,
        {{ jget('v:nameOfInstitution::string') }}       as lea_name,
        {{ jget('v:shortNameOfInstitution::string') }}  as lea_short_name,
        {{ jget('v:webSite::string') }}                 as website,
        -- reference ids
        {{ jget('v:parentLocalEducationAgencyReference:localEducationAgencyId::int') }} as parent_lea_id,
        {{ jget('v:educationServiceCenterReference:educationServiceCenterId::int') }}   as esc_id,
        {{ jget('v:stateEducationAgencyReference:stateEducationAgencyId::int') }}       as sea_id,
        -- descriptors
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }}            as operational_status,
        {{ extract_descriptor('v:charterStatusDescriptor::string') }}                as charter_status,
        {{ extract_descriptor('v:localEducationAgencyCategoryDescriptor::string') }} as lea_category,
        --references
        {{ jget('v:parentLocalEducationAgencyReference') }} as parent_local_education_agency_reference,
        {{ jget('v:stateEducationAgencyReference') }}       as state_education_agency_reference,
        {{ jget('v:educationServiceCenterReference') }}     as education_service_center_reference,
        -- unflattened lists
        {{ jget('v:accountabilities::string') }}       as v_accountabilities,
        {{ jget('v:addresses::string') }}              as v_addresses,
        {{ jget('v:categories::string') }}             as v_categories,
        {{ jget('v:federalFunds::string') }}           as v_federal_funds,
        {{ jget('v:identificationCodes::string') }}    as v_identification_codes,
        {{ jget('v:indicators::string') }}             as v_indicators,
        {{ jget('v:institutionTelephones::string') }}  as v_institution_telephones,
        {{ jget('v:internationalAddresses::string') }} as v_international_addresses,

        -- edfi extensions
        {{ jget('v:_ext::string') }} as v_ext
    from leas
)
select * from renamed