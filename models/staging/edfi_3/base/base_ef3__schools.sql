with schools as (
    {{ source_edfi3('schools') }}
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
        ods_version,
        data_model_version,
        {{ jget("v:schoolId::int") }}                                             as school_id,
        {{ jget("v:nameOfInstitution::string") }}                                 as school_name,
        {{ jget("v:shortNameOfInstitution::string") }}                            as school_short_name,
        {{ jget("v:webSite::string") }}                                           as website,
        {{ jget("v:localEducationAgencyReference:localEducationAgencyId::int") }} as lea_id,
        -- pull out school categories
        case
            when {{ json_array_size(jget("v:schoolCategories")) }} = 1
                then {{ extract_descriptor('v:schoolCategories[0]:schoolCategoryDescriptor::string') }}
            when {{ json_array_size(jget("v:schoolCategories")) }} > 1
                then 'Multiple Categories'
            else NULL
        end as school_category,
        -- descriptors
        {{ extract_descriptor('v:schoolTypeDescriptor::string') }}                         as school_type,
        {{ extract_descriptor('v:operationalStatusDescriptor::string') }}                  as operational_status,
        {{ extract_descriptor('v:administrativeFundingControlDescriptor::string') }}       as administrative_funding_control,
        {{ extract_descriptor('v:internetAccessDescriptor::string') }}                     as internet_access,
        {{ extract_descriptor('v:titleIPartASchoolDesignationDescriptor::string') }}       as title_i_part_a_school_designation,
        {{ extract_descriptor('v:charterStatusDescriptor::string') }}                      as charter_status,
        {{ extract_descriptor('v:charterApprovalAgencyTypeDescriptor::string') }}          as charter_approval_agency,
        {{ extract_descriptor('v:magnetSpecialProgramEmphasisSchoolDescriptor::string') }} as magnet_type,
        -- references
        {{ jget("v:localEducationAgencyReference") }}   as local_education_agency_reference,
        -- unflattened lists
        {{ jget("v:addresses::string") }}                       as v_addresses,
        {{ jget("v:educationOrganizationCategories::string") }} as v_education_organization_categories,
        {{ jget("v:gradeLevels::string") }}                     as v_grade_levels,
        {{ jget("v:identificationCodes::string") }}             as v_identification_codes,
        {{ jget("v:indicators::string") }}                      as v_indicators,
        {{ jget("v:institutionTelephones::string") }}           as v_institution_telephones,
        {{ jget("v:internationalAddresses::string") }}          as v_international_addresses,
        {{ jget("v:schoolCategories::string") }}                as v_school_categories,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from schools
)
select * from renamed
