with stu_ed_org as (
    {{ source_edfi3('student_education_organization_associations') }}
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
        -- key columns
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }} as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}             as ed_org_type,
        {{ jget("v:studentReference:studentUniqueId::string") }}                    as student_unique_id,
        -- value columns
        {{ jget("v:hispanicLatinoEthnicity::boolean") }} as has_hispanic_latino_ethnicity,
        {{ jget("v:loginId::string") }}                  as login_id,
        {{ jget("v:profileThumbnail::string") }}                 as profile_thumbnail_url,
        {{ jget("v:genderIdentity::string") }}                   as gender_identity,
        -- descriptors
        {{ extract_descriptor('v:limitedEnglishProficiencyDescriptor::string') }}   as lep_code,
        {{ extract_descriptor('v:sexDescriptor::string') }}                         as gender,
        {{ extract_descriptor('v:oldEthnicityDescriptor::string') }}                as old_ethnicity,
        {{ extract_descriptor('v:supporterMilitaryConnectionDescriptor::string') }} as supporter_military_connection,
        -- references
        {{ jget("v:studentReference") }}               as student_reference,
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        -- lists
        {{ jget("v:addresses::string") }}                  as v_addresses,
        {{ jget("v:ancestryEthnicOrigins::string") }}      as v_ancestry_ethnic_origins,
        {{ jget("v:cohortYears::string") }}                as v_cohort_years,
        {{ jget("v:disabilities::string") }}               as v_disabilities,
        {{ jget("v:electronicMails::string") }}            as v_electronic_mails,
        {{ jget("v:internationalAddresses::string") }}     as v_international_addresses,
        {{ jget("v:languages::string") }}                  as v_languages,
        {{ jget("v:programParticipations::string") }}      as v_program_participations, -- deprecated
        {{ jget("v:races::string") }}                      as v_races,
        {{ jget("v:studentCharacteristics::string") }}     as v_student_characteristics,
        {{ jget("v:studentIdentificationCodes::string") }} as v_student_identification_codes,
        {{ jget("v:studentIndicators::string") }}          as v_student_indicators,
        {{ jget("v:telephones::string") }}                 as v_telephones,
        {{ jget("v:tribalAffiliations::string") }}         as v_tribal_affiliations,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from stu_ed_org
)
select * from renamed
