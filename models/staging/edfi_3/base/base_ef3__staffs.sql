with src_staffs as (
    {{ source_edfi3('staffs') }}
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
        {{ jget("v:id::string") }}                                 as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:staffUniqueId::string") }}                      as staff_unique_id,
        {{ jget("v:loginId::string") }}                            as login_id,
        {{ jget("v:firstName::string") }}                          as first_name,
        {{ jget("v:lastSurname::string") }}                        as last_name,
        {{ jget("v:maidenName::string") }}                         as maiden_name,
        {{ jget("v:middleName::string") }}                         as middle_name,
        {{ jget("v:generationCodeSuffix::string") }}               as generation_code_suffix,
        {{ jget("v:personalTitlePrefix::string") }}                as personal_title_prefix,
        {{ jget("v:genderIdentity::string") }}                     as gender_identity,
        {{ jget("v:preferredFirstName::string") }}                 as preferred_first_name,
        {{ jget("v:preferredLastSurname::string") }}               as preferred_last_name,
        {{ jget("v:personReference:personId::string") }}           as person_id,
        {{ jget("v:birthDate::date") }}                            as birth_date,
        {{ jget("v:hispanicLatinoEthnicity::boolean") }}           as has_hispanic_latino_ethnicity,
        {{ jget("v:highlyQualifiedTeacher::boolean") }}            as is_highly_qualified_teacher,
        {{ jget("v:yearsOfPriorProfessionalExperience::float") }}  as years_of_prior_professional_experience,
        {{ jget("v:yearsOfPriorTeachingExperience::float") }}      as years_of_prior_teaching_experience,
        -- descriptors
        {{ extract_descriptor('v:sexDescriptor::string') }}                                 as gender,
        {{ extract_descriptor('v:highestCompletedLevelOfEducationDescriptor::string') }}    as highest_completed_level_of_education,
        {{ extract_descriptor('v:citizenshipStatusDescriptor::string') }}                   as citizenship_status,
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }}        as person_source_system,
        {{ extract_descriptor('v:oldEthnicityDescriptor:sourceSystemDescriptor::string') }} as old_ethnicity,
        -- references
        {{ jget("v:personReference") }}                 as person_reference,
        -- lists
        {{ jget("v:addresses::string") }}                       as v_addresses,
        {{ jget("v:ancestryEthnicOrigins::string") }}           as v_ancestry_ethnic_origins,
        {{ jget("v:credentials::string") }}                     as v_credentials,
        {{ jget("v:electronicMails::string") }}                 as v_electronic_mails,
        {{ jget("v:identificationCodes::string") }}             as v_identification_codes,
        {{ jget("v:identificationDocuments::string") }}         as v_identification_documents,
        {{ jget("v:personalIdentificationDocuments::string") }} as v_personal_identification_documents,
        {{ jget("v:internationalAddresses::string") }}          as v_international_addresses,
        {{ jget("v:languages::string") }}                       as v_languages,
        {{ jget("v:otherNames::string") }}                      as v_other_names,
        {{ jget("v:races::string") }}                           as v_races,
        {{ jget("v:recognitions::string") }}                    as v_recognitions,
        {{ jget("v:telephones::string") }}                      as v_telephones,
        {{ jget("v:tribalAffiliations::string") }}              as v_tribal_affiliations,
        {{ jget("v:visas::string") }}                           as v_visas,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from src_staffs
)
select * from renamed