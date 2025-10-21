with parents as (
    {{ source_edfi3('parents') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        __last_modified_timestamp as last_modified_timestamp,
        filename,
        __is_deleted as is_deleted,

        {{ jget("v:id::string") }}                                                      as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:parentUniqueId::string") }}                                          as parent_unique_id,
        {{ jget("v:personReference:personId::string") }}                                as person_id,
        {{ jget("v:firstName::string") }}                                               as first_name,
        {{ jget("v:middleName::string") }}                                              as middle_name,
        {{ jget("v:lastSurname::string") }}                                             as last_name,
        {{ jget("v:maidenName::string") }}                                              as maiden_name,
        {{ jget("v:generationCodeSuffix::string") }}                                    as generation_code_suffix,
        {{ jget("v:personalTitlePrefix::string") }}                                     as personal_title_prefix,
        -- the following three fields were introduced to the Contacts resource, which replaced Parents in v5.0
        -- including them here (they will always be null) to allow the tables to be unioned in stage
        {{ jget("v:genderIdentity::string") }}                                          as gender_identity, 
        {{ jget("v:preferredFirstName::string") }}                                      as preferred_first_name,
        {{ jget("v:preferredLastSurname::string") }}                                    as preferred_last_name,
        {{ jget("v:loginId::string") }}                                                 as login_id,
        {{ extract_descriptor('v:sexDescriptor::string') }}                              as sex,
        {{ extract_descriptor('v:highestCompletedLevelOfEducationDescriptor::string') }} as highest_completed_level_of_education,
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }}     as person_source_system,
        -- references
        {{ jget("v:personReference") }}                 as person_reference,
        -- unflattened lists
        {{ jget("v:addresses") }}                       as v_addresses,
        {{ jget("v:internationalAddresses") }}          as v_international_addresses,
        {{ jget("v:electronicMails") }}                 as v_electronic_mails,
        {{ jget("v:telephones") }}                      as v_telephones,
        {{ jget("v:languages") }}                       as v_languages,
        {{ jget("v:otherNames") }}                      as v_other_names,
        {{ jget("v:personalIdentificationDocuments") }} as v_personal_identification_documents,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from parents
)
select * from renamed