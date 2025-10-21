with students as (
    {{ source_edfi3('students') }}
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
        {{ jget("v:id::string") }}                         as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:studentUniqueId::string") }}            as student_unique_id,
        {{ jget("v:firstName::string") }}                  as first_name,
        {{ jget("v:middleName::string") }}                 as middle_name,
        {{ jget("v:lastSurname::string") }}                as last_name,
        {{ jget("v:generationCodeSuffix::string") }}       as generation_code_suffix,
        {{ jget("v:maidenName::string") }}                 as maiden_name,
        {{ jget("v:personalTitlePrefix::string") }}        as personal_title_prefix,
        {{ jget("v:personReference:personId::string") }}   as person_id,
        {{ jget("v:birthDate::date") }}                    as birth_date,
        {{ jget("v:birthCity::string") }}                  as birth_city,
        {{ jget("v:birthInternationalProvince::string") }} as birth_international_province,
        {{ jget("v:dateEnteredUS::date") }}                as date_entered_us,
        {{ jget("v:multipleBirthStatus::boolean") }}       as is_multiple_birth,
        -- descriptors
        {{ extract_descriptor('v:personReference:sourceSystemDescriptor::string') }} as person_source_system,
        {{ extract_descriptor('v:birthSexDescriptor::string') }}                     as birth_sex,
        {{ extract_descriptor('v:citizenshipStatusDescriptor::string') }}            as citizenship_status,
        {{ extract_descriptor('v:birthStateAbbreviationDescriptor::string') }}       as birth_state,
        {{ extract_descriptor('v:birthCountryDescriptor::string') }}                 as birth_country,
        -- nested lists
        {{ jget("v:identificationDocuments") }}         as v_identification_documents,
        {{ jget("v:otherNames") }}                      as v_other_names,
        {{ jget("v:personalIdentificationDocuments") }} as v_personal_identification_documents,
        {{ jget("v:visas") }}                           as v_visas,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from students
)
select * from renamed