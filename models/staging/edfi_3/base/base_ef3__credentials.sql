with credentials as (
    {{ source_edfi3('credentials') }}
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

        {{ jget('v:id::string') }} as record_guid,
        -- identity components
        {{ jget('v:credentialIdentifier::string') }}                                                as credential_id,
        {{ extract_descriptor('v:stateOfIssueStateAbbreviationDescriptor::string') }} as state_of_issue_state_abbreviation,
        -- non-identity components
        {{ jget('v:effectiveDate::date') }}  as effective_date,
        {{ jget('v:expirationDate::date') }} as expiration_date,
        {{ jget('v:issuanceDate::date') }}   as issuance_date,
        {{ jget('v:namespace::string') }}    as namespace,
        -- descriptors
        {{ extract_descriptor('v:credentialFieldDescriptor::string') }}               as credential_field,
        {{ extract_descriptor('v:credentialTypeDescriptor::string') }}                as credential_type,
        {{ extract_descriptor('v:teachingCredentialDescriptor::string') }}            as teaching_credential,
        {{ extract_descriptor('v:teachingCredentialBasisDescriptor::string') }}       as teaching_credential_basis,
        -- unnested lists
        {{ jget('v:endorsements') }}     as v_endorsements,
        {{ jget('v:academicSubjects') }} as v_academic_subjects,
        {{ jget('v:gradeLevels') }}      as v_grade_levels,
        -- references
        {{ jget('v:studentAcademicRecords') }} as student_academic_records,
        -- edfi extensions
        {{ jget('v:_ext') }} as v_ext 
    from credentials
)
select * from renamed
