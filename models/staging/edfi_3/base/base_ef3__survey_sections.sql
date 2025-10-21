with survey_sections as (
    {{ source_edfi3('survey_sections') }}
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
        {{ jget("v:surveyReference:surveyIdentifier::string") }} as survey_id,
        {{ jget("v:surveyReference:namespace::string") }}        as namespace,
        {{ jget("v:surveySectionTitle::string") }}               as survey_section_title,
        -- references
        {{ jget("v:surveyReference") }} as survey_reference
    from survey_sections
)
select * from renamed
