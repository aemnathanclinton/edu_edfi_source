with survey_section_responses as (
    {{ source_edfi3('survey_section_responses') }}
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
        {{ jget("v:surveyResponseReference:namespace::string") }}                as namespace,
        {{ jget("v:surveySectionReference:surveyIdentifier::string") }}          as survey_id,
        {{ jget("v:surveyResponseReference:surveyResponseIdentifier::string") }} as survey_response_id,
        {{ jget("v:surveySectionReference:surveySectionTitle::string") }}        as survey_section_title,
        -- non-identity components
        {{ jget("v:sectionRating::float") }} as section_rating,
        -- references
        {{ jget("v:surveyResponseReference") }} as survey_response_reference,
        {{ jget("v:surveySectionReference") }}  as survey_section_reference
    from survey_section_responses
)
select * from renamed
