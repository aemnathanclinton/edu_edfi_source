with survey_question as (
    {{ source_edfi3('survey_questions') }}
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

        {{ jget("v:id::string") }}                                                  as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:surveyReference:surveyIdentifier::string") }}                    as survey_id,
        {{ jget("v:surveyReference:namespace::string") }}                           as namespace,
        {{ jget("v:surveySectionReference:surveySectionTitle::string") }}           as survey_section_title,
        {{ jget("v:questionCode::string") }}                                        as question_code,
        {{ jget("v:questionText::string") }}                                        as question_text,
        -- descriptors
        {{ extract_descriptor('v:questionFormDescriptor::string') }} as question_form,
        --references
        {{ jget("v:surveyReference") }}          as survey_reference,
        {{ jget("v:surveySectionReference") }}   as survey_section_reference,
        -- lists
        {{ jget("v:matrices::string") }}                     as v_matrices,
        {{ jget("v:responseChoices::string") }}              as v_response_choices,
        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from survey_question
)
select * from renamed
