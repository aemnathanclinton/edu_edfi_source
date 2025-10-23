with survey_question_responses as (
    {{ source_edfi3('survey_question_responses') }}
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
        {{ jget("v:surveyQuestionReference:namespace::string") }}                   as namespace,
        {{ jget("v:surveyQuestionReference:surveyIdentifier::string") }}            as survey_id,
        {{ jget("v:surveyQuestionReference:questionCode::string") }}                as question_code,
        {{ jget("v:surveyResponseReference:surveyResponseIdentifier::string") }}    as survey_response_id,
        {{ jget("v:comment::string") }}                                             as comment,
        {{ jget("v:noResponse::boolean") }}                                         as no_response,
        --references
        {{ jget("v:surveyQuestionReference") }}   as survey_question_reference,
        {{ jget("v:surveyResponseReference") }}   as survey_response_reference,
        -- lists
        {{ jget("v:surveyQuestionMatrixElementResponses::string") }}  as v_survey_question_matrix_element_responses,
        {{ jget("v:values::string") }}                                as v_values,
        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from survey_question_responses
)
select * from renamed
