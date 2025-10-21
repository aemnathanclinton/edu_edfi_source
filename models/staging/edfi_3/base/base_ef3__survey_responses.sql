with survey_responses as (
    {{ source_edfi3('survey_responses') }}
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
        {{ jget("v:surveyResponseIdentifier::string") }}                            as survey_response_id,
        {{ jget("v:studentReference:studentUniqueId::string") }}                    as student_unique_id,
        {{ jget("v:electronicMailAddress::string") }}                               as electronic_mail_address,
        {{ jget("v:fullName::string") }}                                            as full_name, 
        {{ jget("v:location::string") }}                                            as location, 
        {{ jget("v:responseDate::date") }}                                          as response_date, 
        {{ jget("v:responseTime::int") }}                                           as completion_time_seconds, 
        --references
        {{ jget("v:surveyReference") }}                               as survey_reference,
        {{ jget("v:studentReference") }}                              as student_reference,
        {{ jget("v:staffReference") }}                                as staff_reference,
        coalesce({{ jget("v:parentReference") }}, {{ jget("v:contactReference") }}) as contact_reference, -- parentReference renamed to contactReference in Data Standard v5.0
        -- lists
        {{ jget("v:surveyLevels") }}  as v_survey_levels,    
        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from survey_responses
)
select * from renamed
