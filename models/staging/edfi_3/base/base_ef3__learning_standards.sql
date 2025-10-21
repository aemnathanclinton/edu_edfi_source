with learning_standards as (
    {{ source_edfi3('learning_standards') }}
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
        -- ids
        {{ jget('v:id::string') }}                              as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:learningStandardId::string') }}              as learning_standard_id,
        {{ jget('v:identificationCodes') }}                     as v_learning_standard_identification_codes,
        -- descriptions
        {{ extract_descriptor('v:learningStandardCategoryDescriptor::string') }} as learning_standard_category,
        {{ extract_descriptor('v:learningStandardScopeDescriptor::string') }}    as learning_standard_scope,
        {{ jget('v:learningStandardItemCode::string') }}        as learning_standard_item_code,
        {{ jget('v:courseTitle::string') }}                     as course_title,
        {{ jget('v:description::string') }}                     as learning_standard_description,
        {{ jget('v:academicSubjects') }}                        as v_academic_subjects,
        {{ jget('v:contentStandard') }}                         as v_content_standard,
        {{ jget('v:gradeLevels') }}                             as v_grade_levels,
        {{ jget('v:prerequisiteLearningStandards') }}           as v_prerequisite_learning_standards,
        {{ jget('v:successCriteria::string') }}                 as success_criteria,
        {{ jget('v:namespace::string') }}                       as namespace,
        {{ jget('v:uri::string') }}                             as uri,
        -- references
        {{ jget('v:parentLearningStandardReference') }}         as parent_learning_standard_reference,
        -- edfi extensions
        {{ jget('v:_ext') }}                                    as v_ext
    from learning_standards
)
select * from renamed
