with objective_assessments as (
    {{ source_edfi3('objective_assessments') }}
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
        {{ jget('v:id::string') }}                                       as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:assessmentReference:assessmentIdentifier::string') }} as assessment_identifier,
        {{ jget('v:assessmentReference:namespace::string') }}            as namespace,
        {{ jget('v:identificationCode::string') }}                       as objective_assessment_identification_code,
        {{ jget('v:description::string') }}                              as objective_assessment_description,
        {{ jget('v:maxRawScore::float') }}                               as max_raw_score,
        {{ jget('v:nomenclature::string') }}                             as nomenclature,
        {{ jget('v:percentOfAssessment::float') }}                       as percent_of_assessment,
        -- descriptors
        {{ extract_descriptor('v:academicSubjectDescriptor::string') }} as academic_subject_descriptor,
        -- references
        {{ jget('v:assessmentReference') }}                as assessment_reference,
        {{ jget('v:parentObjectiveAssessmentReference') }} as parent_objective_assessment_reference,
        -- unflattened lists
        {{ jget('v:assessmentItems') }}    as v_assessment_items,
        {{ jget('v:learningObjectives') }} as v_learning_objectives,
        {{ jget('v:learningStandards') }}  as v_learning_standards,
        {{ jget('v:performanceLevels') }}  as v_performance_levels,
        {{ jget('v:scores') }}             as v_scores,
        -- edfi extensions
        {{ jget('v:_ext') }}               as v_ext
    from objective_assessments
)
select * from renamed