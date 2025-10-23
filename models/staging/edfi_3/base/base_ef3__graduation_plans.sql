with graduation_plans as (
    {{ source_edfi3('graduation_plans') }}
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
        ods_version,
        data_model_version,
        {{ jget('v:educationOrganizationReference:educationOrganizationId::int') }}           as ed_org_id,
        {{ jget('v:educationOrganizationReference:link:rel::string') }}                       as ed_org_type,
        {{ jget('v:graduationSchoolYearTypeReference:schoolYear::int') }}                     as graduation_school_year,
        {{ extract_descriptor('v:graduationPlanTypeDescriptor::string') }}      as graduation_plan_type,
        {{ extract_descriptor('v:totalRequiredCreditTypeDescriptor::string') }} as total_required_credit_type,
        {{ jget('v:totalRequiredCreditConversion::float') }}                                  as total_required_credit_conversion,
        {{ jget('v:totalRequiredCredits::float') }}                                           as total_required_credits,
        {{ jget('v:individualPlan::boolean') }}                                               as is_individual_plan,
        -- lists
        {{ jget('v:creditsByCreditCategories::string') }} as v_credits_by_credit_categories,
        {{ jget('v:creditsByCourses::string') }}          as v_credits_by_courses,
        {{ jget('v:creditsBySubjects::string') }}         as v_credits_by_subjects,   
        {{ jget('v:requiredAssessments::string') }}       as v_required_assessments,
        --references
        {{ jget('v:educationOrganizationReference') }}    as education_organization_reference,

        -- edfi extensions
        {{ jget('v:_ext::string') }} as v_ext
    from graduation_plans
)
select * from renamed