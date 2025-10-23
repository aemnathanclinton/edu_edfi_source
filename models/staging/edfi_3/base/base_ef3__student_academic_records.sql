with academic_records as (
    {{ source_edfi3('student_academic_records') }}
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
        ods_version,
        data_model_version,
        -- identity components
        {{ jget("v:educationOrganizationReference:educationOrganizationId::int") }} as ed_org_id,
        {{ jget("v:educationOrganizationReference:link:rel::string") }}             as ed_org_type,
        {{ jget("v:schoolYearTypeReference:schoolYear::int") }}                     as school_year,
        {{ jget("v:studentReference:studentUniqueId::string") }}                    as student_unique_id,
        {{ extract_descriptor('v:termDescriptor::string') }}          as academic_term,
        -- non-identity components
        {{ jget("v:cumulativeAttemptedCreditConversion::float") }}  as cumulative_attempted_credit_conversion,
        {{ jget("v:cumulativeAttemptedCredits::float") }}           as cumulative_attempted_credits,
        {{ jget("v:cumulativeEarnedCreditConversion::float") }}     as cumulative_earned_credit_conversion,
        {{ jget("v:cumulativeEarnedCredits::float") }}              as cumulative_earned_credits,
        {{ jget("v:projectedGraduationDate::date") }}               as projected_graduation_date,
        {{ jget("v:sessionAttemptedCreditConversion::float") }}     as session_attempted_credit_conversion,
        {{ jget("v:sessionAttemptedCredits::float") }}              as session_attempted_credits,
        {{ jget("v:sessionEarnedCreditConversion::float") }}        as session_earned_credit_conversion,
        {{ jget("v:sessionEarnedCredits::float") }}                 as session_earned_credits,
        {{ jget("v:classRanking:classRank::float") }}               as class_rank,
        {{ jget("v:classRanking:totalNumberInClass::float") }}      as class_rank_total_students,
        {{ jget("v:classRanking:percentageRanking::float") }}       as class_percent_rank,
        {{ jget("v:classRanking:classRankingDate::date") }}         as class_rank_date,
        -- deprecated components (use gpa list instead)
        {{ jget("v:cumulativeGradePointAverage::float") }}          as cumulative_gpa,
        {{ jget("v:cumulativeGradePointsEarned::float") }}          as cumulative_grade_points_earned,
        {{ jget("v:gradeValueQualifier::string") }}                 as grade_value_qualifier,
        {{ jget("v:sessionGradePointAverage::float") }}             as session_gpa,
        {{ jget("v:sessionGradePointsEarned::float") }}             as session_grade_points_earned,
        -- descriptors
        {{ extract_descriptor('v:sessionEarnedCreditTypeDescriptor::string') }}       as session_earned_credit_type,
        {{ extract_descriptor('v:sessionAttemptedCreditTypeDescriptor::string') }}    as session_attempted_credit_type,
        {{ extract_descriptor('v:cumulativeEarnedCreditTypeDescriptor::string') }}    as cumulative_earned_credit_type,
        {{ extract_descriptor('v:cumulativeAttemptedCreditTypeDescriptor::string') }} as cumulative_attempted_credit_type,
        -- references 
        {{ jget("v:studentReference") }}               as student_reference,
        {{ jget("v:educationOrganizationReference") }} as education_organization_reference,
        -- lists
        {{ jget("v:academicHonors::string") }}     as v_academic_honors,
        {{ jget("v:diplomas::string") }}           as v_diplomas,
        {{ jget("v:gradePointAverages::string") }} as v_grade_point_averages,
        {{ jget("v:recognitions::string") }}       as v_recognitions,
        {{ jget("v:reportCards::string") }}        as v_report_cards,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from academic_records
)
select * from renamed