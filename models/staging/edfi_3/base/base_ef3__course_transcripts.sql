with course_transcripts as (
    {{ source_edfi3('course_transcripts') }}
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
				--identity components
				{{ extract_descriptor('v:courseAttemptResultDescriptor::string') }} as course_attempt_result,
        {{ jget('v:courseReference:educationOrganizationId::int') }}                      as course_ed_org_id,
        {{ jget('v:courseReference:courseCode::string') }}                                as course_code,
        {{ jget('v:studentAcademicRecordReference:educationOrganizationId::int') }}       as student_academic_record_ed_org_id,
        {{ jget('v:studentAcademicRecordReference:schoolYear::int') }}                    as school_year,
        {{ jget('v:studentAcademicRecordReference:studentUniqueId::string') }}            as student_unique_id,
        {{ extract_descriptor('v:studentAcademicRecordReference:termDescriptor::string') }} as academic_term,
				--non-identity components
        {{ jget('v:courseTitle::string') }}                             as course_title,
        {{ jget('v:alternativeCourseCode::string') }}                   as alternative_course_code,
        {{ jget('v:alternativeCourseTitle::string') }}                  as alternative_course_title,
        {{ jget('v:finalLetterGradeEarned::string') }}                  as final_letter_grade_earned,
        {{ jget('v:finalNumericGradeEarned::float') }}                  as final_numeric_grade_earned,
        {{ jget('v:earnedCredits::float') }}                            as earned_credits,
        {{ jget('v:earnedCreditConversion::float') }}                   as earned_credit_conversion,
        {{ jget('v:attemptedCredits::float') }}                         as attempted_credits,
        {{ jget('v:attemptedCreditConversion::float') }}                as attempted_credit_conversion,
        {{ jget('v:assigningOrganizationIdentificationCode::string') }} as assigning_organization_identification_code,
        {{ jget('v:courseCatalogURL::string') }}                        as course_catalog_url,
        -- descriptors
        {{ extract_descriptor('v:courseRepeatCodeDescriptor::string') }}      as course_repeat_code,
        {{ extract_descriptor('v:attemptedCreditTypeDescriptor::string') }}   as attempted_credit_type,
        {{ extract_descriptor('v:earnedCreditTypeDescriptor::string') }}      as earned_credit_type,
        {{ extract_descriptor('v:methodCreditEarnedDescriptor::string') }}    as method_credit_earned,
        {{ extract_descriptor('v:whenTakenGradeLevelDescriptor::string') }}   as when_taken_grade_level,
				{{ jget('v:externalEducationOrganizationReference:educationOrganizationId::int') }} as external_ed_org_reference_ed_org_id,
        -- references
        {{ jget('v:courseReference') }}                        as course_reference,
        {{ jget('v:studentAcademicRecordReference') }}         as student_academic_record_reference,
        {{ jget('v:externalEducationOrganizationReference') }} as external_education_organization_reference,
        {{ jget('v:responsibleTeacherStaffReference') }}       as responsible_teacher_staff_reference,
				-- non-identity collection components
				{{ jget('v:earnedAdditionalCredits') }}              as v_earned_additional_credits,
        {{ jget('v:academicSubjects') }}                     as v_academic_subjects,
        {{ jget('v:alternativeCourseIdentificationCodes') }} as v_alternative_course_identification_codes,
        {{ jget('v:creditCategories') }}                     as v_credit_categories,
        {{ jget('v:coursePrograms') }}                       as v_programs,
        {{ jget('v:sections') }}                             as v_sections,

        -- edfi extensions
        {{ jget('v:_ext') }} as v_ext
    from course_transcripts
)
select * from renamed
