with discipline_actions as (
    {{ source_edfi3('discipline_actions') }}
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
        {{ jget('v:disciplineActionIdentifier::string') }}          as discipline_action_id,
        {{ jget('v:disciplineDate::date') }}                        as discipline_date,
        {{ jget('v:assignmentSchoolReference:schoolId::int') }}     as assignment_school_id,
        {{ jget('v:responsibilitySchoolReference:schoolId::int') }} as responsibility_school_id,
        {{ jget('v:studentReference:studentUniqueId::string') }}    as student_unique_id,
        {{ jget('v:disciplineActionLength::float') }}               as discipline_action_length,
        {{ jget('v:actualDisciplineActionLength::float') }}         as actual_discipline_action_length,
        {{ jget('v:iepPlacementMeetingIndicator::boolean') }}       as triggered_iep_placement_meeting,
        {{ jget('v:relatedToZeroTolerancePolicy::boolean') }}       as is_related_to_zero_tolerance_policy,
        --descriptors
        {{ extract_descriptor('v:disciplineActionLengthDifferenceReasonDescriptor::string') }} as discipline_action_length_difference_reason,
        -- references
        {{ jget('v:assignmentSchoolReference') }}     as assignment_school_reference,
        {{ jget('v:responsibilitySchoolReference') }} as responsibility_school_reference,
        {{ jget('v:studentReference') }}              as student_reference,
        -- lists
        {{ jget('v:disciplines') }}                                   as v_disciplines,
        {{ jget('v:studentDisciplineIncidentAssociations') }}         as v_student_discipline_incident_associations,
        {{ jget('v:staffs') }}                                        as v_staffs,
        {{ jget('v:studentDisciplineIncidentBehaviorAssociations') }} as v_student_discipline_incident_behavior_associations,

        -- edfi extensions
        {{ jget('v:_ext') }} as v_ext
    from discipline_actions
)
select * from renamed