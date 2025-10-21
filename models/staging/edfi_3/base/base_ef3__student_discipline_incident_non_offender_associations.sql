with student_discipline_incident_nonoffender as (
    {{ source_edfi3('student_discipline_incident_non_offender_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        __last_modified_timestamp as last_modified_timestamp,
        filename,
        file_row_number,
        __is_deleted as is_deleted,
        {{ jget("v:id::string") }}                                             as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:disciplineIncidentReference:incidentIdentifier::string") }} as incident_id,
        {{ jget("v:disciplineIncidentReference:schoolId::int") }}              as school_id,
        {{ jget("v:studentReference:studentUniqueId::string") }}               as student_unique_id,
        -- references
        {{ jget("v:disciplineIncidentReference") }} as discipline_incident_reference,
        {{ jget("v:studentReference") }}           as student_reference,
        -- lists
        {{ jget("v:disciplineIncidentParticipationCodes") }} as v_discipline_incident_participation_codes,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from student_discipline_incident_nonoffender
)
select * from renamed