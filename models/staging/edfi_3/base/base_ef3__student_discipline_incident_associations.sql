-- note: this model is deprecated, but still in use
with student_discipline_incident as (
    {{ source_edfi3('student_discipline_incident_associations') }}
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
        -- descriptors
        {{ extract_descriptor('v:studentParticipationCodeDescriptor::string') }} as student_participation_code,
        -- references
        {{ jget("v:disciplineIncidentReference") }} as discipline_incident_reference,
        {{ jget("v:studentReference") }}           as student_reference,
        -- lists
        {{ jget("v:behaviors") }}                  as v_behaviors,

        -- edfi extensions
        {{ jget("v:_ext") }} as v_ext
    from student_discipline_incident
)
select * from renamed