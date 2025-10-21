with discipline_incident as (
    {{ source_edfi3('discipline_incidents') }}
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
        {{ jget('v:id::string') }}                                  as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:incidentIdentifier::string') }}                  as incident_id,
        {{ jget('v:schoolReference:schoolId::int') }}               as school_id,
        {{ jget('v:incidentDate::date') }}                          as incident_date,
        {{ jget('v:incidentTime::string') }}                        as incident_time,
        {{ jget('v:caseNumber::string') }}                          as case_number,
        {{ jget('v:incidentCost::float') }}                         as incident_cost,
        {{ jget('v:incidentDescription::string') }}                 as incident_description,
        {{ jget('v:reportedToLawEnforcement::boolean') }}           as was_reported_to_law_enforcement,
        {{ jget('v:reporterName::string') }}                        as reporter_name,
        --descriptors
        {{ extract_descriptor('v:reporterDescriptionDescriptor::string') }} as reporter_description,
        {{ extract_descriptor('v:incidentLocationDescriptor::string') }}    as incident_location,
        --lists
        {{ jget('v:behaviors') }}            as v_behaviors,
        {{ jget('v:externalParticipants') }} as v_external_participants,
        {{ jget('v:weapons') }}              as v_weapons,
        --references
        {{ jget('v:schoolReference') }}      as school_reference,
        --deprecated
        {{ jget('v:staffReference') }}       as staff_reference,
        {{ jget('v:staffReference:staffUniqueId::string') }} as staff_unique_id,

        -- edfi extensions
        {{ jget('v:_ext') }} as v_ext
    from discipline_incident
)
select * from renamed
