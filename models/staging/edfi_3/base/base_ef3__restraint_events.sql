with restraint_events as (
    {{ source_edfi3('restraint_events') }}
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
        {{ jget("v:id::string") }}                                         as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:schoolReference:schoolId::integer") }}                  as school_id,
        {{ jget("v:studentReference:studentUniqueId::string") }}           as student_unique_id,
        {{ jget("v:restraintEventIdentifier::string") }}                   as restraint_event_identifier,
        {{ jget("v:eventDate::date") }}                                    as event_date,
        -- descriptors
        {{ extract_descriptor('v:educationalEnvironmentDescriptor::string') }}   as educational_environment,
        -- references
        {{ jget("v:schoolReference") }}  as school_reference,
        {{ jget("v:studentReference") }} as student_reference,
        -- lists
        {{ jget("v:programs::string") }}         as v_programs,
        {{ jget("v:reasons::string") }}          as v_reasons,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from restraint_events
)
select * from renamed