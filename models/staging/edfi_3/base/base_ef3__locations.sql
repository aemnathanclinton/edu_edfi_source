with locations as (
    {{ source_edfi3('locations') }}
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
        {{ jget('v:id::string') }}                          as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:classroomIdentificationCode::string') }} as classroom_id_code,
        {{ jget('v:schoolReference:schoolId::int') }}       as school_id,
        {{ jget('v:maximumNumberOfSeats::int') }}           as maximum_seating,
        {{ jget('v:optimumNumberOfSeats::int') }}           as optimum_seating,
        {{ jget('v:schoolReference') }}                     as school_reference,

        -- edfi extensions
        {{ jget('v:_ext') }} as v_ext
    from locations
)
select * from renamed