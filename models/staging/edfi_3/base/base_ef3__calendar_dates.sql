with calendar_dates as (
    {{ source_edfi3('calendar_dates') }}
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
        {{ jget("v:id::string") }}                              as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:date::date") }}                              as calendar_date,
        {{ jget("v:calendarReference:calendarCode::string") }}  as calendar_code,
        {{ jget("v:calendarReference:schoolId::integer") }}     as school_id,
        {{ jget("v:calendarReference:schoolYear::integer") }}   as school_year,
        {{ jget("v:calendarReference") }}                       as calendar_reference,
        {{ jget("v:calendarEvents::string") }}                          as v_calendar_events,
        {{ json_array_size(jget("v:calendarEvents")) }} as n_calendar_events,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from calendar_dates
)
select * from renamed
