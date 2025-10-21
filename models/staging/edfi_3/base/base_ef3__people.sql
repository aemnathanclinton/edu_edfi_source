with people as (
    {{ source_edfi3('people') }}
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
        -- identity components
        {{ jget("v:personId::int") }} as person_id,
        {{ extract_descriptor('v:sourceSystemDescriptor::string') }} as source_system
    from people
)
select * from renamed
