with descriptors as (
    {{ source_edfi3('_descriptors', join_deletes=False) }}
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
        name                           as descriptor_name,
        {{ jget('v:id::string') }}                   as record_guid,
        ods_version,
        data_model_version,
        {{ jget('v:codeValue::string') }}            as code_value,
        {{ jget('v:namespace::string') }}            as namespace,
        {{ jget('v:description::string') }}          as description,
        {{ jget('v:shortDescription::string') }}     as short_description
    from descriptors
)
select * 
from renamed
