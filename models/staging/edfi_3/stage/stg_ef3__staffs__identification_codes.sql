with stage_staffs as (
    select * from {{ ref('stg_ef3__staffs') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_staff,
        {{ extract_descriptor('value:staffIdentificationSystemDescriptor::string') }} as id_system,
        {{ jget('value:assigningOrganizationIdentificationCode::string') }} as assigning_org,
        {{ jget('value:identificationCode::string') }} as id_code
    from stage_staffs
        {{ json_flatten('v_identification_codes') }}
)
select * from flattened
