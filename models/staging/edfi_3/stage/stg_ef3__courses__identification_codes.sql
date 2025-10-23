with stg_courses as (
    select * from {{ ref('stg_ef3__courses') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_course,
        {{ extract_descriptor('value:courseIdentificationSystemDescriptor::string') }} as id_system,
        {{ jget('value:assigningOrganizationIdentificationCode::string') }} as assigning_org,
        {{ jget('value:courseCatalogURL::string') }} as course_catalog_url,
        {{ jget('value:identificationCode::string') }} as id_code from stg_courses
        {{ json_flatten('v_identification_codes') }}
)
select * from flattened
