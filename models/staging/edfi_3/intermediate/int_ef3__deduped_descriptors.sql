-- this model deduplicates stg_ef3__descriptors across tenants & years, to simplify the code needed in edu_edfi_source.extract_descriptor
    -- to replace descriptor code_values with short descriptions
-- in the future, we could allow for more sophisticated order by logic, to not just select rows based on alphabetical tenant order
with stg_descriptors as (
    select * from {{ ref('stg_ef3__descriptors') }}
),

-- note, order by tenant and api to ensure consistency over time (although it will change as new tenants are added)
-- todo: better way to configure the order by?
deduped_raw as (
    select
        *,
        row_number() over (
            partition by namespace, code_value 
            order by tenant_code, api_year desc
        ) as __rn_new
    from stg_descriptors
),

deduped as (
    select 
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,
        record_guid,
        ods_version,
        data_model_version,
        descriptor_name,
        code_value,
        namespace,
        description,
        short_description,
        k_descriptor,
        school_year
    from deduped_raw
    where __rn_new = 1
),

subset as (
  select
    namespace,
    code_value,
    description,
    short_description
  from deduped
)

select * from subset