with stu_cohort_assoc as (
    {{ source_edfi3('student_cohort_associations') }}
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
        {{ jget("v:id::string") }}                                       as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:studentReference:studentUniqueId::string") }}         as student_unique_id,
        {{ jget("v:cohortReference:educationOrganizationId::integer") }} as cohort_ed_org_id,
        {{ jget("v:cohortReference:cohortIdentifier::string") }}         as cohort_id,
        {{ jget("v:beginDate::date") }}                                  as cohort_begin_date,
        {{ jget("v:endDate::date") }}                                    as cohort_end_date,
        -- references
        {{ jget("v:studentReference") }} as student_reference,
        {{ jget("v:cohortReference") }}  as cohort_reference,
        -- lists
        {{ jget("v:sections::string") }} as v_sections,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext

    from stu_cohort_assoc
)
select * from renamed
