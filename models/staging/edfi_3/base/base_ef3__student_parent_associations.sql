with student_parent_associations as (
    {{ source_edfi3('student_parent_associations') }}
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

        {{ jget("v:id::string") }}                                             as record_guid,
        ods_version,
        data_model_version,
        {{ jget("v:contactPriority::int") }}                                   as contact_priority,
        {{ jget("v:contactRestrictions::string") }}                            as contact_restrictions,
        {{ jget("v:emergencyContactStatus::boolean") }}                        as is_emergency_contact,
        {{ jget("v:livesWith::boolean") }}                                     as is_living_with,
        {{ jget("v:primaryContactStatus::boolean") }}                          as is_primary_contact,
        {{ jget("v:legalGuardian::boolean") }}                                 as is_legal_guardian,
        {{ extract_descriptor('v:relationDescriptor::string') }} as relation_type,
        -- references
        {{ jget("v:parentReference") }}                                        as parent_reference,
        {{ jget("v:studentReference") }}                                       as student_reference,

        -- edfi extensions
        {{ jget("v:_ext::string") }} as v_ext
    from student_parent_associations
)
select * from renamed