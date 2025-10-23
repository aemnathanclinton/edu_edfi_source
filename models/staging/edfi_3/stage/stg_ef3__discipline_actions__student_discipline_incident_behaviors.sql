with stg_discipline_actions as (
    select * from {{ ref('stg_ef3__discipline_actions') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        k_student_xyear,
        {{ extract_descriptor('value:studentDisciplineIncidentBehaviorAssociationReference:behaviorDescriptor::string') }} as behavior_type,
        {{ jget('value:studentDisciplineIncidentBehaviorAssociationReference:incidentIdentifier::string') }} as incident_id,
        {{ jget('value:studentDisciplineIncidentBehaviorAssociationReference:schoolId::string') }} as school_id,
        {{ jget('value:studentDisciplineIncidentBehaviorAssociationReference:studentUniqueId::string') }} as student_unique_id,

        -- edfi extensions
        {{ jget('value:_ext::string') }} as v_ext
        from stg_discipline_actions
        {{ json_flatten('v_student_discipline_incident_behavior_associations') }}

        union all

        select
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        k_student_xyear,
        null as behavior_type,
        {{ jget('value:studentDisciplineIncidentAssociationReference:incidentIdentifier::string') }} as incident_id,
        {{ jget('value:studentDisciplineIncidentAssociationReference:schoolId::string') }} as school_id,
        {{ jget('value:studentDisciplineIncidentAssociationReference:studentUniqueId::string') }} as student_unique_id,

        -- edfi extensions
        {{ jget('value:_ext::string') }} as v_ext
    from stg_discipline_actions
        {{ json_flatten('v_student_discipline_incident_behavior_associations') }}

    union all

    select
        tenant_code,
        api_year,
        discipline_action_id,
        discipline_date,
        k_student,
        k_student_xyear,
        null as behavior_type,
        {{ jget('value:studentDisciplineIncidentAssociationReference:incidentIdentifier::string') }} as incident_id,
        {{ jget('value:studentDisciplineIncidentAssociationReference:schoolId::string') }} as school_id,
        {{ jget('value:studentDisciplineIncidentAssociationReference:studentUniqueId::string') }} as student_unique_id,

        -- edfi extensions
        {{ jget('value:_ext::string') }} as v_ext
    from stg_discipline_actions
        {{ json_flatten('v_student_discipline_incident_associations') }}
),
extended as (
    select
        flattened.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from flattened
)
select * from extended