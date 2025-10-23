with stg_academic_records as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
flattened as (
    select
        tenant_code,
        api_year,
        k_student_academic_record,
        {{ extract_descriptor('value:diplomaTypeDescriptor::string') }} as diploma_type,
        {{ jget('value:diplomaAwardDate::date') }} as diploma_award_date,
        {{ jget('value:diplomaDescription::string') }} as diploma_description,
        {{ extract_descriptor('value:diplomaLevelDescriptor::string') }} as diploma_level_descriptor,
        {{ extract_descriptor('value:achievementCategoryDescriptor::string') }} as achievement_category_descriptor,
        {{ jget('value:achievementCategorySystem::string') }} as achievement_category_system,
        {{ jget('value:achievementTitle::string') }} as achievement_title,
        {{ jget('value:criteria::string') }} as criteria,
        {{ jget('value:criteriaUrl::string') }} as criteria_url,
        {{ jget('value:cteCompleter::boolean') }} as is_cte_completer,
        {{ jget('value:diplomaAwardExpiresDate::date') }} as diploma_award_expires_date,
        {{ jget('value:evidenceStatement::string') }} as evidence_statement,
        {{ jget('value:imageURL::string') }} as image_url,
        {{ jget('value:issuerName::string') }} as issuer_name,
        {{ jget('value:issuerOriginURL::string') }} as issuer_origin_url,
        -- edfi extensions
        {{ jget('value:_ext::string') }} as v_ext
    from stg_academic_records
        {{ json_flatten('v_diplomas') }}
),
-- pull out extensions from v_diplomas.v_ext to their own columns
extended as (
    select
        flattened.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from flattened
)
select * from extended
