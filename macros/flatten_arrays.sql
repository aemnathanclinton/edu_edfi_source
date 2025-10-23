-- flatten emails
{% macro flatten_emails(stg_ref, keys) %}
with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {%- for key in keys %}
        {{key}},
        {% endfor -%}
        {{ edu_edfi_source.extract_descriptor('value:electronicMailTypeDescriptor::string') }} as email_type,
        lower({{ jget('value:electronicMailAddress::string') }}) as email_address,
        {{ jget('value:primaryEmailAddressIndicator::boolean') }} as is_primary_email,
        {{ jget('value:doNotPublishIndicator::boolean') }} as do_not_publish
    from stg
        {{ edu_edfi_source.json_flatten('v_electronic_mails') }}
)
select * from flattened
{% endmacro %}

-- flatten telephones
{% macro flatten_telephones(stg_ref, keys) %}
with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {%- for key in keys %}
        {{key}},
        {% endfor -%}
        {{ edu_edfi_source.extract_descriptor('value:telephoneNumberTypeDescriptor::string') }} as phone_number_type,
        {{ jget('value:telephoneNumber::string') }} as phone_number,
        {{ jget('value:orderOfPriority::int') }} as priority_order,
        {{ jget('value:doNotPublishIndicator::boolean') }} as do_not_publish,
        {{ jget('value:textMessageCapabilityIndicator::boolean') }} as is_text_message_capable
    from stg
        {{ edu_edfi_source.json_flatten('v_telephones') }}
)
select * from flattened
{% endmacro %}

-- flatten addresses
{% macro flatten_addresses(stg_ref, keys) %}
with stg as (
    select * from {{ ref(stg_ref) }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        {%- for key in keys %}
        {{key}},
        {% endfor -%}
        {{ edu_edfi_source.extract_descriptor('addr.value:addressTypeDescriptor::string') }} as address_type,
        {{ jget('addr.value:streetNumberName') }} as street_address,
        {{ jget('addr.value:apartmentRoomSuiteNumber') }} as apartment_room_suite_number,
        {{ jget('addr.value:city') }} as city,
        {{ jget('addr.value:nameOfCounty') }} as name_of_county,
        {{ edu_edfi_source.extract_descriptor('addr.value:stateAbbreviationDescriptor::string') }} as state_code,
        {{ jget('addr.value:postalCode::string') }} as postal_code,
        {{ jget('addr.value:buildingSiteNumber::string') }} as building_site_number,
        {{ edu_edfi_source.extract_descriptor('addr.value:localeDescriptor::string') }} as locale,
        {{ jget('addr.value:congressionalDistrict::string') }} as congressional_district,
        {{ jget('addr.value:countyFIPSCode::string') }} as county_fips_code,
        {{ jget('addr.value:doNotPublishIndicator::boolean') }} as do_not_publish,
        {{ jget('addr.value:latitude::string') }} as latitude,
        {{ jget('addr.value:longitude::string') }} as longitude,
        {{ jget('timing.value:beginDate::date') }} as address_begin_date,
        {{ jget('timing.value:endDate::date') }} as address_end_date
    from stg
        {{ edu_edfi_source.json_flatten('v_addresses', 'addr') }}
            {{ edu_edfi_source.json_flatten(jget('addr.value:periods::array'), 'timing', outer=True) }}
),
full_address as (
    select *,
        concat(
            street_address, ', ',
            {% if target.type == 'sqlserver' %}
            isnull(apartment_room_suite_number, '')
            {% else %}
            coalesce(apartment_room_suite_number, '')
            {% endif %},
            case
                when apartment_room_suite_number is null
                    then ''
                else ', '
            end,
            city, ', ',
            state_code, ' ',
            postal_code
        ) as full_address
    from flattened
)
select * from full_address
{% endmacro %}
