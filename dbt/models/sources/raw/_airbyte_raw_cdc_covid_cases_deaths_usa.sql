with raw_source as (

    select
        *
    from {{ source('raw', '_airbyte_raw_cdc_covid_cases_deaths_usa') }}

),

final as (

    select
        _airbyte_data:"consent_cases"::varchar as consent_cases,
        _airbyte_data:"consent_deaths"::varchar as consent_deaths,
        _airbyte_data:"created_at"::varchar as created_at,
        _airbyte_data:"new_case"::varchar as new_case,
        _airbyte_data:"new_death"::varchar as new_death,
        _airbyte_data:"pnew_case"::varchar as pnew_case,
        _airbyte_data:"pnew_death"::varchar as pnew_death,
        _airbyte_data:"state"::varchar as state,
        _airbyte_data:"submission_date"::varchar as submission_date,
        _airbyte_data:"tot_cases"::varchar as tot_cases,
        _airbyte_data:"tot_death"::varchar as tot_death,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

