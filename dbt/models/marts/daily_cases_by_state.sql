with daily_cases as (

    select * from {{ ref('stg_cases_deaths_daily_usa') }}
),

final as (

    select
        state,
        new_case,
        submission_date, 
        new_death

    from daily_cases

)

select * from final
