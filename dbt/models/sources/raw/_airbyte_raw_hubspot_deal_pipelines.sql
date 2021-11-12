with raw_source as (

    select
        *
    from {{ source('raw', '_airbyte_raw_hubspot_deal_pipelines') }}

),

final as (

    select
        _airbyte_data:"active"::varchar as active,
        _airbyte_data:"createdAt"::varchar as createdat,
        _airbyte_data:"default"::varchar as default,
        _airbyte_data:"displayOrder"::varchar as displayorder,
        _airbyte_data:"label"::varchar as label,
        _airbyte_data:"objectType"::varchar as objecttype,
        _airbyte_data:"objectTypeId"::varchar as objecttypeid,
        _airbyte_data:"pipelineId"::varchar as pipelineid,
        _airbyte_data:"stages"::varchar as stages,
        _airbyte_data:"updatedAt"::varchar as updatedat,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

