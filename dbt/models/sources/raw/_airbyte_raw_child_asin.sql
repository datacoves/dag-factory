with raw_source as (

    select
        *
    from {{ source('raw', '_airbyte_raw_child_asin') }}

),

final as (

    select
        _airbyte_data:"(Child) ASIN"::varchar as _child__asin,
        _airbyte_data:"(Parent) ASIN"::varchar as _parent__asin,
        _airbyte_data:"Buy Box Percentage"::varchar as buy_box_percentage,
        _airbyte_data:"Ordered Product Sales"::varchar as ordered_product_sales,
        _airbyte_data:"Ordered Product Sales - B2B"::varchar as ordered_product_sales_-_b2b,
        _airbyte_data:"Page Views"::varchar as page_views,
        _airbyte_data:"Page Views Percentage"::varchar as page_views_percentage,
        _airbyte_data:"Session Percentage"::varchar as session_percentage,
        _airbyte_data:"Sessions"::varchar as sessions,
        _airbyte_data:"Title"::varchar as title,
        _airbyte_data:"Total Order Items"::varchar as total_order_items,
        _airbyte_data:"Total Order Items - B2B"::varchar as total_order_items_-_b2b,
        _airbyte_data:"Unit Session Percentage"::varchar as unit_session_percentage,
        _airbyte_data:"Unit Session Percentage - B2B"::varchar as unit_session_percentage_-_b2b,
        _airbyte_data:"Units Ordered"::varchar as units_ordered,
        _airbyte_data:"Units Ordered - B2B"::varchar as units_ordered_-_b2b,
        _airbyte_data:"_ab_additional_properties"::varchar as _ab_additional_properties,
        _airbyte_data:"_ab_source_file_last_modified"::varchar as _ab_source_file_last_modified,
        _airbyte_data:"_ab_source_file_url"::varchar as _ab_source_file_url,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

