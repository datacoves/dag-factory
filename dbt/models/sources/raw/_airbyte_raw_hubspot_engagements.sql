with raw_source as (

    select
        *
    from {{ source('raw', '_airbyte_raw_hubspot_engagements') }}

),

final as (

    select
        _airbyte_data:"active"::varchar as active,
        _airbyte_data:"allAccessibleTeamIds"::varchar as allaccessibleteamids,
        _airbyte_data:"associations"::varchar as associations,
        _airbyte_data:"attachments"::varchar as attachments,
        _airbyte_data:"bodyPreview"::varchar as bodypreview,
        _airbyte_data:"bodyPreviewHtml"::varchar as bodypreviewhtml,
        _airbyte_data:"bodyPreviewIsTruncated"::varchar as bodypreviewistruncated,
        _airbyte_data:"createdAt"::varchar as createdat,
        _airbyte_data:"createdBy"::varchar as createdby,
        _airbyte_data:"gdprDeleted"::varchar as gdprdeleted,
        _airbyte_data:"id"::varchar as id,
        _airbyte_data:"lastUpdated"::varchar as lastupdated,
        _airbyte_data:"metadata"::varchar as metadata,
        _airbyte_data:"modifiedBy"::varchar as modifiedby,
        _airbyte_data:"ownerId"::varchar as ownerid,
        _airbyte_data:"portalId"::varchar as portalid,
        _airbyte_data:"queueMembershipIds"::varchar as queuemembershipids,
        _airbyte_data:"source"::varchar as source,
        _airbyte_data:"teamId"::varchar as teamid,
        _airbyte_data:"timestamp"::varchar as timestamp,
        _airbyte_data:"type"::varchar as type,
        _airbyte_data:"uid"::varchar as uid,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

