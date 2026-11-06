{{ config(
    materialized='view'
) }}

/*
  Staging model for Sentinel-2 satellite metadata (ETL output by Aaron).
*/

with source_satellite as (
    select * from {{ source('geoplatform', 'satellite_observations') }}
),

renamed as (
    select
        scene_id,
        satellite,
        acquisition_time,
        cloud_percentage,
        crs,
        s3_bronze_path,
        bbox as footprint_geom,
        -- Quality categorization
        case
            when cloud_percentage <= 10.0 then 'CLEAR'
            when cloud_percentage <= 30.0 then 'MODERATE'
            else 'CLOUD_COVERED'
        end as cloud_usability_flag,
        ingested_at
    from source_satellite
    where bbox is not null
      and ST_IsValid(bbox)
)

select * from renamed
