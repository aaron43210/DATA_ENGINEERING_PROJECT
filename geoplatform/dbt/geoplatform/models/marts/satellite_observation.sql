{{ config(
    materialized='table',
    indexes=[
      {'columns': ['acquisition_time']},
      {'columns': ['geometry_polygon'], 'type': 'gist'}
    ]
) }}

with staged_satellite as (
    select * from {{ ref('stg_sentinel2') }}
)

select
    scene_id,
    satellite_name,
    acquisition_time,
    cloud_percentage,
    
    -- Business logic: categorizing image usability
    case
        when cloud_percentage < 10 then 'Excellent'
        when cloud_percentage < 30 then 'Good'
        else 'Poor'
    end as image_quality,
    
    s3_bronze_path,
    lakefs_commit_id,
    geometry_polygon
from staged_satellite

-- Only expose scenes that are reasonably usable to the API
where cloud_percentage <= 50
