with source as (
    select * from {{ source('geoplatform', 'satellite_observations') }}
),

renamed as (
    select
        id as observation_id,
        scene_id,
        satellite as satellite_name,
        acquisition_time,
        cloud_percentage,
        crs as coordinate_reference_system,
        s3_bronze_path,
        lakefs_commit_id,
        bbox as geometry_polygon,
        ingested_at as row_ingested_at
    from source
)

select * from renamed
