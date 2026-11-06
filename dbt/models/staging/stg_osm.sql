{{ config(
    materialized='view'
) }}

/*
  Staging model for OpenStreetMap vector infrastructure (ELT output).
  Filters and projects raw OSM data with standardized geometry typing.
*/

with source_osm as (
    select * from {{ source('geoplatform', 'vector_features') }}
),

renamed as (
    select
        osm_id,
        lower(feature_type) as feature_category,
        coalesce(name, 'Unnamed Facility') as feature_name,
        -- Extract common healthcare & infrastructure tags from JSONB
        tags->>'amenity' as amenity_type,
        tags->>'emergency' as emergency_type,
        tags->>'healthcare' as healthcare_specialty,
        tags->>'operator' as operator_name,
        tags->>'capacity' as bed_capacity,
        tags->>'phone' as contact_phone,
        geometry as geom,
        -- Calculate spatial centroid coordinates
        ST_X(ST_Centroid(geometry)) as longitude,
        ST_Y(ST_Centroid(geometry)) as latitude,
        ingested_at
    from source_osm
    where geometry is not null
      and ST_IsValid(geometry)
)

select * from renamed
