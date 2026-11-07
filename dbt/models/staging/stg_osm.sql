{{ config(materialized='view') }}

/*
  Staging model for OpenStreetMap vector infrastructure (ELT output).
  anchor_point is a point guaranteed to lie on the feature, so long roads and
  curved lines can be assigned to a district or scene reliably (a centroid can
  fall off the feature).
*/

with source_osm as (
    select * from {{ source('geoplatform', 'vector_features') }}
),

renamed as (
    select
        osm_type,
        osm_id,
        osm_type || '/' || osm_id as feature_key,
        lower(feature_type) as feature_category,
        coalesce(name, 'Unnamed Facility') as feature_name,
        tags->>'amenity' as amenity_type,
        tags->>'emergency' as emergency_type,
        tags->>'healthcare' as healthcare_specialty,
        tags->>'operator' as operator_name,
        tags->>'capacity' as bed_capacity,
        tags->>'phone' as contact_phone,
        geometry as geom,
        ST_PointOnSurface(geometry) as anchor_point,
        ST_X(ST_PointOnSurface(geometry)) as longitude,
        ST_Y(ST_PointOnSurface(geometry)) as latitude,
        ingested_at
    from source_osm
    where geometry is not null
      and ST_IsValid(geometry)
)

select * from renamed
