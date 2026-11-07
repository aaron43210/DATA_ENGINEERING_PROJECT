-- OSM ids are unique per type only, so check the pair.
select osm_type, osm_id
from {{ source('geoplatform', 'vector_features') }}
group by osm_type, osm_id
having count(*) > 1
