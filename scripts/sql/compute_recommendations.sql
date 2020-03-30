--===================================================================================
-- CARGA DE DATOS
--===================================================================================

--===================================================================================
-- EJECUTAR ANALISIS
--===================================================================================
/*
 *  Número de eventos por alquiler que se encuentran en su barrio y a una distancia max de 5 km
 */
DROP TABLE IF EXISTS top_events;
CREATE TABLE top_events AS
select listings.id as airbnb_id, count(1) as num_events from 
( 
	select 
	airbnb.id
	,6371*2*asin(sqrt(
		pow(sin(radians(cast(events_calendar.latitud as float)-cast(airbnb.latitude as float))/2),2) 
		+ cos(radians(airbnb.latitude))
		*cos(radians(events_calendar.latitud))
		*pow(sin(radians(cast(events_calendar.longitud as float)-cast(airbnb.longitude as float))/2),2)
	)) as distance
	,events_calendar.descripcion 
	,events_calendar.tipo
	from airbnb
	inner join events_calendar on airbnb.zipcode = events_calendar.codigo_postal_instalacion 
	where airbnb.city = 'Madrid'
	and 
	(
		events_calendar.tipo like '%Exposiciones%' or
		events_calendar.tipo like '%TeatroPereformance%' or
		events_calendar.tipo like '%Circo%' or
		events_calendar.tipo like '%Teatro%' or
		events_calendar.tipo like '%Musica%' or
		events_calendar.tipo like '%Fiestas%' or
		events_calendar.tipo like '%Marionetas%'
	)
) as listings
where distance <=5 --km
group by listings.id;

/*
 *  Top de alquilers con mayor aferta de lugares de ocio recomendados a menos de 2 km, 
 *  pertenecientes a su barrio y con de 2 eventos programados para los proximos dias 
 */
DROP TABLE IF EXISTS recommended_listings;
CREATE TABLE recommended_listings AS
select 
	airbnb.name
	,airbnb.summary
	,top_list.num_poi
	,top_events.num_events
	,airbnb.price
	,airbnb.number_of_reviews
	,airbnb.review_scores_value
	,airbnb.listing_url
	,airbnb.thumbnail_url
	,airbnb.longitude
	,airbnb.latitude
from
(
	select listings.id, count(1) as num_poi, max(num_events) as num_events from 
	( 
		select 
		airbnb.id
		,6371*2*asin(sqrt(
			pow(sin(radians(cast(poi_data.poi_lat as float)-cast(airbnb.latitude as float))/2),2) 
			+ cos(radians(airbnb.latitude))
			*cos(radians(poi_data.poi_lat))
			*pow(sin(radians(cast(poi_data.poi_lon as float)-cast(airbnb.longitude as float))/2),2)
		)) as distance
		,poi_data.name 
		,poi_data.poi_category
		,top_events.num_events
		from airbnb 
		inner join top_events on airbnb.id = top_events.airbnb_id
		left join poi_data on airbnb.neighbourhood_cleansed = poi_data.neighbourhood
		where airbnb.city = 'Madrid'
		and top_events.num_events > 2
	) as listings
	where distance <=0.2 --km
	group by listings.id
	order by num_poi desc, num_events desc limit 10
) as top_list
inner join airbnb on top_list.id = airbnb.id
inner join top_events on top_list.id = top_events.airbnb_id
order by num_poi desc;

-- Exporta a csv la tabla de recomendaciones
INSERT OVERWRITE DIRECTORY 'gs://kc-airbnb/results/recommended_listings' ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
SELECT * FROM recommended_listings;



