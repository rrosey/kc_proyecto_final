---------------------------------------------------------------------------
-- LOAD AIRBNB DATA
--   *Elimina la filas vacias despues de la carga.
---------------------------------------------------------------------------
DROP TABLE IF EXISTS airbnb;
CREATE TABLE airbnb (
id int,
listing_url string,
scrape_id string,
last_scraped string,
name string,
summary string,
space string,
description string,
experiences_offered string,
neighborhood_overview string,
notes string,
transit string,
access string,
interaction string,
house_rules string,
thumbnail_url string,
medium_url string,
picture_url string,
xl_picture_url string,
host_id string,
host_url string,
host_name string,
host_since string,
host_location string,
host_about string,
host_response_time string,
host_response_rate string,
host_acceptance_rate string,
host_thumbnail_url string,
host_picture_url string,
host_neighbourhood string,
host_listings_count string,
host_total_listings_count string,
host_verifications string,
street string,
neighbourhood string,
neighbourhood_cleansed string,
neighbourhood_group_cleansed string,
city string,
state string,
zipcode string,
market string,
smart_location string,
country_code string,
country string,
latitude double,
longitude double,
property_type string,
room_type string,
accommodates string,
bathrooms string,
bedrooms string,
beds string,
bed_type string,
amenities string,
square_feet string,
price string,
weekly_price string,
monthly_price string,
security_deposit string,
cleaning_fee string,
guests_included string,
extra_people string,
minimum_nights string,
maximum_nights string,
calendar_updated string,
has_availability string,
availability_30 string,
availability_60 string,
availability_90 string,
availability_365 string,
calendar_last_scraped string,
number_of_reviews string,
first_review string,
last_review string,
review_scores_rating string,
review_scores_accuracy string,
review_scores_cleanliness string,
review_scores_checkin string,
review_scores_communication string,
review_scores_location string,
review_scores_value string,
license string,
jurisdiction_names string,
cancellation_policy string,
calculated_host_listings_count string,
reviews_per_month string,
geolocation string,
features string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ';',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 'gs://kc-airbnb/datasets/airbnb-listings-lite_preproc.csv' INTO TABLE airbnb;

INSERT OVERWRITE TABLE airbnb
select * from airbnb where id is not null;
---------------------------------------------------------------------------
-- LOAD POI (Foursquare) DATA
---------------------------------------------------------------------------
DROP TABLE IF EXISTS poi_data;
CREATE TABLE poi_data
(
	neighbourhood string,
	neighbourhood_lat double,
	neighbourhood_lon double,
	name string,
	poi_lat double,
	poi_lon double,
	poi_category string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ';',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 'gs://kc-airbnb/datasets/poi.csv' INTO TABLE poi_data;

---------------------------------------------------------------------------
-- LOAD EVENTS (eventos Madrid) DATA
---------------------------------------------------------------------------
DROP TABLE IF EXISTS events_calendar;
CREATE TABLE events_calendar
(
	id_evento int,
	titulo string,
	precio string,
	gratuito string,
	larga_duracion string,
	dias_semana string,
	dias_excluidos string,
	fecha string,
	fecha_fin string,
	hora string,
	descripcion string,
	content_url string,
	titulo_actividad string,
	url_actividad string,
	url_instalacion string,
	nombre_instalacion string,
	accesibilidad_instalacion string,
	clase_vial_instalacion string,
	nombre_via_instalacion string,
	num_instalacion int,
	distrito_instalacion string,
	barrio_instalacion string,
	codigo_postal_instalacion int,
	coordenada_x int,
	coordenada_y int,
	latitud double,
	longitud double,
	tipo string,
	audiencia string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ';',
   'quoteChar' = '"',
   'escapeChar' = '\\'
   )
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");
LOAD DATA INPATH 'gs://kc-airbnb/datasets/events.csv' INTO TABLE events_calendar;