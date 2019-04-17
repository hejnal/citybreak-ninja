-- CREATE OR REPLACE VIEW
--   `citybreak-ninja.routes.20190402_filtered_routes_view`
-- OPTIONS
--   ( expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR) ) 
--   AS
SELECT
  CONCAT(outbound.carrierFsCode,CAST(outbound.flightNumber AS STRING),' ',CAST(DATE(outbound.departureTime) AS STRING)) AS flight_id,
  outbound.departureAirportFsCode,
  outbound.departure_name,
  outbound.departure_city,
  outbound.departure_countryName,
  outbound.departure_latitude,
  outbound.departure_longitude,
  outbound.arrivalAirportFsCode,
  outbound.arrival_name,
  outbound.arrival_city,
  outbound.arrival_countryName,
  outbound.arrival_latitude,
  outbound.arrival_longitude,
  CONCAT(CAST(outbound.arrival_latitude AS STRING), ",", CAST(outbound.arrival_longitude AS STRING), " (", outbound.arrival_city,")") AS arrival_city_lat_lon,
  DATE(outbound.departureTime) AS departureDate,
  TIME(outbound.departureTime) AS departureTime,
  EXTRACT(HOUR
  FROM
    outbound.departureTime) AS departureHour,
  outbound.arrivalTime,
  outbound.flightNumber,
  outbound.carrierFsCode,
  outbound.carrier_name,
  CASE
    WHEN LOWER(outbound.carrier_name) IN ('airbaltic',  'ryanair',  'wizzair',  'wizz air',  'easyjet',  'blue air',  'eurowings',  'norwegian air shuttle',  'airbaltic',  'aer lingus',  'easyjet',  'flybe',  'jet2',  'tui airways',  'thomas cook airlines',  'laudamotion',  'smartwings',  'condor',  'tuifly',  'norwegian',  'helvetic',  'flybaboo',  'brussels airlines',  'jetairfly',  'pegasus',  'easyjet',  'air europa',  'vueling',  'air italy',  'wind-jet',  'blu-express',  'aegean airlines',  'corendon',  'onur air',  'atlasjet',  'anadolujet',  'wow air', 'wizz air uk', 'loganair') THEN 'Y'
    ELSE 'N'
  END AS is_lowcost,
  outbound.direction,
  outbound.minFare,
  CONCAT(inbound.carrierFsCode,CAST(inbound.flightNumber AS STRING),' ',CAST(DATE(inbound.departureTime) AS STRING)) AS inbound_flight_id,
  inbound.departureAirportFsCode AS inbound_departureAirportFsCode,
  inbound.departure_name AS inbound_departure_name,
  inbound.departure_city AS inbound_departure_city,
  inbound.departure_countryName AS inbound_departure_countryName,
  inbound.departure_latitude AS inbound_departure_latitude,
  inbound.departure_longitude AS inbound_departure_longitude,
  inbound.arrivalAirportFsCode AS inbound_arrivalAirportFsCode,
  inbound.arrival_name AS inbound_arrival_name,
  inbound.arrival_city AS inbound_arrival_city,
  inbound.arrival_countryName AS inbound_arrival_countryName,
  inbound.arrival_latitude AS inbound_arrival_latitude,
  inbound.arrival_longitude AS inbound_arrival_longitude,
  DATE(inbound.departureTime) AS inbound_departureDate,
  TIME(inbound.departureTime) AS inbound_departureTime,
  EXTRACT(HOUR
  FROM
    inbound.departureTime) AS inbound_departureHour,
  inbound.arrivalTime AS inbound_arrivalTime,
  inbound.flightNumber AS inbound_flightNumber,
  inbound.carrierFsCode AS inbound_carrierFsCode,
  inbound.carrier_name AS inbound_carrier_name,
  CASE
    WHEN LOWER(inbound.carrier_name) IN ('airbaltic',  'ryanair',  'wizzair',  'wizz air',  'easyjet',  'blue air',  'eurowings',  'norwegian air shuttle',  'airbaltic',  'aer lingus',  'easyjet',  'flybe',  'jet2',  'tui airways',  'thomas cook airlines',  'laudamotion',  'smartwings',  'condor',  'tuifly',  'norwegian',  'helvetic',  'flybaboo',  'brussels airlines',  'jetairfly',  'pegasus',  'easyjet',  'air europa',  'vueling',  'air italy',  'wind-jet',  'blu-express',  'aegean airlines',  'corendon',  'onur air',  'atlasjet',  'anadolujet',  'wow air', 'wizz air uk', 'loganair') THEN 'Y'
    ELSE 'N'
  END AS inbound_is_lowcost,
  inbound.direction AS inbound_direction,
  inbound.minFare AS inbound_minFare,
  IFNULL((SAFE_CAST(outbound.minFare AS INT64) + SAFE_CAST(inbound.minFare AS INT64)),
    0) AS min_total_fare,
  COUNT(*) OVER (PARTITION BY EXTRACT(DAY FROM outbound.departureTime),
    outbound.departureAirportFsCode,
    outbound.arrivalAirportFsCode) AS outbound_flights_per_day,
  COUNT(*) OVER (PARTITION BY EXTRACT(DAY FROM inbound.departureTime),
    inbound.departureAirportFsCode,
    inbound.arrivalAirportFsCode) AS inbound_flights_per_day,
  IF(COUNT(*) OVER (PARTITION BY EXTRACT(DAY FROM outbound.departureTime),
      outbound.departureAirportFsCode,
      outbound.arrivalAirportFsCode) = 1
    AND COUNT(*) OVER (PARTITION BY EXTRACT(DAY FROM inbound.departureTime),
      inbound.departureAirportFsCode,
      inbound.arrivalAirportFsCode) = 1,
    'Y',
    'N') AS is_true_total_price
FROM
  `citybreak-ninja.routes.20190416_raw_routes` AS outbound
INNER JOIN
  `citybreak-ninja.routes.20190416_raw_routes` AS inbound
ON
  outbound.arrivalAirportFsCode = inbound.departureAirportFsCode
  AND outbound.direction = 'outbound'
WHERE
  EXTRACT(HOUR
  FROM
    outbound.departureTime) BETWEEN 17
  AND 21
  AND EXTRACT(HOUR
  FROM
    inbound.departureTime) BETWEEN 16
  AND 22
  --   AND outbound.carrier_name IN ('Ryanair',
    --     'Wizzair')
  --   AND inbound.carrier_name IN ('Ryanair',
    --     'Wizzair')
ORDER BY
  min_total_fare ASC;