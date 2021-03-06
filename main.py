import os, sys, json, requests, datetime, logging

logging.basicConfig(format='%(asctime)s %(message)s')
logging.info('Welcome to Citybreak Ninja.')

# global configuration for Flightstats API
CONFIG = {
  'output_filename': 'output_full.json'
}

FLIGHTSTATS_CONFIG = {
  'host': 'https://api.flightstats.com/flex/schedules/rest',
  'routes_from_path': '/v1/json/from/{departureAirportCode}/departing/{year}/{month}/{day}/{hourOfDay}',
  'fixed_route_path': '/v1/json/from/{departureAirportCode}/to/{arrivalAirportCode}/departing/{year}/{month}/{day}',
  'app_id': os.environ.get('FLIGHTSTATS_APP_ID', None),
  'api_key': os.environ.get('FLIGHTSTATS_API_KEY', None),
}

# global configuration for Skyscanner API
SKYSCANNER_API_CONFIG = {
  'host': 'https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com',
  'path': '/apiservices/browsequotes/v1.0/{country}/{currency}/{locale}/{departureAirportCode}-sky/{arrivalAirportCode}-sky/{outboundDate}',
  'api_key': os.environ.get('SKYSCANNER_API_KEY', None),
}

# get arguments
if len(sys.argv) == 3:
  outbound = sys.argv[1]
  inbound = sys.argv[2]
else:
  print('Please provide two dates: outbound and inbound, ex: python main.py 2019-06-14 2019-06-16')

input_dates = [ {'outbound': outbound, 'inbound': inbound}]
input_hours = { 'outbound': [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]}

input_home_airports = ['STN', 'LTN', 'LGW', 'LCY', 'LHR', 'SEN']

airport_dict = {}
airline_dict = {}

def main():
  # main loop

  # save an unique destination in a set
  destination_airports = set()

  total_start_ts = datetime.datetime.now()
  print('{} [STARTED] Citybreak - here we go!'.format(total_start_ts))

  for dates in input_dates:
    outbound_date = dates['outbound']
    inbound_date = dates['inbound']

    print('{} Step 1: get all outbound connections.'.format(datetime.datetime.now()))
    for airport in input_home_airports:
      for hour in input_hours['outbound']:
        start_ts = datetime.datetime.now()
        print('{} [STARTED] Flights data fetch for {outbound_date} date at {hour}h from {airport}.'
          .format(start_ts, outbound_date=outbound_date, hour=hour, airport=airport))
        routes = fetch_outbound_scheduled_flights(outbound_date, hour, airport)
        finish_ts = datetime.datetime.now()
        print('{} [FINISHED] Flights data fetch process finshed. It has taken: {}s.'.format(finish_ts, (finish_ts - start_ts).seconds))
        # if no routes, then continue
        if len(routes) == 0:
          continue

        # update the fare for all routes
        start_ts = datetime.datetime.now()
        print('{} [STARTED] Fares data fetch for {outbound_date} date at {hour}h from {airport}.'
          .format(start_ts, outbound_date=outbound_date, hour=hour, airport=airport))
        routes = update_flight_fare(routes)
        finish_ts = datetime.datetime.now()
        print('{} [FINISHED] Fares data fetch process finshed. It has taken: {}s.'.format(finish_ts, (finish_ts - start_ts).seconds))
        
        # update the directon of the routes
        routes = update_direction(routes, 'outbound')
        # add info about airports (departures, and arrival)
        routes = update_airport_details(routes)
        # add info about carrier
        routes = update_carrier_details(routes)
        append_results_file(CONFIG['output_filename'], routes)
        destination_airports = update_destination_set(routes, destination_airports)

    print('{} Step 2, find all return flights from following destinations: {}'.format(datetime.datetime.now(), destination_airports))
    for fromAirport in destination_airports:
      for toAirport in input_home_airports:
        start_ts = datetime.datetime.now()
        print('{} [STARTED] Flights data fetch for {inbound_date} date from {fromAirport} to {toAirport} home airport.'
          .format(start_ts, inbound_date=inbound_date, fromAirport=fromAirport, toAirport=toAirport))
        routes = fetch_inbound_scheduled_flights(inbound_date, fromAirport, toAirport)
        finish_ts = datetime.datetime.now()
        print('{} [FINISHED] Flights data fetch process finshed. It has taken: {}s.'.format(finish_ts, (finish_ts - start_ts).seconds))
        # if no routes, then continue
        if len(routes) == 0:
          continue

        # update the fare for all routes
        start_ts = datetime.datetime.now()
        print('{} [STARTED] Fares data fetch for {inbound_date} date from {fromAirport} to {toAirport} home airport.'
          .format(start_ts, inbound_date=inbound_date, fromAirport=fromAirport, toAirport=toAirport))
        routes = update_flight_fare(routes)
        finish_ts = datetime.datetime.now()
        print('{} [FINISHED] Fares data fetch process finshed. It has taken: {}s.'.format(finish_ts, (finish_ts - start_ts).seconds))
        
        # update the directon of the routes
        routes = update_direction(routes, 'inbound')
        # add info about airports (departures, and arrival)
        routes = update_airport_details(routes)
        # add info about carrier
        routes = update_carrier_details(routes)
        append_results_file(CONFIG['output_filename'], routes)
        
  
  total_finish_ts = datetime.datetime.now()
  print('{} [FINISHED] Citybreak process finshed. It has taken: {}s.'.format(total_finish_ts, (total_finish_ts - total_start_ts).seconds))

def fetch_outbound_scheduled_flights(date, hourOfDay, fromAirport):
  parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d')
  # arguments for the specific request
  request_args = {}
  request_args['departureAirportCode'] = fromAirport
  request_args['year'] = parsed_date.year
  request_args['month'] = parsed_date.month
  request_args['day'] = parsed_date.day
  request_args['hourOfDay'] = hourOfDay  
  # request parameters (authentication)
  request_params = {}
  request_params['appId'] = FLIGHTSTATS_CONFIG['app_id']
  request_params['appKey'] = FLIGHTSTATS_CONFIG['api_key']
  # create a request url (using the dictionary)
  request_url = FLIGHTSTATS_CONFIG['host'] + FLIGHTSTATS_CONFIG['routes_from_path'].format(**request_args)
  try:
    response = requests.get(request_url, params=request_params)
    # update airlines and airport dictionary
    if 'appendix' in response.json() and 'airports' in response.json()['appendix']:
      update_airport_dictionary(response.json()['appendix']['airports'])
    if 'appendix' in response.json() and 'airlines' in response.json()['appendix']:
      update_airline_dictionary(response.json()['appendix']['airlines'])
    return response.json()['scheduledFlights']
  except:
    print('{} [ERROR] Problem calling an API on url {request_url}.'
      .format(datetime.datetime.now(), request_url=request_url))
  return []

def fetch_min_fare(country, currency, locale, fromAirport, toAirport, date):
  # arguments for the specific request
  request_args = {}
  request_args['departureAirportCode'] = fromAirport
  request_args['arrivalAirportCode'] = toAirport
  request_args['country'] = country
  request_args['currency'] = currency
  request_args['locale'] = locale
  request_args['outboundDate'] = date  
  # request parameters (authentication)
  request_headers = {}
  request_headers['X-RapidAPI-Key'] = SKYSCANNER_API_CONFIG['api_key']
  # create a request url (using the dictionary)
  request_url = SKYSCANNER_API_CONFIG['host'] + SKYSCANNER_API_CONFIG['path'].format(**request_args)
  try:
    response = requests.get(request_url, headers=request_headers)
    if 'Quotes' in response.json() and len(response.json()['Quotes']) > 0:
      return response.json()['Quotes'][0]['MinPrice']
    else:
      return 'NaN'
  except:
    print('{} [ERROR] Problem calling an API on url {request_url}.'
      .format(datetime.datetime.now(), request_url=request_url))
  return 'NaN'

def fetch_inbound_scheduled_flights(date, fromAirport, toAirport):
  parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d')
  # arguments for the specific request
  request_args = {}
  request_args['departureAirportCode'] = fromAirport
  request_args['arrivalAirportCode'] = toAirport
  request_args['year'] = parsed_date.year
  request_args['month'] = parsed_date.month
  request_args['day'] = parsed_date.day
   
  # request parameters (authentication)
  request_params = {}
  request_params['appId'] = FLIGHTSTATS_CONFIG['app_id']
  request_params['appKey'] = FLIGHTSTATS_CONFIG['api_key']
  # create a request url (using the dictionary)
  request_url = FLIGHTSTATS_CONFIG['host'] + FLIGHTSTATS_CONFIG['fixed_route_path'].format(**request_args)
  try:
    response = requests.get(request_url, params=request_params)
    # update airlines and airport dictionary
    if 'appendix' in response.json() and 'airports' in response.json()['appendix']:
      update_airport_dictionary(response.json()['appendix']['airports'])
    if 'appendix' in response.json() and 'airlines' in response.json()['appendix']:
      update_airline_dictionary(response.json()['appendix']['airlines'])
    return response.json()['scheduledFlights']
  except:
    print('{} [ERROR] Problem calling an API on url {request_url}.'
      .format(datetime.datetime.now(), request_url=request_url))
  return []

def append_results_file(filename, data):
  with open(filename, 'a+') as outfile:
    for o in data:
      # add timestamp, to results
      o['fetchedAt'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      json.dump(o, outfile)
      outfile.write('\n')

def update_flight_fare(routes):
  for route in routes:
    parsed_date = datetime.datetime.strptime(route['departureTime'], '%Y-%m-%dT%H:%M:%S.%f')
    date = parsed_date.strftime('%Y-%m-%d')
    fare = fetch_min_fare('UK', 'GBP', 'en-UK', route['departureAirportFsCode'], route['arrivalAirportFsCode'], date)
    route['minFare'] = fare
  return routes

def update_direction(routes, direction):
  for route in routes:
    route['direction'] = direction
  return routes

def update_airport_details(routes):
  for idx in range(len(routes)):
    departure_airport_details = airport_dict[routes[idx]['departureAirportFsCode']]
    departure_prefixed_airport = {f'departure_{k}': v for k, v in departure_airport_details.items()}
    arrival_airport_details = airport_dict[routes[idx]['arrivalAirportFsCode']]
    arrival_prefixed_airport = {f'arrival_{k}': v for k, v in arrival_airport_details.items()}
    routes[idx] = {**routes[idx], **departure_prefixed_airport, **arrival_prefixed_airport}
  return routes

def update_carrier_details(routes):
  for idx in range(len(routes)):
    carrier_details = airline_dict[routes[idx]['carrierFsCode']]
    prefixed_carrier = {f'carrier_{k}': v for k, v in carrier_details.items()}
    routes[idx] = {**routes[idx], **prefixed_carrier}
  return routes
  
def update_airport_dictionary(airports):
  for airport in airports:
    if airport['fs'] not in airport_dict:
      airport_dict[airport['fs']] = airport

def update_airline_dictionary(airlines):
  for airline in airlines:
    if airline['fs'] not in airline_dict:
      airline_dict[airline['fs']] = airline

def update_destination_set(routes, destination_set):
  for route in routes:
    destination_set = destination_set | set({route['arrivalAirportFsCode']}) # save a destination airport in a set
  return destination_set

if __name__ == "__main__":
  main()