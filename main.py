import os, json, requests, datetime, logging

logging.basicConfig(format='%(asctime)s %(message)s')
logging.info('Welcome to Citybreak Ninja.')

# global configuration for Flightstats API
CONFIG = {
  'output_filename': 'output.json'
}

FLIGHTSTATS_CONFIG = {
  'host': 'https://api.flightstats.com/flex/schedules/rest',
  'routes_from_path': '/v1/json/from/{departureAirportCode}/departing/{year}/{month}/{day}/{hourOfDay}',
  'fixed_route_path': '/v1/json/from/{departureAirportCode}/to/{arrivalAirportCode}/departing/{year}/{month}/{day}',
  'app_id': os.environ.get('FLIGHTSTATS_APP_ID', None),
  'api_key': os.environ.get('FLIGHTSTATS_API_KEY', None),
}

# global configuration for Skyscanner API
# RAPID_API_CONFIG = {
#   'host': '',
#   'path': ''
#   'api_key': "5b3b186124msh916ad178cad5f1ep112223jsnfac9d39946ff"
# }

input_dates = [ {'outbound': '2019-06-14', 'inbound': '2019-06-16'}]
input_hours = { 'outbound': [17, 18, 19, 20, 21, 22, 23]}

input_home_airports = ['STN', 'LTN', 'LGW', 'LCY', 'LHR', 'SEN']

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
        print('{} [STARTED] Fetching data for {outbound_date} date at {hour}h from {airport}.'
          .format(start_ts, outbound_date=outbound_date, hour=hour, airport=airport))
        routes = fetch_outbound_scheduled_flights(outbound_date, hour, airport)
        append_results_file(CONFIG['output_filename'], routes)
        finish_ts = datetime.datetime.now()
        print('{} [FINISHED] Process finshed. It has taken: {}s.'.format(finish_ts, (finish_ts - start_ts).seconds))
        destination_airports = update_destination_set(routes, destination_airports)

    print('{} Step 2, find all return flights from following destinations: {}'.format(datetime.datetime.now(), destination_airports))
    for fromAirport in destination_airports:
      for toAirport in input_home_airports:
        start_ts = datetime.datetime.now()
        print('{} [STARTED] Fetching data for {inbound_date} date from {fromAirport} to {toAirport} home airport.'
          .format(start_ts, inbound_date=inbound_date, fromAirport=fromAirport, toAirport=toAirport))
        routes = fetch_inbound_scheduled_flights(inbound_date, fromAirport, toAirport)
        append_results_file(CONFIG['output_filename'], routes)
        finish_ts = datetime.datetime.now()
        print('{} [FINISHED] Process finshed. It has taken: {}s.'.format(finish_ts, (finish_ts - start_ts).seconds))
        destination_airports = update_destination_set(routes, destination_airports)
  
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
  response = requests.get(request_url, params=request_params)
  return response.json()['scheduledFlights']

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
  response = requests.get(request_url, params=request_params)
  return response.json()['scheduledFlights']

def append_results_file(filename, data):
  with open(filename, 'a+') as outfile:
    for o in data:
      # add timestamp, to results
      o['fetchedAt'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
      json.dump(o, outfile)
      outfile.write('\n')


def update_destination_set(routes, destination_set):
  for route in routes:
    destination_set = destination_set | set({route['arrivalAirportFsCode']}) # save a destination airport in a set
  return destination_set

if __name__ == "__main__":
  main()