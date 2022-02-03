import io, os, sys, re, shutil
import json
from pathlib import Path
from datetime import *
import urllib.request
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
from enums import *
from google.cloud import storage

def get_destination_dir(file_url, folder=None):
  store_directory = os.environ.get('STORE_DIRECTORY')
  if folder:
    store_directory = folder
  if not store_directory:
    store_directory = os.path.dirname(os.path.realpath(__file__))
  return os.path.join(store_directory, file_url)

def get_download_url(file_url):
  return "{}{}".format(BASE_URL, file_url)

def get_all_symbols(type):
  if type == 'um':
    response = urllib.request.urlopen("https://fapi.binance.com/fapi/v1/exchangeInfo").read()
  elif type == 'cm':
    response = urllib.request.urlopen("https://dapi.binance.com/dapi/v1/exchangeInfo").read()
  else:
    response = urllib.request.urlopen("https://api.binance.com/api/v3/exchangeInfo").read()
  return list(map(lambda symbol: symbol['symbol'], json.loads(response)['symbols']))

def gs_obj_exists(bucket_name, filename):
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(filename)
  return blob.exists()

def upload_to_gs(bucket_name, filename, buffer):
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(filename)
  print()
  print(f"upload to gs://{bucket_name}/{filename}")
  sys.stdout.flush()
  blob.upload_from_file(io.BytesIO(bytes(buffer)))

def download_file(base_path, file_name, date_range=None, folder=None, gs_bucket=None):
  download_path = "{}{}".format(base_path, file_name)
  local_save_path = get_destination_dir(os.path.join(base_path, file_name), folder)
  gs_save_path = os.path.join(base_path.replace("data/","binance/"), file_name)
  if folder:
    base_path = os.path.join(folder, base_path)
    if os.path.exists(local_save_path):
      print("\nfile already exists! {}".format(local_save_path))
      local_save_path = None
    # make the directory
    if not os.path.exists(base_path):
      Path(get_destination_dir(base_path)).mkdir(parents=True, exist_ok=True)
  else:
    local_save_path = None

  if gs_bucket:
    if gs_obj_exists(gs_bucket, gs_save_path):
      print(f"\nfile already exists! gs://{gs_bucket}/{gs_save_path}")
      gs_save_path = None
  else:
    gs_save_path = None

  if local_save_path == None and gs_save_path == None:
    return

  buffer = bytearray()
  # download
  try:
    download_url = get_download_url(download_path)
    dl_file = urllib.request.urlopen(download_url)
    length = dl_file.getheader('content-length')
    if length:
      length = int(length)
      blocksize = max(4096,length//100)

    dl_progress = 0
    print("\nFile Download: {}".format(download_url))
    while True:
      buf = dl_file.read(blocksize)   
      if not buf:
        break
      dl_progress += len(buf)
      buffer.extend(buf)
      done = int(50 * dl_progress / length)
      sys.stdout.write("\r[%s%s]" % ('#' * done, '.' * (50-done)) )    
      sys.stdout.flush()
  except urllib.error.HTTPError:
    print("\nFile not found: {}".format(download_url))
    return

  # write to local file
  if folder and local_save_path:
    with open(local_save_path, 'wb') as out_file:
      out_file.write(buffer)
  
  # write to goolge storage
  if gs_bucket and gs_save_path:
    upload_to_gs(gs_bucket, gs_save_path, buffer)

def convert_to_date_object(d):
  year, month, day = [int(x) for x in d.split('-')]
  date_obj = date(year, month, day)
  return date_obj

def get_start_end_date_objects(date_range):
  start, end = date_range.split()
  start_date = convert_to_date_object(start)
  end_date = convert_to_date_object(end)
  return start_date, end_date

def match_date_regex(arg_value, pat=re.compile(r'\d{4}-\d{2}-\d{2}')):
  if not pat.match(arg_value):
    raise ArgumentTypeError
  return arg_value

def get_path(trading_type, market_data_type, time_period, symbol, interval=None):
  trading_type_path = 'data/spot'
  if trading_type != 'spot':
    trading_type_path = f'data/futures/{trading_type}'
  if interval is not None:
    path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/'
  else:
    path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/'
  return path

def get_parser(parser_type):
  parser = ArgumentParser(description=("This is a script to download historical {} data").format(parser_type), formatter_class=RawTextHelpFormatter)
  parser.add_argument(
      '-s', dest='symbols', nargs='+',
      help='Single symbol or multiple symbols separated by space')
  parser.add_argument(
      '-y', dest='years', default=YEARS, nargs='+', choices=YEARS,
      help='Single year or multiple years separated by space\n-y 2019 2021 means to download {} from 2019 and 2021'.format(parser_type))
  parser.add_argument(
      '-m', dest='months', default=MONTHS,  nargs='+', type=int, choices=MONTHS,
      help='Single month or multiple months separated by space\n-m 2 12 means to download {} from feb and dec'.format(parser_type))
  parser.add_argument(
      '-d', dest='dates', nargs='+', type=match_date_regex,
      help='Date to download in [YYYY-MM-DD] format\nsingle date or multiple dates separated by space\ndownload past 35 days if no argument is parsed')
  parser.add_argument(
      '-startDate', dest='startDate', type=match_date_regex,
      help='Starting date to download in [YYYY-MM-DD] format')
  parser.add_argument(
      '-endDate', dest='endDate', type=match_date_regex,
      help='Ending date to download in [YYYY-MM-DD] format')
  parser.add_argument(
      '-folder', dest='folder',
      help='Directory to store the downloaded data')
  parser.add_argument(
      '-gs_bucket', dest='gs_bucket',
      help='Google Storage bucket to store the downloaded data')
  parser.add_argument(
      '-c', dest='checksum', default=0, type=int, choices=[0,1],
      help='1 to download checksum file, default 0')
  parser.add_argument(
      '-t', dest='type', default='spot', choices=TRADING_TYPE,
      help='Valid trading types: {}'.format(TRADING_TYPE))

  if parser_type == 'klines':
    parser.add_argument(
      '-i', dest='intervals', default=INTERVALS, nargs='+', choices=INTERVALS,
      help='single kline interval or multiple intervals separated by space\n-i 1m 1w means to download klines interval of 1minute and 1week')


  return parser


