# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import traceback
import time


class Message:
  # from specification, each type's fixed string length
  spec_len = {'X': 24, 'A': 42, 'E': 33}

  def __init__(self, line, line_no):
    '''
    if message type is not in ['A', 'X', 'E']
    return Null dict {}

    if exception raised, return contains key 'error' with 'line' and 'line_no'
    '''
    line = line.strip()
    self.data = {}
    self.error = {}
    self.type = None
    self.buysell = None

    try:
      if line.strip():  # in case empty lines
        message_type = line[8]
        self.type = message_type
        if message_type in ['A', 'X', 'E']:
          # validate input string
          if len(line) != self.spec_len[message_type]:
            raise Exception

          # parse message
          if message_type == 'A':
            self.data = self.create_Add_message(line)
          if message_type == 'E':
            self.data = self.create_Execute_message(line)
          if message_type == 'X':
            self.data = self.create_Xcancel_message(line)

        # strip whitespaces
        for k in self.data:
          if isinstance(self.data[k], str):
            self.data[k] = self.data[k].strip()

    except Exception as e:
      self.create_error_message(line, line_no, e)

  def create_Add_message(self, line):
    self.buysell = line[18]
    return {
        'time': line[0:8],
        'message_type': line[8],
        'ref_no': line[9:18],
        'BS': line[18],
        'volume': int(line[19:25]),
        'stock': line[25:31],
        'price': int(line[31:41]) / 10000,
        'display': line[41]
    }

  def create_Execute_message(self, line):
    return {
        'time': line[0:8],
        'message_type': line[8],
        'ref_no': line[9:18],
        'volume': int(line[18:24]),
        'match_no': line[24:]
    }

  def create_Xcancel_message(self, line):
    return {
        'time': line[0:8],
        'message_type': line[8],
        'ref_no': line[9:18],
        'volume': int(line[18:24]),
    }

  def create_error_message(self, line, line_no, e):
    self.error = {
        'error': True,
        'line': line,
        'line_no': line_no,
        'error_message': str(e),
        'traceback': traceback.print_exc()
    }


class BookList:

  def __init__(self, n_levels):
    # event_df = [
    #     'message_type', 'AEX_ref_no', 'A_stock',
    #     'A_price', 'A_BS', 'A_display', 'AEX_volume', 'E_match_no'
    # ]
    self.n_levels = n_levels
    self.bid_price_volume_dict = dict()
    self.ask_price_volume_dict = dict()
    self.book_dict_list = []

  def update(self, message, ref_message):
    self.update_market_book(message, ref_message)

    ask_price_dict, ask_volume_dict = self.get_n_level_book(
        self.ask_price_volume_dict, 'ask')
    bid_price_dict, bid_volume_dict = self.get_n_level_book(
        self.bid_price_volume_dict, 'bid')

    ref_dict = dict()
    if ref_message:
      ref_dict = {'ref_%s' % k: ref_message.data[k] for k in ref_message.data}
    event_dict = {'event_%s' % k: message.data[k] for k in message.data}

    all_dict = dict()
    for d in [
        ask_price_dict, ask_volume_dict, bid_price_dict, bid_volume_dict,
        ref_dict, event_dict
    ]:
      all_dict.update(d)

    self.book_dict_list.append(all_dict)

  def get_n_level_book(self, price_volume_dict, askbid):
    reverse = False
    if askbid == 'bid':
      reverse = True
    sorted_key = sorted(price_volume_dict.keys(), reverse=reverse)
    sorted_key = sorted_key[:self.n_levels]
    price_dict = dict()
    volume_dict = dict()

    # fill n_levels
    for i, k in enumerate(sorted_key):
      i = i + 1
      price_dict['%s_price_%d' % (askbid, i)] = k
      volume_dict['%s_volume_%d' % (askbid, i)] = price_volume_dict[k]

    # fill null levels
    for i in range(len(sorted_key) + 1, self.n_levels + 1):
      price_dict['%s_price_%d' % (askbid, i)] = 0.0
      volume_dict['%s_volume_%d' % (askbid, i)] = 0

    return price_dict, volume_dict

  def update_market_book(self, message, ref_message):
    """Update Book using incoming Message data."""

    if message.type == 'A':
      if message.buysell == 'B':
        book_dict = self.bid_price_volume_dict
      else:
        book_dict = self.ask_price_volume_dict

      if message.data['price'] in book_dict.keys():
        book_dict[message.data['price']] += message.data['volume']
      else:
        book_dict[message.data['price']] = message.data['volume']

    if message.type in ['E', 'X']:
      if ref_message.buysell == 'B':
        book_dict = self.bid_price_volume_dict
      else:
        book_dict = self.ask_price_volume_dict

      book_dict[ref_message.data['price']] -= message.data['volume']
      if book_dict[ref_message.data['price']] == 0:
        book_dict.pop(ref_message.data['price'])

  def to_hdf5(self, filepath, stock, date):
    df = pd.DataFrame(self.book_dict_list)
    df.to_hdf(filepath, key='%s/%s' % (stock, date), mode='a')


def look_up_message(ref_no, message_dict):
  ref_message = message_dict.get(ref_no)
  return ref_message


def read_stock_date_hdf5(filepath, stock, date, n_levels):
  ask_price_keys = ['ask_price_%d' % i for i in range(1, n_levels + 1)]
  ask_volume_keys = ['ask_volume_%d' % i for i in range(1, n_levels + 1)]
  bid_price_keys = ['bid_price_%d' % i for i in range(1, n_levels + 1)]
  bid_volume_keys = ['bid_volume_%d' % i for i in range(1, n_levels + 1)]
  event_keys = [
      'event_BS',
      'event_display',
      'event_message_type',
      'event_price',
      'event_ref_no',
      'event_stock',
      'event_time',
      'event_volume',
  ]
  ref_keys = [
      'ref_BS', 'ref_display', 'ref_message_type', 'ref_price', 'ref_ref_no',
      'ref_stock', 'ref_time', 'ref_volume'
  ]
  df = pd.read_hdf(filepath, '%s/%s' % (stock, date))
  ask_price_df, ask_volume_df, bid_price_df, bid_volume_df, event_df, ref_df = df[
      ask_price_keys], df[ask_volume_keys], df[bid_price_keys], df[
          bid_volume_keys], df[event_keys], df[ref_keys]
  return ask_price_df, ask_volume_df, bid_price_df, bid_volume_df, event_df, ref_df


def parse_v2(stock_list, date_list, n_levels, h5_filepath, data_path):
  stock_booklist = {k: BookList(n_levels) for k in stock_list}

  log_interval = 10000
  last_time = time.time()
  for date in date_list:
    filename = data_path + 'S%s-v2.txt' % date

    with open(filename, 'r') as f:
      message_dict = dict()
      for line_no, line in enumerate(f):
        message = Message(line, line_no)
        if message.error:
          print(message.error)
          break
        else:
          if message.type == 'A' and message.data['stock'] in stock_list:
            message_dict[message.data['ref_no']] = message

          # reverse find ref message
          stock = message.data.get('stock')
          ref_message = None
          if message.type in ['E', 'X']:
            ref_message = look_up_message(message.data['ref_no'], message_dict)

            # if no ref message, either data error or not in stock_list
            if not ref_message:
              continue

            stock = ref_message.data['stock']

          if stock in stock_list:
            booklist = stock_booklist[stock]
            booklist.update(message, ref_message)

          if not line_no % log_interval:
            print('time elapsed: %f minutes' % ((time.time() - last_time) / 60))
            print(line_no)
            last_time = time.time()
    # save hdf5 file
    for k in stock_list:
      stock_booklist[k].to_hdf5(h5_filepath, k, date)

  return stock_booklist


if __name__ == "__main__":

  date_list = ['122607']
  stock_list = ['QQQQ', 'AAPL']
  n_levels = 10
  h5_filepath = './v2_%d_levels.h5' % n_levels
  data_path = './'

  stock_booklist = parse_v2(stock_list, date_list, n_levels, h5_filepath,
                            data_path)

  # # read hdf5 file example
  # date = date_list[0]
  # stock = stock_list[0]
  # ask_price_df, ask_volume_df, bid_price_df, bid_volume_df, event_df, ref_df = read_stock_date_hdf5(
  #     h5_filepath, stock, date, n_levels)
  # 4:47
