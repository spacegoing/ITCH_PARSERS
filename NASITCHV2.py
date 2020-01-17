# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import traceback


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
    self.message_list = []
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
    self.message_list.append(message)

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


def look_up_message(ref_no, message_list):
  for m in reversed(message_list):
    if m.type == 'A':
      if m.data['ref_no'] == ref_no:
        return m

  return Exception('ref_no %s not found' % ref_no)


filename = 'S122607-v2.txt'

n_levels = 10
stock_list = ['AAPL']
stock_booklist = {k: BookList(n_levels) for k in stock_list}
message_list = []

with open(filename, 'r') as f:
  message_list = []
  for line_no, line in enumerate(f):
    message = Message(line, line_no)
    if message.error:
      print(message.error)
      break
    else:
      message_list.append(message)

      # reverse find ref message
      stock = message.data.get('stock')
      ref_message = None
      if message.type in ['E', 'X']:
        ref_message = look_up_message(message.data['ref_no'], message_list)
        stock = ref_message.data['stock']

      if stock in stock_list:
        booklist = stock_booklist[stock]
        booklist.update(message, ref_message)
        if ref_message:
          import ipdb; ipdb.set_trace(context=7)

