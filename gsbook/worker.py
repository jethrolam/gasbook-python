import os
import pprint
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SCOPE = ['https://spreadsheets.google.com/feeds']
OAUTH = 'gsbook/gsbook-oauth.json'


def numberToLetters(q):
    """ Turn numbers into A1 column names
        See: https://www.dataiku.com/learn/guide/code/python/export-a-dataset-to-google-spreadsheets.html
    """
    q = q - 1
    result = ''
    while q >= 0:
        remain = q % 26
        result = chr(remain+65) + result;
        q = q//26 - 1
    return result


def _get(gc, sheet_title, tab_title):
    """ Query a Google Spreadsheet and return result as dataframe """
    tab = gc.open(sheet_title).worksheet(tab_title)

    data = tab.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0])


def _put(gc, sheet_title, tab_title, df):
    """ Send a dataframe to Google Spreadsheet 
        See: https://www.dataiku.com/learn/guide/code/python/export-a-dataset-to-google-spreadsheets.html
    """
    tab = gc.open(sheet_title).worksheet(tab_title)

    # Write header
    columns = df.reset_index().columns.values.tolist()
    cell_list = tab.range('A1:'+numberToLetters(len(columns))+'1')
    for cell in cell_list:
        val = columns[cell.col-1]
        if type(val) is str:
            val = val.decode('utf-8')
        cell.value = val
    tab.update_cells(cell_list)

    # Write body
    num_lines, num_columns = df.reset_index().shape
    cell_list = tab.range('A2:'+numberToLetters(num_columns)+str(num_lines+1))
    for cell in cell_list:
        val = df.reset_index().iloc[cell.row-2,cell.col-1]
        if type(val) is str:
            val = val.decode('utf-8')
        elif isinstance(val, (int, long, float, complex)):
            val = int(round(val))
        cell.value = val
    tab.update_cells(cell_list)


def _transform(event_df):
    """ Transform event_df into various forms """ 
    summary_df = event_df.groupby(['Name','Standard']).max()['Score'].unstack()
    return summary_df


def update(sheet_title):
    """ Triggered by user, this method takes event data from Google Spreadsheet,
        tranform it into various forms, and publish them back to Googl
    """        
    credentials = ServiceAccountCredentials.from_json_keyfile_name(OAUTH, SCOPE)
    gc = gspread.authorize(credentials)

    print('Reading data from {}:{}...'.format(sheet_title, 'Event'))
    event_df = _get(gc, sheet_title, 'Event')

    print('Transforming data...')
    summary_df = _transform(event_df)

    print('Writing data into {}:{}...'.format(sheet_title, 'Summary'))
    _put(gc, sheet_title, 'Summary', summary_df)


if __name__ == '__main__':
    update('Math000_SBG')

