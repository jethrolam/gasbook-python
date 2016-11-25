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


def _get(sheet, tab_title):
    """ Query a Google Spreadsheet and return result as dataframe """
    tab = sheet.worksheet(tab_title)

    data = tab.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0])


def _put(sheet, tab_title, df):
    """ Send a dataframe to Google Spreadsheet 
        See: https://www.dataiku.com/learn/guide/code/python/export-a-dataset-to-google-spreadsheets.html
    """
    try:
        tab = sheet.worksheet(tab_title)
    except gspread.exceptions.WorksheetNotFound:
        pass
    else:
        sheet.del_worksheet(tab)
    finally:
        columns = df.reset_index().columns.values.tolist()
        num_lines, num_columns = df.reset_index().shape
        tab = sheet.add_worksheet(title=tab_title, rows=num_lines+4, cols=num_columns+4)

    # Write header
    cell_list = tab.range('A1:'+numberToLetters(len(columns))+'1')
    for cell in cell_list:
        val = columns[cell.col-1]
        if type(val) is str:
            val = val.decode('utf-8')
        cell.value = val
    tab.update_cells(cell_list)

    # Write body
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
    """ Transform event_df into a map of {tab_title: df}  """ 
    summary_df = event_df.groupby(['Name','Standard']).max()['Score'].unstack()
    standard_df = event_df.groupby(['Standard', 'Version']).count()['Date'].unstack().fillna(0)
    individual_df = event_df.groupby(['Name','Standard','Version','Date'])\
                    .agg({'Score':['max','count'], 'Remark':['sum']})[['Score', 'Remark']]

    return {
        'Summary': summary_df,
        'Standard': standard_df, 
        'Individual': individual_df
    }


def update(sheet_key):
    """ Triggered by user, this method takes event data from Google Spreadsheet,
        tranform it into various forms, and publish them back to Googl
    """        
    credentials = ServiceAccountCredentials.from_json_keyfile_name(OAUTH, SCOPE)
    gc = gspread.authorize(credentials)
    sheet = gc.open_by_key(sheet_key)

    print('Reading Event data from {}...'.format(sheet.title))
    event_df = _get(sheet, 'Event')

    print('Transforming data...')
    df_map =  _transform(event_df)

    for tab_title, df in df_map.iteritems():
        print('Writing {0} data into {1}...'.format(tab_title, sheet.title))
        _put(sheet, tab_title, df)


def create(sheet_tile):
    """ Create new tabs from scratch from test data"""









if __name__ == '__main__':
    update('1wuUG7IIga74bZzYMMIkhepPufgI5QvNQie6CPTX5U5E')

