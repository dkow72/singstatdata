from singstatdata import __version__
from singstatdata import singstatdata

import requests
import pandas as pd
import re
from requests.exceptions import HTTPError
from IPython.display import display 

def test_version():
    assert __version__ == '0.1.0'
    
def test_get_resource_id():
    assert type(singstatdata.get_resource_id('price')) == list
    for id in singstatdata.get_resource_id('price'):
        assert type(id) == int
    assert type(singstatdata.get_resource_id(['food', 'price'])) == list
    for id in singstatdata.get_resource_id(['food', 'price']):
        assert type(id) == int
    
def test_get_resource_id_case_insensitivity():
    assert singstatdata.get_resource_id('price') == singstatdata.get_resource_id('Price') 
    assert singstatdata.get_resource_id('price') == singstatdata.get_resource_id('PRicE') 

def test_get_resource_id_match_all():
    len_food = len(singstatdata.get_resource_id('food'))
    len_price = len(singstatdata.get_resource_id('price'))
    len_food_price = len(singstatdata.get_resource_id(['food', 'price'], match_all=True))
    assert len(singstatdata.get_resource_id(['food', 'price'], match_all=False)) == len_food + len_price - len_food_price

def test_get_overview():
    assert type(singstatdata.get_overview(17030)) == pd.DataFrame
    assert type(singstatdata.get_overview([17030, 17035, 17036, 17037])) == pd.DataFrame
    
def test_get_overview_length():
    ids = [17030, 17035, 17036, 17037]
    assert len(singstatdata.get_overview(ids)) == len(ids)
    
def test_get_timeseries():
    assert type(singstatdata.get_timeseries(17030)) == dict
    assert type(singstatdata.get_timeseries([17030, 17035, 17036, 17037])) == dict
    
def test_get_timeseries_length():
    ids = [17030, 17035, 17036, 17037]
    assert len(singstatdata.get_timeseries(ids)) == len(ids)