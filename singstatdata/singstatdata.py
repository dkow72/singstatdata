import requests
import pandas as pd
import re
from requests.exceptions import HTTPError
from IPython.display import display 

def get_json(url, params=None):
    if params != None:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            response_json = response.json()
            return response_json
            # If the response was successful, no Exception will be raised
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
    else:
        try:
            response = requests.get(url)
            response.raise_for_status()
            response_json = response.json()
            return response_json
            # If the response was successful, no Exception will be raised
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')   
            
def get_resource_id(keyphrases=None, match_all=False):
    """
    Retrieves resourceIDs of relevant Singstat time series datasets based on non case-sensitive keyphrases 

    Parameters
    ----------
    keyphrases: str or list 
      A python string or list of strings of keyphrases to match with available Singstat time series titles
    match_all: boolean
      A python boolean which determines whether to return Singstat time series with titles which match at least one or all of the keyphrases

    Returns
    -------
    list 
      Returns a list of resourceIDs of Singtat time series datasets identified by the keyphrases

    Example
    -------
    >>> from singstat import get_resource_id
    >>> keyphrases = ['property', 'office'] 
    >>> get_resource_id(keyphrases, match_all=False)
    [15139, 17122, 14657, 14682, 15138, 15135, 16708]
    >>> get_resource_id(keyphrases, match_all=True)
    [15138]
    """
    # Checking parameters
    if keyphrases != None:
        if type(keyphrases) not in [str, list]:
            raise TypeError('The argument keyphrases should be a string or a list of strings')
        if type(keyphrases) == list:
            if not all(isinstance(keyphrase, str) for keyphrase in keyphrases):
                raise TypeError('The argument keyphrases should be a string or a list of strings')
    if not isinstance(match_all, bool):
        raise TypeError('The argument match_all should be a boolean')

    url = 'https://www.tablebuilder.singstat.gov.sg/publicfacing/rest/timeseries/resourceId?keyword=%&searchOption=all'
    json = get_json(url)

    if not json:
        print('Failed to retrieve json file from Singstat')
        return
    
    if len(json['records']) == 0:
        print("No relevant datasets for given keyphrase")
        return None
    
    # Extracting resourceIDs by filtering for presence of keyphrases in time series datasets
    if keyphrases == None:
        return [list(dataset.values())[0] for dataset in json['records']]
    if type(keyphrases) == str:
        return [list(dataset.values())[0] for dataset in json['records'] if keyphrases.lower() in list(dataset.values())[1].lower()]
    elif type(keyphrases) == list:
        if match_all == False:
            regstr = '|'.join([keyphrase.lower() for keyphrase in keyphrases])
            return [list(dataset.values())[0] for dataset in json['records'] if re.search(regstr, list(dataset.values())[1].lower())]
        else:
            return [list(dataset.values())[0] for dataset in json['records'] if all(keyphrase.lower() in list(dataset.values())[1].lower() for keyphrase in keyphrases)]

def check_resource_ids(resource_ids):
    if type(resource_ids) not in [int, list]:
        raise TypeError('The argument resource_ids should be an integer or a list of integers')
        
    if type(resource_ids) == list:
        if not all(isinstance(ID, int) for ID in resource_ids):
            raise TypeError('The argument keyphrases should be an integer or a list of integers')

def get_overview(resource_ids):
    """
    Retrieves metadata of corresponding Singstat time series datasets presented in a pandas DataFrame

    Parameters
    ----------
    resource_ids: int or list 
      A python integer or list of integers to match with available Singstat time series resourceIDs
   
    Returns
    -------
    pandas DataFrame 
      Returns a pandas DataFrame containing metadata of the corresponding Singstat time series datasets

    Example
    -------
    >>> from singstat import get_overview
    >>> resource_ids = [15139, 17122, 14657]
    >>> get_overview(resource_ids)
                    resourceId                                              title  frequency  \
        0       14657  Judicial Officers In State Courts (End Of Peri...     Annual   
        1       15139  Private Residential Property Price Index By Ty...  Quarterly   
        2       17122  Property Price Index By Type Of Property (4th ...  Quarterly   

              uom                     datasource  \
        0  Number                   STATE COURTS   
        1   Index  URBAN REDEVELOPMENT AUTHORITY   
        2   Index  URBAN REDEVELOPMENT AUTHORITY   

                                                    footnote startPeriod endPeriod  \
        0                                               None        2012      2019   
        1  "Data are computed using stratified hedonic re...     1975 1Q   2020 3Q   
        2  The price index is computed based on fixed wei...     1975 1Q   2020 3Q   

                                                   variables  
        0  [{'variableCode': 'M891021.1', 'variableName':...  
        1  [{'variableCode': 'M212261.1', 'variableName':...  
        2  [{'variableCode': 'M210641.2', 'variableName':...  
        
    >>> resource_ids = 15139
    >>> get_overview(resource_ids)
               resourceId                                              title  frequency  \
        0       15139  Private Residential Property Price Index By Ty...  Quarterly   

             uom                     datasource  \
        0  Index  URBAN REDEVELOPMENT AUTHORITY   

                                                    footnote startPeriod endPeriod  \
        0  "Data are computed using stratified hedonic re...     1975 1Q   2020 3Q   

                                                   variables  
        0  [{'variableCode': 'M212261.1', 'variableName':...  
    """
    # Checking parameters
    check_resource_ids(resource_ids)
    
    df_full = pd.DataFrame()

    def build_metadata_table(df_full, ID):
        url = f'https://www.tablebuilder.singstat.gov.sg/publicfacing/rest/timeseries/metadata/{ID}'
        json = get_json(url)
        df = pd.DataFrame(json['records'])
        df.drop(columns=['downloadFormats', 'termsOfUse', 'apiTermsOfService', 'url'], inplace=True)
        df_full = pd.concat([df_full, df])
        return df_full

    if type(resource_ids) == int:
        df_full = build_metadata_table(df_full, resource_ids)

    elif type(resource_ids) == list:
        for ID in resource_ids:
            df_full = build_metadata_table(df_full, ID)

    df_full.sort_values('resourceId', inplace=True)
    df_full.reset_index(inplace=True, drop=True)
    return df_full

def get_timeseries(resource_ids, limit=10000, offset=0):
    """
    Retrieves corresponding Singstat timeseries datasets
    
    Parameters
    ----------
    resource_ids: int or list 
      A python integer or list of integers to match with available Singstat time series resourceIDs
    
    limit: int
      A python integer specifying the maximum number of records to be included for each corresponding dataset
    
    offset: int
      A python integer specifying the first n number of records to be excluded for each corresponding dataset


    Returns
    -------
    dict 
      Returns a dictionary of key value pairings of resourceIDs and their corresponding time series dataset in a pandas DataFrame 

    Example
    -------
    >>> from singstat import get_timeseries
    >>> resource_ids = [15139, 17122]
    >>> get_timeseries(resource_ids)
            {15139: variableCode              M212261.1 M212261.1.2 M212261.1.1
         variableName Residential Properties  Non-landed      Landed
         time                                                       
         1975 1Q                         8.9        10.5         7.4
         1975 2Q                         9.1        11.2         7.7
         1975 3Q                         9.1        11.5         7.8
         1975 4Q                         9.1        11.5         7.9
         1976 1Q                         9.5        11.5         7.9
         ...                             ...         ...         ...
         2019 3Q                       152.8       150.0       165.8
         2019 4Q                       153.6       149.6       171.8
         2020 1Q                       152.1       148.1       170.3
         2020 2Q                       152.6       148.7       170.3
         2020 3Q                       153.8       148.8       176.6

         [183 rows x 3 columns],
         17122: variableCode                      M210641.2  \
         variableName Office Space In Central Region   
         time                                          
         1975 1Q                                15.2   
         1975 2Q                                15.2   
         1975 3Q                                15.8   
         1975 4Q                                16.3   
         1976 1Q                                17.2   
         ...                                     ...   
         2019 3Q                               138.8   
         2019 4Q                               138.1   
         2020 1Q                               132.6   
         2020 2Q                               126.9   
         2020 3Q                               127.2   

         variableCode                                  M210641.2.2  \
         variableName Office Space In Central Region (Fringe Area)   
         time                                                        
         1975 1Q                                               NaN   
         1975 2Q                                               NaN   
         1975 3Q                                               NaN   
         1975 4Q                                               NaN   
         1976 1Q                                               NaN   
         ...                                                   ...   
         2019 3Q                                             123.9   
         2019 4Q                                             120.1   
         2020 1Q                                             113.4   
         2020 2Q                                             119.2   
         2020 3Q                                             114.0   

         variableCode                                   M210641.2.1  \
         variableName Office Space In Central Region (Central Area)   
         time                                                         
         1975 1Q                                                NaN   
         1975 2Q                                                NaN   
         1975 3Q                                                NaN   
         1975 4Q                                                NaN   
         1976 1Q                                                NaN   
         ...                                                    ...   
         2019 3Q                                              141.0   
         2019 4Q                                              140.9   
         2020 1Q                                              135.7   
         2020 2Q                                              129.9   
         2020 3Q                                              131.2   

         variableCode                      M210641.5  \
         variableName Retail Space In Central Region   
         time                                          
         1975 1Q                                 NaN   
         1975 2Q                                 NaN   
         1975 3Q                                 NaN   
         1975 4Q                                 NaN   
         1976 1Q                                 NaN   
         ...                                     ...   
         2019 3Q                               112.0   
         2019 4Q                               114.0   
         2020 1Q                               110.5   
         2020 2Q                               108.8   
         2020 3Q                               111.2   

         variableCode                                   M210641.5.1  \
         variableName Retail Space In Central Region (Central Area)   
         time                                                         
         1975 1Q                                                NaN   
         1975 2Q                                                NaN   
         1975 3Q                                                NaN   
         1975 4Q                                                NaN   
         1976 1Q                                                NaN   
         ...                                                    ...   
         2019 3Q                                               96.0   
         2019 4Q                                               99.5   
         2020 1Q                                               94.4   
         2020 2Q                                               93.8   
         2020 3Q                                               92.2   

         variableCode                                  M210641.5.2  
         variableName Retail Space In Central Region (Fringe Area)  
         time                                                       
         1975 1Q                                               NaN  
         1975 2Q                                               NaN  
         1975 3Q                                               NaN  
         1975 4Q                                               NaN  
         1976 1Q                                               NaN  
         ...                                                   ...  
         2019 3Q                                             132.4  
         2019 4Q                                             126.3  
         2020 1Q                                             122.4  
         2020 2Q                                             117.1  
         2020 3Q                                             123.5  

         [183 rows x 6 columns]}
    """
    # Checking parameters
    check_resource_ids(resource_ids)
    
    if type(limit) != int:
        raise TypeError('The argument limit should be an integer')
    
    if type(offset) != int:
        raise TypeError('The argument offset should be an integer')
    
    def build_table(ID):
        url = f'https://www.tablebuilder.singstat.gov.sg/publicfacing/rest/timeseries/tabledata/{ID}'
        json = get_json(url, params={'limit': limit, 'offset': offset})
        df = pd.DataFrame(json['records'])
        df = df.pivot(index='time', columns=['variableCode', 'variableName'], values='value')
        return df
    
    timeseries_dict = dict()
    
    if type(resource_ids) == int:
        timeseries_dict[resource_ids] = build_table(resource_ids)
    
    elif type(resource_ids) == list:
        for ID in resource_ids:
            timeseries_dict[ID] = build_table(ID)
    
    return timeseries_dict

class timeseries_search():
    def __init__(self, keyphrases=None, match_all=False):
        self.resource_ids = get_resource_id(keyphrases, match_all=match_all)
        self.overview = get_overview(self.resource_ids)
        self.timeseries = get_timeseries(self.resource_ids)
    
    def filter_datasets(self, frequency, start_year=None, inplace=False):
        """
        Filters timeseries_search object based on frequency and start year of time series dataset

        Parameters
        ----------
        frequency:
          A python string or list of strings which filters datasets based on frequency 
        
        start_year:
          A python integer 

        Returns
        -------
        dict 
          Returns a dictionary of key value pairings of resourceIDs and their corresponding time series dataset in a pandas DataFrame 
        
        OR
        
        NoneType 
          Updates the timeseries_search object

        Example
        -------
        """
        # Checking parameters
        if type(frequency) not in [str, list] or not all(isinstance(freq, str) for freq in frequency):
            raise TypeError('The argument frequency should be a string or a list of strings')
        
        if type(start_year) not in [int, type(None)] or len(str(start_year)) != 4:
            raise TypeError('The argument start_year should be a 4 digit integer')
        
        if not isinstance(inplace, bool):
            raise TypeError('The argument inplace should be a boolean')
        
        frequencies = ['all,', 'annual', 'quarterly', 'monthly', 'ad-hoc', 'half-yearly']
        if type(frequency) == list:
            for freq in frequency:
                assert freq in frequencies, "Frequencies must be chosen from: 'all', 'annual', 'quarterly', 'monthly', 'ad-hoc', 'half-yearly'"
            if 'all' not in frequency: 
                filtered_overview = self.overview[self.overview['frequency'].str.lower().isin(frequency)]
            else:
                filtered_overview = self.overview
        
        elif type(frequency) == str:
            assert freq in frequencies, "Frequencies must be chosen from: 'all', 'annual', 'quarterly', 'monthly', 'ad-hoc', 'half-yearly'"
            if frequency != 'all':
                filtered_overview = self.overview[self.overview['frequency'].str.lower() == frequency]
            else:
                filtered_overview = self.overview
                
        if start_year:
            filtered_overview = filtered_overview[[int(i[0]) >= start_year for i in filtered_overview['startPeriod'].str.split()]]
        else:
            filtered_overview = filtered_overview
        
        new_ids = list(filtered_overview['resourceId'])
        
        if inplace == True:
            self.resource_ids = new_ids
            self.overview = self.overview.loc[self.overview['resourceId'].isin(new_ids)]
            self.timeseries = {ID: self.timeseries[ID] for ID in self.resource_ids}
            display(self.overview.loc[self.overview['resourceId'].isin(new_ids)])
        
        else:
            display(self.overview.loc[self.overview['resourceId'].isin(new_ids)])
            return {ID: self.timeseries[ID] for ID in new_ids}