# TODO Link data description

# Datasource local = https://github.com/daenuprobst/covid19-cases-switzerland
# Datasource global = https://github.com/CSSEGISandData/COVID-19

# Imports
import pandas as pd
from datetime import date, timedelta

# Constants
DROP_COLUMNS = ['FIPS','Admin2','Province_State','Recovered','Combined_Key',
                'Incidence_Rate','Case-Fatality_Ratio', 'Province/State', 'Last_Update', 'Long','Lat']
TODAY = date.today()
YESTERDAY = TODAY - timedelta(1)

def get_data():
    """
    Pulls latest data from sources
    :return df_global: Dataframe containg current Covid-19 Data from John Hopkins University
    :return df_CH_cases: Daily new cases in Switzerland
    :return df_CH_fatal: Daily new fatalities in Switzerland
    """
    
    df_global_daily_cases = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')    
    df_global_daily_fatal = pd.read_csv('https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
    
    # Get most recent data from covid19-cases-switzerland
    df_CH_cases = pd.read_csv('https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/covid19_cases_switzerland_openzh-phase2.csv')
    df_CH_fatal = pd.read_csv('https://raw.githubusercontent.com/daenuprobst/covid19-cases-switzerland/master/covid19_fatalities_switzerland_openzh-phase2.csv')
    
    return [df_global_daily_cases, df_global_daily_fatal, df_CH_cases, df_CH_fatal]

def drop_columns(dfs):
    """
    Drops columns in list of dataframes if column name is in columns
    :param dfs: Lisst of dataframes
    :return dfs: List of dataframes
    """
    for df in dfs:
        columns = df.columns.intersection(DROP_COLUMNS)
        df.drop(columns=columns, inplace=True)
    return dfs

def rename(dfs):
    """
    Rename Columns with same information to get uniform columns across dataframes (easier down the line)
    :param dfs: List of dataframes
    :return dfs: List of dataframes with equal column names
    """
    for df in dfs:
        if 'Country_Region' in df.columns or 'Country/Region' in df.columns:
            df.rename({'Country_Region':'country',
                       'Country/Region':'country'}, inplace=True, axis=1)
        if 'Date' in df.columns:
            df.rename({'Date':'date'}, inplace=True, axis=1)
    return dfs

def drop_nan(dfs):
    """
    Drops empty rows
    :param dfs: Lisst of dataframes
    :return dfs: List of dataframes without NAN rows
    """
    for df in dfs:
        df.dropna(how='all', inplace=True)
    return dfs

def groupby_country(dfs):
    """
    Group data by Country or. Canton
    :param dfs: List of dataframes
    :return dfs: List of dataframes all grouped by country or Canton
    """
    for i,df in enumerate(dfs):
        # World data
        if 'country' in df.columns:
            df_copy = df.copy()
            df_copy = df_copy[['country']]
            
            columns = [e for e in df.columns if e not in ['country']]
            
            df = df.groupby(by=['country'])[columns].agg('sum').reset_index()
            
            df = df.merge(df_copy, left_on='country', right_on='country')
            
            dfs[i] = df.drop_duplicates(subset='country', keep='first')
    return dfs

def new_data(dfs):
    """
    Melts DataFrames into format "country"("canton"),"date","total_cases","new_cases","total_fatal","new_fatal"
    :param dfs: List of Dataframes
    :return dfs: List of Dataframes in new format
    """
    label = ['cases','fatal','cases','fatal']
    
    for i, df in enumerate(dfs):
        # global data
        if 'country' in df.columns:
            df_new = None
            for c in df.country.unique():
                df_c = df[df['country']==c].copy()
                df_c  = df_c.T.reset_index()
                df_c.columns = ['date',f'total_{label[i]}']
                df_c = df_c.drop(0)
                df_c[f'new_{label[i]}'] = df_c[f'total_{label[i]}'].diff()
                df_c['country'] = c
                if df_new is None:
                    df_new = df_c
                else:
                    df_new = df_new.append(other=df_c,ignore_index=True)
            # add new_data
            dfs[i] = df_new
        # local data
        else:
            df_new = None
            df = df.melt(id_vars='date',var_name='canton',value_name=f'total_{label[i]}')
            for c in df.canton.unique():
                df_c = df[df['canton']==c].copy()
                df_c[f'new_{label[i]}'] = df_c[f'total_{label[i]}'].diff()
                
                if df_new is None:
                    df_new = df_c
                else:
                    df_new = df_new.append(other=df_c,ignore_index=True)
            
            # add new_data
            dfs[i] = df_new
            
    return dfs

            
def merge_data(dfs):
    """
    Merges prepared Dataframes into two dataframes (df_global and df_local)
    :param dfs: List of DataFrames
    :return df_global: DataFrame containing Covid-Data for every country
    :return df_local: DataFrame containing Covid-Data for every canton in switzerland
    """
    df_global, df_local = None, None
    
    for df in dfs:
        if 'country' in df.columns:
            if df_global is None:
                df_global = df
            else:
                df_global = df_global.merge(right=df, on=['country','date'],how='left')
        elif 'canton' in df.columns:
            if df_local is None:
                df_local = df
            else:
                df_local = df_local.merge(right=df, on=['canton','date'],how='left')

    return df_global, df_local

def moving_average(df_global, df_local):
    df_global['ma_cases'] = df_global['new_cases'].rolling(7).mean()
    df_global['ma_fatal'] = df_global['new_fatal'].rolling(7).mean()
    df_local['ma_cases'] = df_local['new_cases'].rolling(7).mean()
    df_local['ma_fatal'] = df_local['new_fatal'].rolling(7).mean()

    return df_global, df_local

def covid_pipe():
    """
    Covid-19 data pipeline
    :return df_global: DataFrame containing total_cases, new_cases, total_fatal (fatalities), 
                       new_fatal for every day and country since tracking
    :return df_local: DataFrame containing total_cases, new_cases, total_fatal (fatalities), 
                      new_fatal for every day and canton in switzerland since tracking
    """
    dfs = get_data()
    dfs = drop_columns(dfs)
    dfs = rename(dfs)
    dfs = drop_nan(dfs)
    dfs = groupby_country(dfs)
    dfs = new_data(dfs)
    df_global, df_local = merge_data(dfs)
    df_global, df_local = moving_average(df_global, df_local)
    
    return df_global, df_local