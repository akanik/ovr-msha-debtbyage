import pandas as pd
import numpy as np
import datetime, glob, os 


data_dir = '../data/'
now = datetime.datetime.now()

delinquency_data = pd.read_csv(data_dir + 'debtbyage_20181205_REFINED.csv')
delinquency_data['Delinquent Date'] =  pd.to_datetime(delinquency_data['Delinquent Date'], format='%m/%d/%Y')


def get_delinquent_injuries(mine_id):
    accident_data = pd.read_csv(data_dir + 'msha_accident_20190302_y94_y19.csv', escapechar='\\')
    mine_data = accident_data[accident_data['mine_id'] == mine_id]
    
    inj_data = mine_data[(mine_data.no_injuries > 0) & (pd.isna(mine_data.cntctr_id))]
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
        
    inj_data['delinquent_date'] = inj_data['mine_id'].map(earliest_series)
    inj_data['delinquent'] = np.where(pd.to_datetime(inj_data['ai_dt'], format='%Y/%m/%d') > inj_data['delinquent_date'],1,0)
    
    inj_data.to_csv(data_dir + 'analysis/' + str(mine_id) + '-injury-data.csv')
    

def get_annual_hrs_prod(mine_id):
    oprtr_hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20190302_y94_y19.csv')
    mine_hrs_data = oprtr_hrs_data[oprtr_hrs_data['mine_id'] == mine_id]
    
    annual_hrs_prod = mine_hrs_data.groupby('calendar_yr', as_index=False)['annual_hrs','annual_coal_prod'].sum()
    
    annual_hrs_prod.to_csv(data_dir + 'analysis/' + str(mine_id) + '-annual-hrs-prod.csv')
    
    

def return_hours():
    oprtr_hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20190302_y94_y19.csv')
    return oprtr_hrs_data

def get_inj_prod_by_mine(inj_data, injury_type):    
    oprtr_hrs_data = return_hours()
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
        
    
    def calc_injury_rate(row):
        if row['rate_type'] == 'Delinquent':
            return (row['del_inj'] / (row['annual_hrs'] / 2000)) * 100
        elif row['rate_type'] == 'Becoming delinquent':
            return ((row['del_inj'] + row['non_inj']) / (row['annual_hrs'] / 2000)) * 100
        else:
            return (row['non_inj'] / (row['annual_hrs'] / 2000)) * 100
        
    
    inj_mine_year = pd.pivot_table(inj_data, index=['mine_id','cal_yr'], values='document_no', 
                                    columns=['delinquent'], aggfunc='count').my_flatten_cols()
                                        
    hrs_prod_mine_year = oprtr_hrs_data.groupby(['mine_id','calendar_yr'], as_index=False)['annual_hrs','annual_coal_prod'].sum()
    hrs_prod_mine_year = hrs_prod_mine_year.rename(index=str, columns={'calendar_yr': 'cal_yr'})
    
    hrs_prod_inj_mine_year = pd.merge(hrs_prod_mine_year, inj_mine_year, how='left', on=['mine_id','cal_yr'], suffixes=('_m','_inj'))
    hrs_prod_inj_mine_year = hrs_prod_inj_mine_year.rename(index=str, columns={0: 'non_inj',1:'del_inj'})
    
    hrs_prod_inj_mine_year['earliest_date'] = hrs_prod_inj_mine_year['mine_id'].map(earliest_series)
    hrs_prod_inj_mine_year['earliest_year'] = pd.DatetimeIndex(hrs_prod_inj_mine_year['earliest_date']).year + 1    
    hrs_prod_inj_mine_year['rate_type'] = hrs_prod_inj_mine_year.apply(find_rate_type, axis=1)
    hrs_prod_inj_mine_year['inj_rate'] = hrs_prod_inj_mine_year.apply(calc_injury_rate, axis=1)
    
    by_mine_delinquency = hrs_prod_inj_mine_year.pivot_table(hrs_prod_inj_mine_year, index='mine_id', columns='rate_type',
                                                            aggfunc={'annual_hrs':'sum','annual_coal_prod':'sum','non_inj':'sum',
                                                                    'del_inj':'sum','inj_rate':'mean'}).reset_index()
    
    by_mine_delinquency.columns = [' '.join(col).strip() for col in by_mine_delinquency.columns.values]
    by_mine_delinquency = by_mine_delinquency[['mine_id','annual_coal_prod Became delinquent', 'annual_coal_prod Delinquent','annual_coal_prod Non-delinquent', 
                                    'annual_hrs Became delinquent','annual_hrs Delinquent', 'annual_hrs Non-delinquent',
                                    'del_inj Became delinquent', 'del_inj Delinquent', 
                                    'inj_rate Became delinquent','inj_rate Delinquent', 'inj_rate Non-delinquent',
                                    'non_inj Became delinquent','non_inj Non-delinquent']]
    by_mine_delinquency = by_mine_delinquency.rename(index=str, columns={'annual_coal_prod Became delinquent': 'prod_became',
                                                                        'annual_coal_prod Delinquent':'prod_del',
                                                                        'annual_coal_prod Non-delinquent':'prod_non',
                                                                        'annual_hrs Became delinquent': 'hrs_became',
                                                                        'annual_hrs Delinquent':'hrs_del',
                                                                        'annual_hrs Non-delinquent':'hrs_non',
                                                                        'del_inj Became delinquent': 'del_inj_became',
                                                                        'del_inj Delinquent':'del_inj',
                                                                        'non_inj Became delinquent': 'non_inj_became',
                                                                        'non_inj Non-delinquent':'non_inj',
                                                                        'inj_rate Became delinquent': 'inj_rate_became',
                                                                        'inj_rate Delinquent':'inj_rate_del',
                                                                        'inj_rate Non-delinquent':'inj_rate_non'})

    by_mine_delinquency.to_csv(data_dir + 'analysis/msha_' + injury_type + '_MINE_delinquency.csv')
    
    
#get_delinquent_injuries(4407252)
get_annual_hrs_prod(4407252)