#https://enforcedata.dol.gov/views/data_catalogs.php

import pandas as pd
import numpy as np
import datetime, glob, os


data_dir = '../data/'
now = datetime.datetime.now()

delinquency_data = pd.read_csv(data_dir + 'debtbyage_20181205_REFINED.csv')
delinquency_data['Delinquent Date'] =  pd.to_datetime(delinquency_data['Delinquent Date'], format='%m/%d/%Y')


#################################################################
# HELPER FUNTIONS
#################################################################

def __my_flatten_cols(self, how="_".join, reset_index=True):
    how = (lambda iter: list(iter)[-1]) if how == "last" else how
    self.columns = [how(filter(None, map(str, levels))) for levels in self.columns.values] \
                    if isinstance(self.columns, pd.MultiIndex) else self.columns
    return self.reset_index() if reset_index else self
pd.DataFrame.my_flatten_cols = __my_flatten_cols
    
def combine_segment_violations():
    #violations data comes in 5 different files. we're combining them here
    violations_dir = data_dir + 'msha_violation_20190302.csv'
    files = glob.glob(os.path.join(violations_dir, "*.csv"))
    
    all_violations = pd.concat((pd.read_csv(f) for f in files))
    all_violations['iss_dt'] =  pd.to_datetime(all_violations['iss_dt'], format='%Y-%m-%d')
    
    #segment by date
    rel_violations = all_violations[all_violations['iss_dt'] > '1993-12-31']
    rel_violations.to_csv(data_dir + 'msha_violations_20190302_y94_y19.csv')
    
    #test date segmentation
    print(rel_violations.iss_dt.min())
    print(rel_violations.iss_dt.max())


def get_refined_delinquencies():
    del_dtypes = {'Ending Balance': np.float64} 
    delinquency_data = pd.read_csv(data_dir + 'DEBTAGE12052018_DET2.csv', dtype=del_dtypes)
    
    print(delinquency_data.info())
    
    #filter by Violator Type == 'Operator'
    oprtr_delinquencies = delinquency_data[delinquency_data['Violator Type'] == 'Operator']
    
    #filter by Age Category not in ['1-30 days','31-60 days','61-90 days']
    remove_dates = ['1-30 days','31-60 days','61-90 days']
    aged_delinquencies = oprtr_delinquencies[~oprtr_delinquencies['Age Category'].isin(remove_dates)]
    
    #filter by Delinquent Type not in ['Hold Status','HoldStatus']
    remove_types = ['Hold Status','HoldStatus']
    active_delinquencies = aged_delinquencies[~aged_delinquencies['Delinquent Type'].isin(remove_types)]
    
    active_delinquencies.to_csv(data_dir + 'debtbyage_20181205_REFINED.csv')
    
    #test delinquency filtering
    print(delinquency_data.groupby('Violator Type').agg('count'))
    print(delinquency_data.groupby('Age Category').agg('count'))
    print(delinquency_data.groupby('Delinquent Type').agg('count'))


# There's an issue with the ai_narr column and double quotes, as in "He fell off
# of a 6" ledge..." Removing that column because it breaks everything.    
def segment_accidents():
    fields = ['mine_id', 'ai_dt', 'controller_id', 'controller_name', 
        'operator_id', 'operator_name', 'document_no', 'cal_yr', 'accident_time', 
        'degree_injury_cd', 'fips_state_cd', 'no_injuries','ai_year']    
       
    accident_data = pd.read_csv(data_dir + 'msha_accident_20190302-0.csv', escapechar='\\')
    accident_data['ai_dt'] =  pd.to_datetime(accident_data['ai_dt'], format='%d-%m-%Y')
    
    rel_accidents = accident_data[accident_data['ai_dt'] > '1993-12-31']
    rel_accidents.to_csv(data_dir + 'msha_accident_20190302_y94_y19.csv')
    
    #test date segmentation
    print(rel_accidents.ai_dt.min())
    print(rel_accidents.ai_dt.max())
    
    
def segment_hrs():  
    hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20190302-0.csv')
    
    rel_hrs = hrs_data[hrs_data['calendar_yr'] > 1993]
    rel_hrs.to_csv(data_dir + 'msha_cy_oprtr_emplymnt_20190302_y94_y19.csv')
    
    #test date segmentation
    print(rel_hrs.calendar_yr.min())
    print(rel_hrs.calendar_yr.max())
    
   
    
def refine_mines():
    mine_data = pd.read_csv(data_dir + 'msha_mine_20190209-0.csv', escapechar='\\')
       
    mine_rename = {'controller_id':'curr_cntrlr_id', 'controller_nm':'curr_cntrlr_nm', 
                    'oper_id':'curr_oper_id', 'oper_nm':'curr_oper_nm'}
                    
    mine_data = mine_data.rename(index=str, columns=mine_rename)
    
    mine_data = mine_data[~((mine_data['curr_stat_cd'].isin(['Abandoned','AbandonedSealed'])) & (pd.to_datetime(mine_data['curr_stat_dt'], format='%d-%m-%Y') < pd.to_datetime('1994-01-01', format='%Y-%m-%d')))]
    
    return mine_data
    

def find_rate_type(row):
    if row['cal_yr'] >= row['earliest_year']:
        return 'Delinquent'
    elif row['cal_yr'] == (row['earliest_year']-1):
        return 'Became delinquent'
    else:
        return 'Non-delinquent'


#Create a unique list of currently delinquent mines
def get_delinquent_mines():
    return delinquency_data['Mine ID'].unique()


#Create a unique list of mines that have been delinquent since the year passed to this function
def get_delinquent_since(year): 
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min')
    
    earliest_df = pd.DataFrame({'mine_id':earliest_series.index, 'earliest_date':earliest_series.values})
    
    #Need to round earliest_date up to the next year so we can factor only years with full delinquency
    earliest_df['earliest_year'] = earliest_df['earliest_date'].map(lambda x: x.year+1)
    
    delinquent_since = earliest_df[earliest_df['earliest_year'] <= year]
        
    return delinquent_since['mine_id'].unique()

    
#60% of mines have more than one active delinquency. 
#Create a dataframe with unique mine_ids and the earliest delinquency date on record
def find_delinquent_mine_dates():
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min')
    
    earliest_df = pd.DataFrame({'mine_id':earliest_series.index, 'earliest_date':earliest_series.values})
    
    #Need to round earliest_date up to the next year so we can factor only years with full delinquency
    earliest_df['earliest_year'] = earliest_df['earliest_date'].map(lambda x: x.year+1)
    
    return earliest_df
    
    
def get_delinquency_data_by_mine():
    by_mine = delinquency_data.groupby('Mine ID').agg({'Delinquent Date':['max','min'],'Ending Balance':['sum','count'], 'Controller Id':'nunique'})
    print(by_mine.head(20))

    
def get_delinquency_data_by_controller():
    by_controller = delinquency_data.groupby('Controller Id').agg({'Controller Name':'unique','Delinquent Date':['max','min'],'Ending Balance':['sum','count'], 'Mine ID':'nunique'})
    print(by_controller.sort_values(('Ending Balance','sum'), ascending=False))
    
    
def get_delinquent_viols():
    viol_data = pd.read_csv(data_dir + 'msha_violations_20190302_y94_y19.csv')
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
        
    viol_data['delinquent_date'] = viol_data['mine_id'].map(earliest_series)
    #this adds a delinquency flag to the violation
    viol_data['delinquent'] = np.where(pd.to_datetime(viol_data['iss_dt'], format='%Y/%m/%d') > viol_data['delinquent_date'],1,0)
    
    #viol_data.to_csv(data_dir + 'analysis/delinquent_violations.csv')
    return viol_data
    
    #at this point we can create two dataframes, del_viol and non_viol
    #we can create the pivot tables of likelihood, see mine_likelihood_viol above
    #this is good for mine level data
    #but then we can do groupbys on each pivot table and get average violations of each type per mine for both delinquent and 
    #non-delinquent mines
    
    #You need to do this above and also for injuries because this is a much better way of doing it... right?


def get_delinquent_injuries():
    accident_data = pd.read_csv(data_dir + 'msha_accident_20190302_y94_y19.csv', escapechar='\\')
    inj_data = accident_data[(accident_data.no_injuries > 0) & (pd.isna(accident_data.cntctr_id))]
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
        
    inj_data['delinquent_date'] = inj_data['mine_id'].map(earliest_series)
    inj_data['delinquent'] = np.where(pd.to_datetime(inj_data['ai_dt'], format='%Y/%m/%d') > inj_data['delinquent_date'],1,0)
    
    return inj_data
    
def get_hours():
    oprtr_hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20190302_y94_y19.csv')
    return oprtr_hrs_data
    
#################################################################

def get_national_inj_rate():
    oprtr_hrs_data = get_hours()
    inj_data = get_delinquent_injuries()
    
    def calc_injury_rate(row):
        if row['annual_hrs'] < 2000:
            return ''
        else:
            return (row['inj_cnt'] / (row['annual_hrs'] / 2000)) * 100
    
    inj_mine_year = inj_data.groupby('cal_yr')['document_no'].count().reset_index()
    inj_mine_year = inj_mine_year.rename(index=str, columns={'document_no': 'inj_cnt'})
                                    
    hrs_prod_mine_year = oprtr_hrs_data.groupby('calendar_yr', as_index=False)['annual_hrs'].sum()
    hrs_prod_mine_year = hrs_prod_mine_year.rename(index=str, columns={'calendar_yr': 'cal_yr'})
    
    hrs_prod_inj_mine_year = pd.merge(hrs_prod_mine_year, inj_mine_year, how='left', on=['cal_yr'], suffixes=('_m','_inj'))
    #hrs_prod_inj_mine_year = hrs_prod_inj_mine_year.rename(index=str, columns={0: 'non_inj',1:'del_inj'})
    
    hrs_prod_inj_mine_year['inj_rate'] = hrs_prod_inj_mine_year.apply(calc_injury_rate, axis=1)
    
    national_avg_inj_rate = hrs_prod_inj_mine_year[hrs_prod_inj_mine_year['cal_yr'] < 2019]['inj_rate'].mean()
    print(national_avg_inj_rate)
    


def get_inj_prod_by_mine(inj_data, injury_type):    
    oprtr_hrs_data = get_hours()
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
    
    
    
def get_inj_prod_by_mine_year(inj_data, injury_type):    
    oprtr_hrs_data = get_hours()
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
    
    print(hrs_prod_inj_mine_year[hrs_prod_inj_mine_year['mine_id'] == 4601437])
    
    #by_mine_delinquency = hrs_prod_inj_mine_year.pivot_table(hrs_prod_inj_mine_year, index='mine_id', columns='rate_type',
    #                                                        aggfunc={'annual_hrs':'sum','annual_coal_prod':'sum','non_inj':'sum',
    #                                                                'del_inj':'sum','inj_rate':'mean'}).reset_index()
    #
    #by_mine_delinquency.columns = [' '.join(col).strip() for col in by_mine_delinquency.columns.values]
    #by_mine_delinquency = by_mine_delinquency[['mine_id','annual_coal_prod Became delinquent', 'annual_coal_prod Delinquent','annual_coal_prod Non-delinquent', 
    #                                'annual_hrs Became delinquent','annual_hrs Delinquent', 'annual_hrs Non-delinquent',
    #                                'del_inj Became delinquent', 'del_inj Delinquent', 
    #                                'inj_rate Became delinquent','inj_rate Delinquent', 'inj_rate Non-delinquent',
    #                                'non_inj Became delinquent','non_inj Non-delinquent']]
    #by_mine_delinquency = by_mine_delinquency.rename(index=str, columns={'annual_coal_prod Became delinquent': 'prod_became',
    #                                                                    'annual_coal_prod Delinquent':'prod_del',
    #                                                                    'annual_coal_prod Non-delinquent':'prod_non',
    #                                                                    'annual_hrs Became delinquent': 'hrs_became',
    #                                                                    'annual_hrs Delinquent':'hrs_del',
    #                                                                    'annual_hrs Non-delinquent':'hrs_non',
    #                                                                    'del_inj Became delinquent': 'del_inj_became',
    #                                                                    'del_inj Delinquent':'del_inj',
    #                                                                    'non_inj Became delinquent': 'non_inj_became',
    #                                                                    'non_inj Non-delinquent':'non_inj',
    #                                                                    'inj_rate Became delinquent': 'inj_rate_became',
    #                                                                    'inj_rate Delinquent':'inj_rate_del',
    #                                                                    'inj_rate Non-delinquent':'inj_rate_non'})
    #
    #by_mine_delinquency.to_csv(data_dir + 'analysis/msha_' + injury_type + '_MINE_YEAR_delinquency.csv')
    

def get_inj_prod_by_year(inj_data, injury_type):
    oprtr_hrs_data = get_hours()
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
        
    
    def calc_injury_rate(row):
        return (row['inj_count'] / (row['annual_hrs'] / 2000)) * 100
        
    
    inj_mine_year = pd.pivot_table(inj_data, index=['mine_id','cal_yr'], values='document_no', 
                                    columns=['delinquent'], aggfunc='count').my_flatten_cols()
                                        
    hrs_prod_mine_year = oprtr_hrs_data.groupby(['mine_id','calendar_yr'], as_index=False)['annual_hrs','annual_coal_prod'].sum()
    hrs_prod_mine_year = hrs_prod_mine_year.rename(index=str, columns={'calendar_yr': 'cal_yr'})
    
    hrs_prod_inj_mine_year = pd.merge(hrs_prod_mine_year, inj_mine_year, how='left', on=['mine_id','cal_yr'], suffixes=('_m','_inj'))
    hrs_prod_inj_mine_year = hrs_prod_inj_mine_year.rename(index=str, columns={0: 'non_inj',1:'del_inj'})
    
    hrs_prod_inj_mine_year['earliest_date'] = hrs_prod_inj_mine_year['mine_id'].map(earliest_series)
    hrs_prod_inj_mine_year['earliest_year'] = pd.DatetimeIndex(hrs_prod_inj_mine_year['earliest_date']).year + 1    
    hrs_prod_inj_mine_year['rate_type'] = hrs_prod_inj_mine_year.apply(find_rate_type, axis=1)
    
    del_hrs_prod_inj = hrs_prod_inj_mine_year[hrs_prod_inj_mine_year['rate_type'] == 'Delinquent']
    del_by_year = del_hrs_prod_inj.groupby('cal_yr')['annual_hrs','annual_coal_prod','del_inj'].sum()
    del_by_year['del_inj_rate'] = (del_by_year['del_inj'] / (del_by_year['annual_hrs'] / 2000)) * 100
    del_by_year = del_by_year.rename(index=str, columns={'annual_hrs': 'del_hrs','annual_coal_prod':'del_coal_prod'})
    
    became_hrs_prod_inj = hrs_prod_inj_mine_year[hrs_prod_inj_mine_year['rate_type'] == 'Became delinquent']
    became_by_year = became_hrs_prod_inj.groupby('cal_yr')['annual_hrs','annual_coal_prod','non_inj','del_inj'].sum()
    became_by_year['total_inj'] = became_by_year['non_inj'] + became_by_year['del_inj']
    became_by_year['became_inj_rate'] = (became_by_year['total_inj'] / (became_by_year['annual_hrs'] / 2000)) * 100
    
    became_by_year = became_by_year.rename(index=str, columns={'annual_hrs': 'became_hrs','annual_coal_prod':'became_coal_prod'})
    
    nondel_hrs_prod_inj = hrs_prod_inj_mine_year[hrs_prod_inj_mine_year['rate_type'] == 'Non-delinquent']
    nondel_by_year = nondel_hrs_prod_inj.groupby('cal_yr')['annual_hrs','annual_coal_prod','non_inj'].sum()
    nondel_by_year['non_inj_rate'] = (nondel_by_year['non_inj'] / (nondel_by_year['annual_hrs'] / 2000)) * 100
    nondel_by_year = nondel_by_year.rename(index=str, columns={'annual_hrs': 'non_hrs','annual_coal_prod':'non_coal_prod'})
    
    merge_1_hrs_prod_inj = pd.merge(del_by_year, became_by_year, how='left', on=['cal_yr'], suffixes=('_d','_b'))
    merged_hrs_prod_inj = pd.merge(merge_1_hrs_prod_inj, nondel_by_year, how='left', on=['cal_yr'], suffixes=('_m','_n'))
    
    merged_hrs_prod_inj.to_csv(data_dir + 'analysis/msha_' + injury_type + '_YEAR_delinquency.csv')
    
    print(merged_hrs_prod_inj)
    
    
def get_prod_by_mine_year():
    oprtr_hrs_data = get_hours()
    mine_data = pd.read_csv(data_dir + 'msha_mine_20190209-0.csv', escapechar='\\')[['mine_id','state_abbr','c_m_ind','mine_type_cd']]
    cost_data = pd.read_csv(data_dir + 'eia_coal_prices_1217.csv')
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
                                        
    prod_mine_year = oprtr_hrs_data.groupby(['mine_id','calendar_yr'], as_index=False)['annual_coal_prod'].sum()
    prod_mine_year = prod_mine_year[prod_mine_year['calendar_yr'].isin(['2012','2013','2014','2015','2016','2017'])]
    prod_mine_year = prod_mine_year.rename(index=str, columns={'calendar_yr': 'cal_yr'})
    
    prod_mine_year_ex = pd.merge(prod_mine_year, mine_data, how='left', on=['mine_id'], suffixes=('_p','_m'))
    coal_prod_mine_year = prod_mine_year_ex[prod_mine_year_ex['c_m_ind'] == 'C']
    
    coal_prod_mine_year['earliest_date'] = coal_prod_mine_year['mine_id'].map(earliest_series)
    coal_prod_mine_year['earliest_year'] = pd.DatetimeIndex(coal_prod_mine_year['earliest_date']).year + 1    
    coal_prod_mine_year['year_type'] = coal_prod_mine_year.apply(find_rate_type, axis=1)
    
    coal_prod_mine_year_cost = pd.merge(coal_prod_mine_year,cost_data, how='left', on=['state_abbr','mine_type_cd','cal_yr'], suffixes=('_c','_dol'))
    coal_prod_mine_year_cost['est_total_income'] = coal_prod_mine_year_cost['annual_coal_prod'] * coal_prod_mine_year_cost['price_per_s_ton']
    
    coal_prod_mine_year_cost.to_csv(data_dir + 'analysis/msha_PRODUCTION_MINE_YEAR_delinquency.csv')
    
    

def get_inj_by_controller():
    inj_data = get_delinquent_injuries()
    earliest_series = delinquency_data.groupby('Controller Id')['Delinquent Date'].agg('min').to_dict()
        
    def calc_injury_rate(row):
        if row['rate_type'] == 'Delinquent':
            return (row['del_inj'] / (row['annual_hrs'] / 2000)) * 100
        else:
            return (row['non_inj'] / (row['annual_hrs'] / 2000)) * 100
                                           
    del_inj = inj_data[inj_data['delinquent'] == 1]
    del_inj_mine_year = del_inj.groupby(['controller_id'])['document_no'].count().reset_index()
    del_inj_mine_year = del_inj_mine_year.rename(index=str, columns={'document_no': 'del_inj'})
    
    non_inj = inj_data[inj_data['delinquent'] == 0]
    non_inj_mine_year = non_inj.groupby(['controller_id'])['document_no'].count().reset_index()
    non_inj_mine_year = non_inj_mine_year.rename(index=str, columns={'document_no': 'non_inj'})
        
    cntrlr_inj = pd.merge(del_inj_mine_year, non_inj_mine_year, how='left', on=['controller_id'], suffixes=('_del','_non')).fillna(0)
    
    cntrlr_inj.to_csv(data_dir + 'analysis/msha_INJURIES_CONTROLLER_delinquency.csv')        

    
def get_viol_likelihood_del_type():
    oprtr_hrs_data = get_hours()
    viol_data = pd.read_csv(data_dir + 'msha_violations_20181229_y94_y18.csv')
    
    #This is where you would filter out any violations you don't want to calculate
    #if 'cntctr_id' then violation was to a contractor
    #if 'inj_illness' then the violation was in connection to an actual injury
    #look to 'likelihood' for seriousness of violation
    
    viol_mine_lh = viol_data.groupby(['mine_id','likelihood'], as_index=False)['viol_no'].count()
    viol_mine_lh = viol_mine_lh.rename(index=str, columns={'viol_no': 'viol_count'})
    
    delinquent_mines_since = get_delinquent_since(2014)
    all_delinquent_mines = get_delinquent_mines()
    
    def all_del_mine_rate(series):
       return series.sum()/del_mine_count
       
    def all_non_del_mine_rate(series):
       return series.sum()/non_del_mine_count
        
    #All violations that happened at mines that have been delinquent since 2014
    #regardless of when the violation occurred. 
    del_viol = viol_mine_lh[viol_mine_lh['mine_id'].isin(delinquent_mines_since)]
    #Count of mines that have been delinquent since 2014, regardless of whether
    #they have violations or not
    del_mine_count = len(oprtr_hrs_data[oprtr_hrs_data['mine_id'].isin(delinquent_mines_since)]['mine_id'].unique())
    
    #All violations that happened at mines that are not currently delinquent
    non_del_viol = viol_mine_lh[~viol_mine_lh['mine_id'].isin(all_delinquent_mines)]
    #Count of mines that are not currently delinquent
    non_del_mine_count = len(oprtr_hrs_data[~oprtr_hrs_data['mine_id'].isin(all_delinquent_mines)]['mine_id'].unique())
    
    
    del_viol_lh = del_viol.groupby('likelihood').agg({'viol_count': ['sum', all_del_mine_rate]}).reset_index()
    del_viol_lh.columns = [' '.join(col).strip() for col in del_viol_lh.columns.values]
    
    non_del_viol_lh = non_del_viol.groupby('likelihood').agg({'viol_count': ['sum', all_non_del_mine_rate]}).reset_index()
    non_del_viol_lh.columns = [' '.join(col).strip() for col in non_del_viol_lh.columns.values]
    
    combo_viol = pd.merge(del_viol_lh,non_del_viol_lh, how='left', on='likelihood', suffixes=('_del2014','_nondel2014'))  
    
    combo_viol.to_csv(data_dir + 'analysis/msha_VIOLATIONS_LIKELIHOOD_delinquency.csv')  


def get_viol_likelihood_by_mine():
    viol_data = get_delinquent_viols()
    mine_data = refine_mines()
    
    likelihood_agg_dict = {
        'mine_id':'nunique',
        'Highly':'mean',
        'NoLikelihood':'mean',
        'Occurred':'mean',
        'Reasonably':'mean',
        'Unlikely':'mean'
    }
    
    likelihood_viol = pd.pivot_table(viol_data, values='viol_no', index=['mine_id','delinquent'],
                                    columns=['likelihood'], aggfunc='count').reset_index()
                                            
    del_viols = likelihood_viol[likelihood_viol['delinquent'] == 1]
    non_viols = likelihood_viol[likelihood_viol['delinquent'] == 0]
    
    #this step is imperative because we want to take from ALL MINES, not just mines that have had violations 
    mine_del = pd.merge(mine_data,del_viols, how='left', on='mine_id', suffixes=('_m','_del')).fillna(0)
    mine_viols = pd.merge(mine_del, non_viols, how='left', on='mine_id', suffixes=('_del','_non')).fillna(0)
            
    mine_viols.to_csv(data_dir + 'analysis/msha_VIOLATIONS_MINE_LIKELIHOOD_delinquency.csv')
    
    
def get_viol_likelihood_by_controller():
    viol_data = get_delinquent_viols()
    mine_data = refine_mines()
    
    likelihood_agg_dict = {
        'mine_id':'nunique',
        'Highly':'mean',
        'NoLikelihood':'mean',
        'Occurred':'mean',
        'Reasonably':'mean',
        'Unlikely':'mean'
    }
    
    likelihood_viol = pd.pivot_table(viol_data, values='viol_no', index=['controller_id','delinquent'],
                                    columns=['likelihood'], aggfunc='count').reset_index()
                                            
    del_viols = likelihood_viol[likelihood_viol['delinquent'] == 1]
    non_viols = likelihood_viol[likelihood_viol['delinquent'] == 0]
    
    #this step is imperative because we want to take from ALL MINES, not just mines that have had violations 
    cntrlr_viols = pd.merge(del_viols, non_viols, how='left', on='controller_id', suffixes=('_del','_non')).fillna(0)
            
    cntrlr_viols.to_csv(data_dir + 'analysis/msha_VIOLATIONS_CONTROLLER_LIKELIHOOD_delinquency.csv')
    
    
def get_bad_viols_per_year():
    viol_data = get_delinquent_viols()
    bad_viol_data = viol_data[viol_data['likelihood'].isin(['Highly','Occurred','Reasonably'])]
    delinquency_by_year = pd.pivot_table(viol_data, index='cal_yr', columns='delinquent', 
                                        aggfunc={'viol_no' : 'nunique', 'mine_id' : 'nunique'}).reset_index()    
    
    delinquency_by_year.to_csv(data_dir + 'analysis/msha_VIOLATIONS_SERIOUS_YEAR_delinquency.csv')
    
    
def get_mine_serious_viols_per_year():
    viol_data = get_delinquent_viols()
    bad_viol_data = viol_data[viol_data['likelihood'].isin(['Highly','Occurred','Reasonably'])]
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
    
    
    delinquency_by_year = pd.pivot_table(bad_viol_data, index='mine_id', values='viol_no', columns='cal_yr', 
                                        aggfunc='count')
        
    delinquency_by_year['delinquent_date'] = delinquency_by_year.index.map(earliest_series.get)
    
    delinquency_by_year.to_csv(data_dir + 'analysis/msha_VIOLATIONS_SERIOUS_MINE_YEAR_delinquency.csv')
    
    
def pull_justice_violations():
    viol_data = get_delinquent_viols()
    
    justice_viols = viol_data[viol_data['controller_id'].isin(['0091855','C04355'])]
    
    justice_viols.to_csv(data_dir + 'analysis/justice_VIOLATIONS.csv')
    

def pull_justice_injuries():
    inj_data = get_delinquent_injuries()
    
    justice_inj = inj_data[inj_data['controller_id'].isin(['0091855','C04355'])]
    
    justice_inj.to_csv(data_dir + 'analysis/justice_INJURIES.csv')
    
    
def pull_justice_current_mines():
    mine_data = refine_mines()
    earliest_delinquency = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
    delinquency_controller = delinquency_data.groupby('Mine ID')['Controller Name'].agg('sum').to_dict()
    
    justice_mines = mine_data[mine_data['curr_cntrlr_id'].isin(['0091855','C04355'])]
    justice_mines['earliest_date'] = justice_mines['mine_id'].map(earliest_delinquency)
    justice_mines['del_controllers'] = justice_mines['mine_id'].map(delinquency_controller)
    
    justice_mines.to_csv(data_dir + 'analysis/justice_MINES_CURRENT.csv')
    
    
def pull_justice_past_mines():
    mine_data = refine_mines()
    earliest_delinquency = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min').to_dict()
    delinquency_controller = delinquency_data.groupby('Mine ID')['Controller Name'].agg('sum').to_dict()
    
    mine_data['del_controllers'] = mine_data['mine_id'].map(delinquency_controller)
    
    justice_mines = mine_data[mine_data['del_controllers'].str.contains('Justice')==True]
    
    #justice_mines['earliest_date'] = justice_mines['mine_id'].map(earliest_delinquency)
    #justice_mines['del_controllers'] = justice_mines['mine_id'].map(delinquency_controller)
    
    justice_mines.to_csv(data_dir + 'analysis/justice_MINES_PAST.csv')


def get_fatalities():
    inj_data = get_delinquent_injuries()
    
    fatalities = inj_data[inj_data['degree_injury_cd'] == '01']
    perm_dis = inj_data[inj_data['degree_injury_cd'] == '02']
    fat_by_year = fatalities.groupby('cal_yr').agg({'document_no':'count','delinquent':'sum'})
    perm_dis_by_year = perm_dis.groupby('cal_yr').agg({'document_no':'count','delinquent':'sum'})
    
    print('Permenantly Disabled')
    print(perm_dis_by_year)
    print(perm_dis_by_year.delinquent.sum())
    
    
    #del_inj = inj_data[inj_data['delinquent'] == 1]
    #non_inj = inj_data[inj_data['delinquent'] == 0]
    #
    #print(del_inj.groupby(['inj_degr_desc','degree_injury_cd'])['mine_id'].count())
    #print(non_inj.groupby(['inj_degr_desc','degree_injury_cd'])['mine_id'].count())


pull_justice_past_mines()
#pull_justice_current_mines()
#pull_justice_violations()
#pull_justice_injuries()
#get_national_inj_rate()
#get_fatalities()
#fatalities = inj_data[inj_data['inj_degr_desc'] == 1]
#get_inj_prod_by_mine(fatalities, 'FATALITIES')
#inj_data = get_delinquent_injuries()
#injury_type = 'INJURIES'
#get_inj_prod_by_year(inj_data, injury_type)
#get_inj_prod_by_mine(inj_data, injury_type)




