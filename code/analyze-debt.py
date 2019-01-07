# What is happening at mines after they become delinquent?
# -- accidents, deaths and injuries
# -- violations
# -- production

# Output
# - Compare injury rates at delinquent and non-delinquent mines,
#   include delinquent mines pre delinquency in non-delinquent category
# - Does a mine's injury rate change once they become delinquent?
# - Pivot by Age Category
# - Bracket by Ending Balance

# You're going to need to use the DebtByAge data, columns Mine ID and Delinquent Date
# and the msha_operator_history data, columns mine_id, oper_start_dt and oper_end_dt
# in order to attach the correct operator to the the delinquency.

# Initial processes for Delinquency Data
# - Filter only operators
# - Remove all delinquencies of 90 days and less
# - Remove all delinquencies in Hold Status

# Step 1: Attach operator and controller at time of delinquency to the delinquency

# Step 2: Pull accidents at each mine after Delinquent Date (plus 90 days or filter
# out delinquencies that are less than 90 days old)

# Step 3: Pull violations at each mine after Delinquent Date

# Step 4: Pull production numbers since Delinquent Date

import pandas as pd
import numpy as np
import datetime, glob, os


data_dir = '../data/'
now = datetime.datetime.now()


#################################################################
# HELPER FUNTIONS
#################################################################

    
def combine_segment_violations():
    #combine
    violations_dir = data_dir + 'msha_violation_20181229.csv'
    files = glob.glob(os.path.join(violations_dir, "*.csv"))
    
    all_violations = pd.concat((pd.read_csv(f) for f in files))
    
    #segment by date
    rel_violations = all_violations[(all_violations['mine_id'] == 1512685) | (all_violations['iss_dt'] > '1993-12-31')]
    rel_violations.to_csv(data_dir + 'msha_violations_20181229_y94_y18.csv')
    
    #test date segmentation
    print(rel_violations.iss_dt.min())
    print(rel_violations.iss_dt.max())

def get_refined_delinquencies():
    delinquency_data = pd.read_csv(data_dir + 'DEBTAGE12052018_DET2.csv')
    
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
       
    accident_data = pd.read_csv(data_dir + 'msha_accident_20181229-0.csv', escapechar='\\')
    accident_data['ai_dt'] =  pd.to_datetime(accident_data['ai_dt'], format='%d-%m-%Y')
    
    rel_accidents = accident_data[(accident_data['mine_id'] == 1512685) | (accident_data['ai_dt'] > '1993-12-31')]
    rel_accidents.to_csv(data_dir + 'msha_accident_20181229_y94_y18.csv')
    
    #test date segmentation
    print(rel_accidents.ai_dt.min())
    print(rel_accidents.ai_dt.max())
    
    
def segment_hrs():  
       
    hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20181229-0.csv')
    
    rel_hrs = hrs_data[hrs_data['calendar_yr'] > 1993]
    rel_hrs.to_csv(data_dir + 'msha_cy_oprtr_emplymnt_20181229_y94_y18.csv')
    
    #test date segmentation
    print(rel_hrs.calendar_yr.min())
    print(rel_hrs.calendar_yr.max())
    
    
def test_oprtr_data():
    oprtr_hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20181229-0.csv')
    
    #by_mine_id = oprtr_hrs_data.groupby('mine_id').agg('count')
    by_mine_id = pd.pivot_table(oprtr_hrs_data, values='annual_hrs', index=['mine_id'], columns='calendar_yr', aggfunc=np.sum)
    
    mine_dict = by_mine_id.to_dict('index')
    
    mine_list=list(mine_dict.values())
    print(mine_list[0])    
    #by_mine_id.to_csv(data_dir + 'test/oprtr-mine-id-test.csv')
    
    
#################################################################    
    
delinquency_data = pd.read_csv(data_dir + 'debtbyage_20181205_REFINED.csv')
delinquency_data['Delinquent Date'] =  pd.to_datetime(delinquency_data['Delinquent Date'], format='%m/%d/%Y')

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
def get_delinquent_mine_dates():
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min')
    
    earliest_df = pd.DataFrame({'mine_id':earliest_series.index, 'earliest_date':earliest_series.values})
    
    #Need to round earliest_date up to the next year so we can factor only years with full delinquency
    earliest_df['earliest_year'] = earliest_df['earliest_date'].map(lambda x: x.year+1)
    
    return earliest_df
            

def injury_rates():
    oprtr_hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20181229_y94_y18.csv')
    accident_data = pd.read_csv(data_dir + 'msha_accident_20181229_y94_y18.csv', escapechar='\\')
    inj_data = accident_data[accident_data.no_injuries > 0]
    mine_delinquencies = get_delinquent_mine_dates()
    
    hrs_mine_year = oprtr_hrs_data.groupby(['mine_id','calendar_yr'], as_index=False)['annual_hrs'].agg(np.sum)
    hrs_mine_year = hrs_mine_year.rename(index=str, columns={'mine_id': 'mine_id', 'calendar_yr': 'cal_yr', 'annual_hrs':'annual_hrs'})
    
    inj_mine_year = inj_data.groupby(['mine_id','cal_yr'], as_index=False)['document_no'].agg('count')
    inj_mine_year = inj_mine_year.rename(index=str, columns={'mine_id': 'mine_id', 'cal_yr': 'cal_yr', 'document_no':'injuries'})
    
    hrs_inj_mine_year = pd.merge(hrs_mine_year, inj_mine_year, how='left', on=['mine_id','cal_yr'])
    hrs_inj_del_mine_year = pd.merge(hrs_inj_mine_year, mine_delinquencies, how='left', on=['mine_id'])
    
    #grouped = hrs_inj_del_mine_year.groupby(['mine_id'])['cal_yr'].agg('count')
    #grouped.to_csv(data_dir + 'test/testing-mine-years.csv')
    
    def calc_injury_rate(row):
        return (row['injuries'] / (row['annual_hrs'] / 2000)) * 100
        
    def find_rate_type(row):
        if row['cal_yr'] >= row['earliest_year']:
            return 'Delinquent'
        else:
            return 'Non-delinquent'
            
    delinquent_mines_since = get_delinquent_since(2014)
    all_delinquent_mines = get_delinquent_mines()
    
    #These would be all mines that have been delinquent since 2014, with data for annual hours and number
    #of injuries sustained by year
    del_rates = hrs_inj_del_mine_year[hrs_inj_del_mine_year['mine_id'].isin(delinquent_mines_since)]
    
    #These would be all mines that are not currently delinquent. We do not have the data to state that they
    #never were delinquent. Only that if they had a delinquency in the past 5 years, they have since paid it.
    non_del_rates = hrs_inj_del_mine_year[~hrs_inj_del_mine_year['mine_id'].isin(all_delinquent_mines)]
        
    del_rates_year = del_rates.groupby(['cal_yr'])['injuries','annual_hrs'].agg(np.sum)
    del_rates_year['injury_rate'] = del_rates_year.apply(calc_injury_rate, axis=1)
    
    non_del_rates_year = non_del_rates.groupby(['cal_yr'])['injuries','annual_hrs'].agg(np.sum)
    non_del_rates_year['injury_rate'] = non_del_rates_year.apply(calc_injury_rate, axis=1)
    
    combo_rates = pd.merge(del_rates_year,non_del_rates_year, how='left', on='cal_yr', suffixes=('_del2014','_nondel2014'))
    
    
    combo_rates.to_csv(data_dir + 'analysis/rates-del-nondel-since-2014.csv')
    
    
    
    
    
    
    #del_rates.to_csv(data_dir + 'analysis/delinquent-mines-since-2014.csv')
    #non_del_rates.to_csv(data_dir + 'analysis/non-delinquent-mines-since-2014.csv')
        
    #inj_hrs_del_mine_year['rate_type'] = inj_hrs_del_mine_year.apply(find_rate_type, axis=1)
    #inj_hrs_del_mine_year['injury_rate'] = inj_hrs_del_mine_year.apply(calc_injury_rate, axis=1)
    #
    #avg_inj_rate = inj_hrs_del_mine_year.groupby(['rate_type'])['injury_rate'].median()
    #
    #print(avg_inj_rate)
    #inj_hrs_del_mine_year.to_csv(data_dir + 'test/inj-hrs-del-mine-year.csv')
    
def get_mines_delinquent_since():
    
    delinquent_mines_since = get_delinquent_mines(2014) #['mine_id','mine_id','mine_id'...]
    #delinquent_mine_dates = get_delinquent_mine_dates() #{'mine_id':'delinquncy_start','mine_id':'delinquncy_start',...}
    #
    #is_delinquent = inj_data['mine_id'].isin(delinquent_mines)
    #in_timeframe = inj_data['ai_year'] >= injury_data['mine_id'].map(delinquent_mine_dates)
    #
    #delinquent_inj = inj_data[is_delinquent & in_timeframe]
    #non_delinquent_inj = inj_data[(is_delinquent & ~in_timeframe) | ~is_delinquent]
    #
    #del_inj_by_mine_year = pd.pivot_table(delinquent_inj, values='document_no', index=['mine_id'], columns='cal_yr', aggfunc='count')
    #non_inj_acc_by_mine_year = pd.pivot_table(non_delinquent_inj, values='document_no', index=['mine_id'], columns='cal_yr', aggfunc='count')
    #
    #del_inj_by_mine_year.to_csv(data_dir + 'test/delinquent-inj-year.csv')
    #non_inj_acc_by_mine_year.to_csv(data_dir + 'test/non-delinquent-inj-year.csv')
    
    # Note here that my count of injuries at delinquent mines since 1994 is very similar to what NPR got in 2014: 
    # - this analysis: 3,868
    # - NPR 2014: 3,894
    # Unless a bunch of orgs have payed back their delinquent debts, the number from this analysis should be higher
    # You should do an Age Category analysis to see if a substantial number of these delinquencies are few than 4
    # years old. 
    
    #Now we need to calculate injury rates per mine, disregarding years where the mine was part delinquent, part non-delinquent
    # Injury rate = (Injuries / FTE ) * 100 
    # Fulltime equivalent workers = total hrs worked at mine / 2000
    
    #delinquent_accident.to_csv(data_dir + 'msha_delinquent_injuries_y94_y18.csv')
    #non_delinquent_accident.to_csv(data_dir + 'msha_delinquent_injuries_y94_y18.csv')
    
    #        
    #print(len(delinquent_accident))
    #print(len(non_delinquent_accident))
    
    
    
    #print(accident_data.info())
    
    #for accident in accidents:
    #    if row['no_injuries'] > 0:
    #        try:
    #            if row['ai_dt'] > delinquent_mine_dates[row['mine_id']]:
    #                delinquent_accident.push(row)
    #            
    #        except IndexError:
    #            non_delinquent_accident.push(row)
    
def get_violations():
    violation_data = pd.read_csv(data_dir + 'msha_violations_20181229_y94_y18.csv')
            
    print(violation_data.iss_dt.min())
    print(violation_data.iss_dt.max())

#get_delinquent_mine_dates()
#test_oprtr_data()            
#segment_accidents() 
#segment_hrs()          
injury_rates()  
#get_violations()
#combine_segment_violations()
    
    
    