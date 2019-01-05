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
    
    
#################################################################    
    
delinquency_data = pd.read_csv(data_dir + 'debtbyage_20181205_REFINED.csv')

def test_oprtr_data():
    oprtr_hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20181229-0.csv')
    
    #by_mine_id = oprtr_hrs_data.groupby('mine_id').agg('count')
    by_mine_id = pd.pivot_table(oprtr_hrs_data, values='annual_hrs', index=['mine_id'], columns='calendar_yr', aggfunc=np.sum)
    
    mine_dict = by_mine_id.to_dict('index')
    
    mine_list=list(mine_dict.values())
    print(mine_list[0])    
    #by_mine_id.to_csv(data_dir + 'test/oprtr-mine-id-test.csv')


def get_delinquent_mines():    
    return delinquency_data['Mine ID'].unique()

    
# 60% of mines have more than one active delinquency. Grab the earliest Delinquent Date    
def get_delinquent_mine_dates():
    earliest_series = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min')
    
    earliest_df = pd.DataFrame({'mine_id':earliest_series.index, 'earliest_date':earliest_series.values})
    earliest_df['earliest_date'] =  pd.to_datetime(earliest_df['earliest_date'], format='%m/%d/%Y')
    
    #Need to round earliest_date up to the next year so we can factor only years with full delinquency
    earliest_df['earliest_year'] = earliest_df['earliest_date'].map(lambda x: x.year+1)
    delinquency_refined = earliest_df[['mine_id','earliest_year']]
    delinquency_dict = delinquency_refined.set_index('mine_id').to_dict()
    
    return delinquency_dict['earliest_year']
    
    #return delinquency_dict['earliest_year']
    

def injury_rates():
    accident_data = pd.read_csv(data_dir + 'msha_accident_20181229_y94_y18.csv', escapechar='\\')
    oprtr_hrs_data = pd.read_csv(data_dir + 'msha_cy_oprtr_emplymnt_20181229-0.csv')
    inj_data = accident_data[accident_data.no_injuries > 0]
    
    inj_mine_year = pd.pivot_table(inj_data, values='document_no', index=['mine_id'], columns='cal_yr', aggfunc='count')
    hrs_mine_year = pd.pivot_table(oprtr_hrs_data, values='annual_hrs', index=['mine_id'], columns='calendar_yr', aggfunc=np.sum)
    
    inj_hrs_mine_year = pd.merge(inj_mine_year, hrs_mine_year, on='mine_id', suffixes=('_inj', '_hrs'))
    
    def calc_injury_rate(inj, hrs):
        return (inj / (hrs / 2000)) * 100
    
    inj_hrs_mine_year['1994_FTE'] = np.vectorize(calc_injury_rate)(inj_hrs_mine_year['1994_inj'], inj_hrs_mine_year['1994_hrs'])
    inj_hrs_mine_year['1995_FTE'] = np.vectorize(calc_injury_rate)(inj_hrs_mine_year['1995_inj'], inj_hrs_mine_year['1995_hrs'])
    inj_hrs_mine_year['1996_FTE'] = np.vectorize(calc_injury_rate)(inj_hrs_mine_year['1996_inj'], inj_hrs_mine_year['1996_hrs'])
    inj_hrs_mine_year['1997_FTE'] = np.vectorize(calc_injury_rate)(inj_hrs_mine_year['1997_inj'], inj_hrs_mine_year['1997_hrs'])
    inj_hrs_mine_year['1998_FTE'] = np.vectorize(calc_injury_rate)(inj_hrs_mine_year['1998_inj'], inj_hrs_mine_year['1998_hrs'])
    
    
    inj_hrs_mine_year.to_csv(data_dir + 'test/inj-hrs-mine-year.csv')
    
    #delinquent_mines = get_delinquent_mines() #['mine_id','mine_id','mine_id'...]
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
injury_rates()  
#get_violations()
#combine_segment_violations()
    
    
    