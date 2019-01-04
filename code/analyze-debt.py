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
import datetime, glob, os


data_dir = '../data/'
now = datetime.datetime.now()

#accident_data = pd.read_csv(data_dir + 'msha_accident_20181229-0.csv')
#violation_data = pd.read_csv(data_dir + 'msha_violations_20181229_y15_y18.csv')

# Compare injury rates at delinquent and non-delinquent mines,
# include delinquent mines pre delinquency in non-delinquent category

#################################################################
# HELPER FUNTIONS
#################################################################

def question_data():
    print(delinquency_data['Delinquent Date'].min())
    print(delinquency_data['Delinquent Date'].max())
    
def combine_segment_violations():
    #combine
    violations_dir = data_dir + 'msha_violation_20181229.csv'
    files = glob.glob(os.path.join(violations_dir, "*.csv"))
    
    all_violations = pd.concat((pd.read_csv(f) for f in files))
    
    #segment by date
    rel_violations = all_violations[(all_violations['mine_id'] == 1512685) | (all_violations['iss_dt'] > '1993-12-31')]
    rel_violations.to_csv(data_dir + 'msha_violations_20181229_y94_y18.csv')

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
    
    
#################################################################    
    
delinquency_data = pd.read_csv(data_dir + 'debtbyage_20181205_REFINED.csv')

def get_delinquent_mines():    
    return delinquency_data['Mine ID'].unique()

    
# 60% of mines have more than one active delinquency. Grab the earliest Delinquent Date    
def get_delinquent_mine_dates():
    earliest_delinquency = delinquency_data.groupby('Mine ID')['Delinquent Date'].agg('min')
    
    return earliest_delinquency.to_dict()


def injury_rates():
    accident_data = pd.read_csv(data_dir + 'msha_accident_20181229_y94_y18.csv', escapechar='\\')
            
    print(accident_data.ai_dt.min())
    print(accident_data.ai_dt.max())
    
    #issue_rows = []
    #
    #injury_data = accident_data[accident_data.no_injuries > 0]
    #
    #delinquent_mines = get_delinquent_mines() #['mine_id','mine_id','mine_id'...]
    #delinquent_mine_dates = get_delinquent_mine_dates() #{'mine_id':'delinquncy_start','mine_id':'delinquncy_start',...}
    #
    #delinquent_accident = injury_data[
    #                        (injury_data['mine_id'].isin(delinquent_mines)) & 
    #                        (injury_data['ai_dt'] > delinquent_mine_dates[injury_data['mine_id']])
    #                    ]
    #non_delinquent_accident = []
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
            
#segment_accidents()           
#injury_rates()  
get_violations()
#combine_segment_violations()
    
    
    