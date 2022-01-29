import psycopg2
import pandas.io.sql as sqlio
import datetime
import pandas as pd
from datetime import datetime

conn = psycopg2.connect("dbname='postgres' user='amedvedeva' host='135.181.61.116' password='JhnbgLrt@345nbvYukfbg^739cdsg'")

# IEO name, month and year of start of this IEO
ieos_info_qwr = """
select name, started
from view_crowdsale_manager_crowdsale
where status != 'REGISTERED';
"""
ieos_info = sqlio.read_sql_query(ieos_info_qwr, conn) 
ieos_info['started'] = ieos_info['started'].apply(lambda x: str(x).split(' ')[0])

ieos_data = pd.read_excel('IEO.xlsx')
ieos_info = ieos_info.merge(ieos_data, how = 'left', on = 'name')

# Aggregated number of IEOs with status = ACTIVE
active_ieos_qwr = """
select count(distinct name)
from view_crowdsale_manager_crowdsale
where status = 'ACTIVE';
"""
active_ieos = sqlio.read_sql_query(active_ieos_qwr, conn)

# Aggregated number of IEOs with status = FINISHED
finished_ieos_qwr = """
select count(distinct name)
from view_crowdsale_manager_crowdsale
where status = 'FINISHED';
"""
finished_ieos = sqlio.read_sql_query(finished_ieos_qwr, conn)

# Aggregation of traders participated in each IEO
traders_ieos_qwr = """
select count(distinct deal.user) as traders,
       ieo.name
from view_crowdsale_manager_crowdsale as ieo
join view_crowdsale_manager_deal as deal
on ieo.id::uuid = deal.crowdsale_id
where ieo.status != 'REGISTERED'
group by ieo.name;
"""
traders_ieos = sqlio.read_sql_query(traders_ieos_qwr, conn)
ieos_info = ieos_info.merge(traders_ieos, how = 'left', on = 'name')
ieos_info.to_excel('ieo_info.xlsx')

# Retention of traders after you identify who participated in which IEO
retention_qwr = """
select deal.user as trader,
       user_info.__create_date as registered,
       ieo.name as ieo
from view_crowdsale_manager_crowdsale as ieo

join view_crowdsale_manager_deal as deal
on ieo.id::uuid = deal.crowdsale_id

join view_user_manager_user as user_info
on user_info.id = deal.user
where ieo.status != 'REGISTERED'
group by registered, trader, ieo;
"""
retention_info = sqlio.read_sql_query(retention_qwr, conn)

id_ieo_table = retention_info[['trader', 'registered']].drop_duplicates(subset='trader')
ieo_groups = retention_info[['ieo', 'trader']].groupby('ieo')

for ieo, traders in ieo_groups:
    project_start_date = datetime.strptime(ieos_info[ieos_info['name'] == ieo]['started'].values[0],'%Y-%m-%d')
    
    #for index, row in traders.iterrows():
    #    if row['registered'] > project_start_date.date():
    #        print('ERR!')
            
    id_ieo_table = id_ieo_table.merge(traders, how = 'left', on = 'trader')
    id_ieo_table.rename(columns={'ieo':ieo}, inplace=True)
    id_ieo_table[ieo] = id_ieo_table[ieo].apply(lambda x: True if x == ieo else False)
    
id_ieo_table.to_excel('id_ieo.xlsx')
len(ieo_groups)

ieo_list = []
traders_list = []
for ieo, traders in ieo_groups:
    ieo_list.append(ieo)
    traders_list.append(len(traders))
    
new_df = pd.DataFrame()
new_df['name'] = ieo_list
new_df['traders'] = traders_list


