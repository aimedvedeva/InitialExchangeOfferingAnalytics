import psycopg2
import pandas.io.sql as sqlio
import datetime
import pandas as pd
from datetime import datetime
import exchange 

conn = psycopg2.connect("dbname='postgres' user='' host='' password=''")

qwr = """
select ieo.name,
       deal.cost as ieo_volume,
       deal.fee as ieo_fee,
       deal.__update_date as date,
       purchase_cur.tag as quote_tag
from view_crowdsale_manager_deal deal
join view_crowdsale_manager_crowdsale ieo
    on ieo.id::uuid = deal.crowdsale_id
join view_asset_manager_currency as purchase_cur
    on deal.purchase_currency = purchase_cur.id
where ieo.status != 'REGISTERED'
and deal.status != 'REJECTED'
order by date;
"""

fees_volumes_ieos = sqlio.read_sql_query(qwr, conn)
fees_volumes_ieos = exchange.convert_to_USDT(fees_volumes_ieos, columns=['ieo_volume', 'ieo_fee'])
fees_volumes_ieos.drop(columns=['ieo_volume', 'ieo_fee', 'date', 'quote_tag'], inplace=True)
version_to_print = fees_volumes_ieos.groupby('name').agg({'ieo_volume_USDT':sum, 'ieo_fee_USDT':sum})
version_to_print.to_excel('ieos_fees_volumes.xlsx')
