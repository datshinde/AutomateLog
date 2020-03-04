import pandas as pd
import datetime as dt 

df = pd.read_csv("/Users/Shachi/Documents/log_ex", sep=' at ', header=None, engine='python', 
                 index_col=False)
df = df.rename(columns={0:'context', 1:'date'})

df[['day','datetime']] = df.date.str.split(" ", 1, expand=True)
df[['status','job_name']] = df.context.str.split(" ", 1, expand=True)
df[['job_name','job_file_name']] = df.job_name.str.split(" ", 1, expand=True)

df = df[['status', 'job_name', 'job_file_name', 'day', 'datetime']]
df["datetime"] = pd.to_datetime(df["datetime"])

df['date'] = [d.date() for d in df['datetime']]
df['time'] = [d.time() for d in df['datetime']]

df = df[['status', 'job_name', 'job_file_name', 'day', 'datetime', 'date', 'time']]

df1 = df[(df['job_name']=='failed')]


# df = df.pivot(index='status', columns='job_name', values='datetime')
df=df.replace(to_replace='5amflashsalesExec', value='5amflashsales')
df=df.replace(to_replace='dailgy files', value='daily files')
df.loc[df['job_name'] == '5amflashsales', 'job_file_name'] = 'nan'
df.loc[df['job_file_name'] == ('daily files'), 'job_name'] = 'daily files'
df.loc[df['job_file_name'] == 'put daily files', 'job_name'] = 'daily files'
df.loc[df['job_file_name'] == 'historical files', 'job_name'] = 'historical files'
df.loc[df['job_file_name'] == 'put historical files', 'job_name'] = 'historical files'


#---------------------------------------------------
df2=df[(df['status']=='starting')]
df2.sort_values(["job_name","datetime"], axis = 0, ascending = True, 
                 inplace = True, na_position ='last')

df3=df[(df['status']=='ending') | (df['status']=='successful')]
df3.sort_values(["job_name","datetime"], axis = 0, ascending = True, 
                 inplace = True, na_position ='last') 

#final=pd.merge(df2, df3, on='job_name', how='inner', indicator=True)

#ff=pd.concat([df2,df3], axis=1)
#ff=df2.merge(df3, left_index=True, right_index=True)
#ff=df2.join(df3)


#df.loc[df['job_name'] == 'ending', 'status'] = 'ending'
#------------------------------------------

idx = (df['job_name'] == 'ending')
df.loc[idx,['job_name','status']] = df.loc[idx,['status','job_name']].values

idx = (df['job_name'] == 'completed')
df.loc[idx,['job_name','status']] = df.loc[idx,['status','job_name']].values

df=df.replace(to_replace='completed', value='ending')
df=df.replace(to_replace='successful', value='ending')


df  = df[(df['status']=='starting') | (df['status']=='ending')]

#df.sort_values(["job_name"], axis = 0, ascending = True, 
#                 inplace = True, na_position ='last')

df["Date"] = df["date"].map(lambda ts: ts.strftime("%Y-%m-%d"))

Sample_df = df[(df['Date']=='2019-12-12')]

#sample_date = ('2019-03-24')
#mask = (df['date'] == sample_date)
#sample_df = df.loc[mask]

sample_strt_df=Sample_df[(Sample_df['status']=='starting')]
sample_end_df=Sample_df[(Sample_df['status']=='ending')]


final_strt=sample_strt_df.groupby('job_name').min()
final_end=sample_end_df.groupby('job_name').min()

#final_strt=pd.merge(df2, sample_strt_df, on=('job_name','time'), how='inner', indicator=False)
#final_end=pd.merge(df3, sample_end_df, on=('job_name','time'), how='inner', indicator=False)

final_df=pd.merge(final_strt, final_end, on='job_name', how='inner', indicator=False)
final_df.rename(columns = {'Index':'JobName', 'status_x':'Start_status', 'job_file_name_x':'JobFileName', 
                              'datetime_x':'start_datetime','day_x':'Day', 'date_x':'Date', 'status_y':'end_status',
                              'datetime_y':'end_datetime'}, inplace = True) 
   
final_df = final_df[[ 'JobFileName', 'Day','Date','Start_status','start_datetime', 'end_status', 'end_datetime']]


df8 = df[(df['job_name']=='daily files')] 
#dfff = df[(df['job_name']=='5amflashsalesExec')]
#-----------------------------------------------------------------------

#FMT = '%H:%M:%S'
#runtime = dt.strptime(final_df['time_y'], FMT) - dt.strptime(final_df['time_x'], FMT)
import numpy as np

final_df['runtime']= final_df['end_datetime'] - final_df['start_datetime']
final_df['runtime']=final_df['runtime']/np.timedelta64(1,'m')


 

