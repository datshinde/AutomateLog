import pandas as pd
import datetime as dt 
import numpy as np


###loading file into csv format
df = pd.read_csv("/Users/Shachi/Documents/sampleLog", sep=' at ', header=None, engine='python', 
                 index_col=False)
df = df.rename(columns={0:'context', 1:'date'})

###Defining columns
df[['day','datetime']] = df.date.str.split(" ", 1, expand=True)
df[['status','job_name']] = df.context.str.split(" ", 1, expand=True)
df[['job_name','job_file_name']] = df.job_name.str.split(" ", 1, expand=True)
df = df[['status', 'job_name', 'job_file_name', 'day', 'datetime']]

###converting date into datetime format
df["datetime"] = pd.to_datetime(df["datetime"])

df['date'] = [d.date() for d in df['datetime']]
df['time'] = [d.time() for d in df['datetime']]

###Final columns
df = df[['status', 'job_name', 'job_file_name', 'day', 'datetime', 'date', 'time']]

###Seperating out failed jobs
failed_jobs = df[(df['job_name']=='failed')]

###Claening and matching jobnames by replacing ans swaping values
df=df.replace(to_replace='5amflashsalesExec', value='5amflashsales')
df.loc[df['job_file_name'] == 'daily files', 'job_name'] = 'daily files'
df.loc[df['job_file_name'] == 'put daily files', 'job_name'] = 'daily files'
df.loc[df['job_file_name'] == 'historical files', 'job_name'] = 'historical files'
df.loc[df['job_file_name'] == 'put historical files', 'job_name'] = 'historical files'

#idx = (df['job_name'] == 'ending')
df.loc[(df['job_name'] == 'ending'),['job_name','status']] = df.loc[(df['job_name'] == 'ending'),['status','job_name']].values
#idx = (df['job_name'] == 'completed')
df.loc[(df['job_name'] == 'completed'),['job_name','status']] = df.loc[(df['job_name'] == 'completed'),['status','job_name']].values

df=df.replace(to_replace='completed', value='ending')
df=df.replace(to_replace='successful', value='ending')

###To obtain starting and ending jobs
df  = df[(df['status']=='starting') | (df['status']=='ending')]

###Conterving daye to string to filter 
df["Date"] = df["date"].map(lambda ts: ts.strftime("%Y-%m-%d"))
#Sample_df = df[(df['Date']=='2019-11-05')]


###Seperating starting and ending data
sample_strt_df=df[(df['status']=='starting')]
sample_end_df=df[(df['status']=='ending')]

###finding unique start and end time foe each day
mstrt_df=sample_strt_df.groupby(['job_name', 'Date']).min()
m_end_df=sample_end_df.groupby(['job_name', 'Date']).min()

###Final start time data
final_strt=pd.merge(mstrt_df, sample_strt_df, on=(['job_name','datetime']), how='inner', indicator=False)
final_strt.rename(columns = { 'status_x':'status','job_file_name_x':'job_file_name', 'date_x':'date','day_x':'day', 'time_x':'time'}, inplace = True) 
final_strt = final_strt[[ 'status','job_name','job_file_name','day','Date','time','datetime']]

###final end time data
final_end=pd.merge(m_end_df, sample_end_df, on=(['job_name','datetime']), how='inner', indicator=False)
final_end.rename(columns = { 'status_x':'status', 'date_x':'date','day_x':'day', 'time_x':'time'}, inplace = True) 
final_end = final_end[[ 'status','job_name','day','Date','time','datetime']]

###merging start and end time data to get final output
final_df=pd.merge(final_strt, final_end, on=['job_name', 'Date'], how='inner', indicator=False)
final_df.rename(columns = {'job_name':'JobName','job_file_name':'JobFileName', 'status_x':'Start_status', 'job_file_name_x':'JobFileName', 
                              'datetime_x':'start_datetime','day_x':'Day', 'status_y':'end_status',
                              'datetime_y':'end_datetime'}, inplace = True) 

###Adding default columns

final_df['Internal_SLA'] = '05:45:00'
final_df.loc[final_df['JobName'] == '5amflashsales', 'Internal_SLA'] = '04:45:00'
final_df.loc[final_df['JobName'] == 'middaysales', 'Internal_SLA'] = '15:45:00'

final_df['Internal_SLA']=pd.to_datetime(final_df['Date'] + ' ' + final_df['Internal_SLA'], dayfirst=True) 
final_df['Deviation']= final_df['end_datetime'] - final_df['Internal_SLA']
final_df['Deviation']=round(final_df['Deviation']/np.timedelta64(1,'m'), 4)
final_df['Internal_SLA'] = final_df['Internal_SLA'].dt.time
#
#final_df['Internal_SLA']=pd.to_datetime(final_df['Internal_SLA'], format='%H:%M:%S')
#final_df['Internal_SLA'] = final_df['Internal_SLA'].dt.time

final_df['SLA'] = '06:00:00'
final_df.loc[final_df['JobName'] == '5amflashsales', 'SLA'] = '05:00:00'
final_df.loc[final_df['JobName'] == 'middaysales', 'SLA'] = '16:00:00'

final_df['Pipeline'] = 'Sales'
final_df.loc[final_df['JobName'] == '5amflashsales', 'Pipeline'] = 'Flashsales'
   

final_df.sort_values(["Date","JobName"], axis = 0, ascending = True, inplace = True, na_position ='last')


###Calculating runtime 
final_df['runtime']= final_df['end_datetime'] - final_df['start_datetime']
final_df['runtime']=round((final_df['runtime']/np.timedelta64(1,'m')), 4)


final_df = final_df[[ 'Pipeline', 'JobName','JobFileName','Day','Date','start_datetime', 'end_datetime','runtime','Internal_SLA', 'SLA','Deviation']]


###Calculating aggregated values for runtime
maxi=round(final_df.groupby( 'JobName').max()['runtime'],4)
mini=round(final_df.groupby( 'JobName').min()['runtime'],4)
average=round(final_df.groupby('JobName').mean()['runtime'],4)

###combing all aggregate values 
aggregate=pd.concat([maxi, mini, average], axis=1)
aggregate.columns =['max_runtime', 'min_runtime', 'avg_runtime'] 

agg_merge=pd.merge(final_df, aggregate, on=(['JobName']), how='inner', indicator=False)
agg_merge.sort_values(["Date","JobName"], axis = 0, ascending = True, inplace = True, na_position ='last')

 

####Converting and exporting file into csv
agg_merge.to_csv(r'/Users/Shachi/Documents/LogFiles.csv', index=False)
#aggregate.to_csv(r'/Users/Shachi/Documents/AggregateFiles.csv')
failed_jobs.to_csv(r'/Users/Shachi/Documents/Failed_jobFiles.csv', index=False)


