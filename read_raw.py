import pandas as pd
df = pd.read_csv('C:\\Users\\Dattarya\\Desktop\\Logs.txt', sep=' at ', header=None, engine='python')
df = df.rename(columns={0:'context', 1:'date'})

df[['day','datetime']] = df.date.str.split(" ", 1, expand=True)

df[['status','job_name']] = df.context.str.split(" ", 1, expand=True)
df[['job_name','job_file_name']] = df.job_name.str.split(" ", 1, expand=True)

df = df[['status', 'job_name', 'job_file_name', 'day', 'datetime']]

df["datetime"] = pd.to_datetime(df["datetime"])

 #  idx=(df['job_name']=='ending')
   # df.loc[idx,['job_name','Job_file_name']]=df.loc[idx,[df.job_file_name.str.split(" ",1,expand=True)]]

idx = (df['status'] == 'completed')
df.loc[idx,'status'] = 'ending'

idx = (df['job_name'] == 'ending')
df.loc[idx,['job_name','status']] = df.loc[idx,['status','job_name']].values

idx = (df['job_name'] == 'completed')
df.loc[idx,['job_name','status']] = df.loc[idx,['status','job_name']].values

idx = (df['job_name'] == 'failed')
df.loc[idx,['job_name','status']] = df.loc[idx,['status','job_name']].values

#df.at[(df['job_name']=='completed' | df['job_name']=='failed'),df['status']]=df[[df['job_name'],df['status']]]
#df  = df[(df['status']=='starting') | (df['status']=='ending')]



df["formatted_col"] = df["datetime"].map(lambda ts: ts.strftime("%Y-%m-%d"))

#dt=pd.to_datetime('2019-03-24')
#df_sample=df[(df['datetime']==dt)]
#df['datetime'].eq(dt)
df.status.unique()
df.job_name.unique()






#########################
df_starting = df[(df['status']=='starting')]
df_ending = df[(df['status']=='ending') | (df['status']=='successful')]

# df = df.pivot(index='status', columns='job_name', values='datetime')
