import pandas as pd
import numpy as np
import datetime
import pyodbc as db
import os
import glob
from dateutil.relativedelta import relativedelta
from Parameter1 import A as A
from Parameter1 import B as B
from Parameter1 import writeA as WA
from Parameter1 import writeB as WB
#from Email import send_mail as email
from Parameter1 import Employee_Profile as EP
from Parameter1 import COVID_APP as COVID
from openpyxl import load_workbook

start_datetime = datetime.datetime.now()
print (start_datetime,'execute')
today = datetime.datetime.now().strftime('%Y-%m-%d')

################################################  STEP 1 ########################################################
# Query Confirm Case from DATBASE
'''
"""database connection ={SQL Server Native Client 11.0};"""
conn = db.connect('Driver={SQL Server Native Client 11.0};'
                    'Server=SBNDCTSREMP;'	
                    'Database=TB_SR_Employee;'
                    'Trusted_Connection=yes;')
cursor = conn.cursor()
dfout = pd.DataFrame(columns=['No','Employee ID', 'Status','Confirm_DateTime'])
SQL = """ 
SELECT [No]
      ,[EmployeeId]
      ,[Status]
      ,[Confirm_DateTime]
      ,[Status_Confirm]
  FROM [TB_SR_Employee].[dbo].[TRACE_Covid_Confirm_Status]
  where [Trace_Flg]=0 and Status_Confirm='A'
  ---where [No]=478
""" 
cursor.commit()
#print(SQL)
cursor.execute(SQL)
data_Out = cursor.fetchall()
for row in data_Out:
    newrow= {'No':row[0],'Employee ID':row[1],'Status':row[2],'Confirm_DateTime':row[3],'Status_Confirm':row[4]}
    dfout = dfout.append(newrow, ignore_index=True)
data_In = dfout
print(data_In)

Status=data_In.values[0][4]
confrim_dateTime = data_In.values[0][3]
case = len(data_In)
'''

# Manaul Input
#แบบ 3 กรอก Employee ID เอง
Employee_ID = {
              'No':['1']
              ,'Employee ID': ['11031811']
              ,'Status':['A']
              ,'Confirm_DateTime':['2021-09-20']
              ,'Status_Confirm':['A']
              }
data_In = pd.DataFrame(Employee_ID, columns = ['No','Employee ID','Status','Confirm_DateTime','Status_Confirm'])
Status=data_In.values[0][2]
confrim_dateTime = data_In.values[0][3]
case = len(data_In)

'''
#แบบ 4 excel file
Input_excel = r'./TEMP/Inputdata.xlsx'
data_In= pd.read_excel(Input_excel)
Status=data_In.values[0][2]
confrim_dateTime = data_In.values[0][3]
case = len(data_In)
print(data_In)
'''
################################################  STEP 2 ########################################################
# Query data
try:
    # Query Timeline
    Input_external = r'./TEMP/Input_ExternalTimeline/EXTERNAL_TIMELINE.xlsx'
    dfoutA = pd.DataFrame(columns=['Confirm_ID','STATUS','Employeeid','Location_Datetime','DATE','latitude','longitude','RUN_TIME','Source','LocationName','MAP'])
    Output_PathA = r'./TEMP/Append/Append_TIMELINE.xlsx'
    dfobj = pd.DataFrame(data_In)
    df_outA = A(dfobj,dfoutA,Output_PathA,Input_external)

    # Query Network
    dfoutB = pd.DataFrame(columns=['ConfirmID','TRACE_DATE','FROM_EMPID','FROM_STATUS','FROM_LAT','FROM_LONG','FROM_LOCATION_DATE','TO_EMPID','TO_LAT','TO_LONG','TO_LOCATION_DATE','TO_LOCATION_NM','FROM_LOCATION_NM'])
    Input_PathB = r'./TEMP/Append/Append_TIMELINE.xlsx'
    Output_PathB = r'./TEMP/Treaceability'+str(today)+'.xlsx'
    df_outB = B(dfoutB,Input_PathB,Output_PathB)
except IndexError as Error:
    print(Error)

    
################################################  STEP 3 ########################################################

#### 3.1 PREP DATA #####
# Employee 
df_EP = EP() 
# COVID_Status
df_COVID = COVID()
print(df_COVID)
#### 3.2 Lookup value COVID_Status #####
df_join1 = df_outB.merge(df_COVID, how='left', left_on='TO_EMPID', right_on='EC_EMPID')
#### 3.3 Lookup value Employee #####
df_join2 = df_join1.merge(df_EP, how='left', left_on='TO_EMPID', right_on='EmployeeId')
df_join3 = df_join2.merge(df_EP, how='left', left_on='FROM_EMPID', right_on='EmployeeId')
#### 3.4 TRANSFORM DATA #####
#DROP COL.
df_drop = df_join3.drop(['EC_EMPID','TRACE_DATE','EmployeeId_x','EmployeeId_y','CompanyName_y','GroupBU_y','ContactPhone_y','PGBU_y','LEVEL_y'], axis=1)
#df_drop.to_excel(r'./TEMP/Append/DF_DROP.xlsx',index=False)
#RENAME COL.
df_drop.rename(columns = {'ConfirmID':'ConfirmID','FROM_EMPID':'FROM_EMPID','FROM_STATUS':'FROM_STATUS','FROM_LAT':'FROM_LAT','FROM_LONG':'FROM_LONG','FROM_LOCATION_DATE':'FROM_LOCATION_DATE','TO_EMPID':'EMPLOYEEID','TO_LAT':'TO_LAT','TO_LONG':'TO_LONG','TO_LOCATION_DATE':'LOCATION_DATE','TO_LOCATION_NM':'LOCATION_NAME','FROM_LOCATION_NM':'FROM_LOCATION_NM','COVID_APP_STATUS':'COVID_STATUS','COVID_APP_DATETIME':'COVID_DATETIME','FullName_x':'FULLNAME', 'CompanyName_x':'COMPANYNAME', 'GroupBU_x':'GROUPBU','ContactPhone_x':'CONTACTPHONE','FullName_y':'FROM_FULLNAME','PGBU_x':'PGBU','LEVEL_x':'LEVEL'}, inplace = True) 
#df_drop.to_excel(r'./TEMP/Append/DF_DROP1.xlsx',index=False)
#REARRAGE COL.
order = [0,1,20,2,3,4,11,5,6,14,15,16,17,7,8,9,10,12,13,18,19] # Rearrage col
df_drop = df_drop[[df_drop.columns[i] for i in order]]
#Replace ไม่ประเมินวันนี้เฉพาะ col. Covid_status
df_drop['COVID_STATUS'] = df_drop['COVID_STATUS'].fillna('ไม่ประเมินวันนี้')

################################################  STEP 4 ########################################################
# Pivot TABLE
#TIMELINE
pivot = df_outA.pivot_table(df_outA,index=["Employeeid","DATE","LocationName","MAP"])
pivot4 = pivot.drop(['Confirm_ID'], axis=1)

#TRACEABILITY_NETWORK
#pivot2 = df_drop.pivot_table(df_drop,index=["TO_EMPID","TO_FULLNAME","TO_CONTACTPHONE","TO_COMPANYNAME","TO_GROUPBU","COVID_STATUS","COVID_DATETIME","TO_LOCATION_NM","TO_LOCATION_DATE"])
#pivot2 = df_drop.pivot_table(df_drop,index=["EMPLOYEEID","FULLNAME","CONTACTPHONE","COMPANYNAME","GROUPBU","COVID_STATUS","COVID_DATETIME","LOCATION_NAME","LOCATION_DATE"])
#pivot3 = pivot2.drop(['ConfirmID','FROM_LAT','FROM_LONG'], axis=1)

################################################  STEP 5 ########################################################
# SAVE FILE
PATH_NETWORK = r'./TEMP/SUMMARY.xlsx'
writer = pd.ExcelWriter(PATH_NETWORK, engine = 'xlsxwriter')
df_outA.to_excel(writer, sheet_name = 'DATA_TIMELINE',index=False)
pivot4.to_excel(writer, sheet_name = 'TIMELINE')
df_drop.to_excel(writer, sheet_name = 'DATA_NETWORK',index=False)
#pivot3.to_excel(writer, sheet_name = 'NETWORK')
writer.save()
writer.close()

################################################  STEP 6 ########################################################
#SEND EMAIL
#email(PATH_NETWORK,Status,confrim_dateTime,case)
print("Finish send Mail")

################################################  STEP 7 ########################################################
# WRITE TO DATABASE
#Write TIMLINE 
df_WA = WA(df_outA)
print('Complte Write A to DATABASE')

#Write TRACEABILITY  
df_WB = WB(df_outB,Status)
print('Complte Write B to DATABASE')
 
end_datetime = datetime.datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')

