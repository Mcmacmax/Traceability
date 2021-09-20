import pandas as pd
import numpy as np
import datetime
import pyodbc as db
import os
import glob
from dateutil.relativedelta import relativedelta

def A(dfobj,dfoutA,Output_PathA,Input_external):
    ##################################################### 1. Del Data and write from data frame
    """ database connection ={SQL Server Native Client 11.0};"""
    conn = db.connect('Driver={SQL Server Native Client 11.0};'
                        'Server=SBNDCTSREMP;'
                        'Database=SR_APP;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    count = 0
    for v in dfobj.values:
        count = count+1
        No = v[0]
        FROM_EMPID = v[1]
        FROM_STATUS = v[2]
        dateto =v[3]
        if type(dateto) == str:
            dateto = datetime.datetime.strptime(dateto, '%Y-%m-%d')
        else:
            dateto=v[3]
        datefrom = dateto - relativedelta(days=13)
        dfout=[]
        dfout = pd.DataFrame(columns=['Confirm_ID','STATUS','Employeeid','Location_Datetime','DATE','latitude','longitude','RUN_TIME','Source','LocationName','MAP'])
        #print(v,datefrom,dateto)
        SQL = """
        declare @Confirm_ID nvarchar(8) = '"""+str(No)+"""'
        declare @STATUS nvarchar(8) = '"""+str(FROM_STATUS)+"""'
        declare @dateFrom date = '"""+str(datefrom)+"""'
        declare @dateTo date = '"""+str(dateto)+"""'
        declare @EmpID nvarchar(15) = '"""+str(FROM_EMPID)+"""'
        select distinct AA.*
        from
        (
        select @Confirm_ID [Confirm_ID]
        ,@STATUS [STATUS]
        ,[employee_id] [employeeid]
        ,[create_date] [Timestamp]
        ,cast([create_date] as date) [DATE]
        ,format(cast([latitude] as float),'#.000000') [latitude]
        ,format(cast([longitude] as float),'#.000000') [longitude]
        ,cast(getdate() as smalldatetime) RUN_TIME
        ,'TB_SR_Covid_location' Source
        ,'' LocationName
		,CONCAT('https://www.google.com/maps/place/',format(cast([latitude] as float),'#.000000') ,',',format(cast([longitude] as float),'#.000000')) MAP
        from [SR_APP].[dbo].[TB_SR_Covid_location]
        where concat([latitude],',',[longitude]) <> '0,0'
        and ([latitude] is not null or [longitude] is not null)
        and cast([create_date] as date) between @dateFrom and @dateTo
        and [employee_id] = @EmpID

        union
        SELECT  @Confirm_ID [Confirm_ID]
        ,@STATUS [STATUS]
        ,CF.[EmployeeID] [employeeid]
        ,CF.[CHECKIN] [Timestamp]
        ,cast(CF.[CHECKIN] as date) [DATE]
        ,left( CF.[Location],CHARINDEX(',',CF.[Location], 1)-1) [latitude]
        ,SUBSTRING ( CF.[Location],CHARINDEX(',',CF.[Location], 1)+1,len(CF.[Location])) [longitude]
        ,cast(getdate() as smalldatetime) RUN_TIME
        ,'TB_SR_EMPLOYEE' Source
        ,'' LocationName
		,CONCAT('https://www.google.com/maps/place/',left( CF.[Location],CHARINDEX(',',CF.[Location], 1)-1),',',SUBSTRING ( CF.[Location],CHARINDEX(',',CF.[Location], 1)+1,len(CF.[Location]))) MAP
        FROM [TB_SR_Employee].[dbo].[FCT_CHKIN_FL] CF
        where EmployeeID = @EmpID
        and cast(CF.[CHECKIN] as date)  between @dateFrom and @dateTo

        union
        /* PG check in*/
        select  @Confirm_ID [Confirm_ID]
        ,@STATUS [STATUS]
        ,A.[EmployeeId]
        ,cast(A.[CreatedDateTime] as smalldatetime) [Location_Datetime]
        ,CAST(A.[CreatedDateTime] AS DATE) as [DATE]
        ,[UserLat] as [latitude]
        ,[UserLong] as [longitude]
        ,cast(getdate() as smalldatetime) RUN_TIME
        ,'PG_CHECKIN' Source
        ,ShopName LocationName
		,CONCAT('https://www.google.com/maps/place/',[UserLat],',',[UserLong]) MAP
        from [SR_APP].[dbo].[TB_Checkin_PG] A
        where cast(A.[CreatedDateTime] as date) between @dateFrom and @dateTo
        and A.EmployeeId=cast(@EmpID as float) ---ไม่เอาตัวเอง
        ) AA

        union

        SELECT @Confirm_ID [Confirm_ID]
        ,@STATUS [STATUS]
        ,[EmployeeId] [employeeid]
        ,cast(TS.CreatedDateTime as smalldatetime) [Location_Datetime]
        ,cast(TS.CreatedDateTime as date) [DATE]
        ,case when [UserLat]='0.0' then format(cast([LocationLat] as float),'#.000000') else format(cast([UserLat] as float),'#.000000') end [latitude]
        ,case when [UserLong]='0.0' then format(cast([LocationLong] as float),'#.000000') else format(cast([UserLong] as float),'#.000000') end [longitude]
        ,cast(getdate() as smalldatetime) RUN_TIME
        ,'TB_QR_TimeStamp' Source
        ,coalesce(B.LocationNameTH, case when TS.ShopName = '' then '' else TS.ShopName end ) LocationName
		,CONCAT('https://www.google.com/maps/place/',case when [UserLat]='0.0' then format(cast([LocationLat] as float),'#.000000') else format(cast([UserLat] as float),'#.000000') end,',',case when [UserLong]='0.0' then format(cast([LocationLong] as float),'#.000000') else format(cast([UserLong] as float),'#.000000') end) MAP
        FROM [SR_APP].[dbo].[TB_QR_TimeStamp] TS
                left join [SR_APP].[dbo].[TB_QR_Location] B on TS.LocationId=B.LocationId
        where cast(TS.CreatedDateTime as date) between @dateFrom and @dateTo
                and case when [UserLat]='0.0' then format(cast([LocationLat] as float),'#.000000') else format(cast([UserLat] as float),'#.000000') end <> '.000000'
        and case when [UserLong]='0.0' then format(cast([LocationLong] as float),'#.000000') else format(cast([UserLong] as float),'#.000000') end <> '.000000'
        and action ='CheckIn'
        and [EmployeeId] = @EmpID

        """
        #print(SQL)
        cursor.execute(SQL)
        data_Out = cursor.fetchall()
        for row in data_Out:
            newrow= {'Confirm_ID':float(row[0]),'STATUS':row[1],'Employeeid':row[2],'Location_Datetime':row[3], 'DATE':row[4],'latitude':row[5],'longitude':row[6],'RUN_TIME':row[7],'Source':row[8],'LocationName':row[9],'MAP':row[10]}
            dfout = dfout.append(newrow, ignore_index=True)
        print('A Complete ===>> ',count,' : ',v[1])
        Output_Path = r'./TEMP/TIMELINE.xlsx'
        dfout.to_excel(Output_Path,index=False)
        dfoutA = dfoutA.append(dfout,ignore_index=True)
        ###Input External Timeline
    data_external= pd.read_excel(Input_external)
    df_external = pd.DataFrame(data_external)
    if df_external.values[0][0]==dfoutA.values[0][0]: #Checkว่า เป็น case เดียวกันรึเปล่า
        df_filter = df_external.loc[df_external['Confirm_ID'] ==dfoutA.values[0][0]]
        dfoutA = pd.concat([dfoutA,df_filter])
        print(dfoutA)
    else:
        dfoutA = dfoutA
    dfoutA.sort_values(by=['DATE'])
    dfoutA.to_excel(Output_PathA,index=False)
    cursor.commit()
    return(dfoutA)

def B(dfoutB,Input_PathB,Output_PathB):
    count = 0
    for xlsx in glob.glob(Input_PathB, recursive=True): 
        dfout=[]
        count=count+1
        data_In= pd.read_excel(xlsx)
        dfobj = pd.DataFrame(data_In)
        df_write=dfobj.replace(np.nan,"''")
        for v in df_write.values:
            ConfirmID1 = v[0]
            FROM_STATUS1 =v[1]
            FROM_EMPID1 = v[2]
            FROM_LOCATION_DATE1 = v[3].strftime('%Y-%m-%d %H:%M:%S')
            datefrom1 = v[4].strftime('%Y-%m-%d')
            dateto1 = v[4].strftime('%Y-%m-%d')
            FROM_LAT1 = v[5]
            FROM_LONG1 = v[6]
            LocationName =str(v[9]).replace("'", "")
            #TRACE_DATE1 =v[7]
            print(ConfirmID1,"_",FROM_STATUS1,"_",FROM_EMPID1,"_",datefrom1,"_",dateto1,"_",FROM_LAT1,"_",FROM_LONG1)

            dfout = pd.DataFrame(columns=['ConfirmID','TRACE_DATE','FROM_EMPID','FROM_STATUS','FROM_LAT','FROM_LONG','FROM_LOCATION_DATE','TO_EMPID','TO_LAT','TO_LONG','TO_LOCATION_DATE','TO_LOCATION_NM','FROM_LOCATION_NM'])
            ##################################################### 1. Del Data and write from data frame
            """ database connection ={SQL Server Native Client 11.0};"""
            conn = db.connect('Driver={SQL Server Native Client 11.0};'
                                'Server=SBNDCTSREMP;'
                                'Database=SR_APP;'
                                'Trusted_Connection=yes;')
            cursor = conn.cursor()
            SQL =  """
            /* Declare Tracking Date Preriod */

            --------------INPUT FILED---------------
            declare @confirmid nvarchar(8) ='"""+str(ConfirmID1)+"""'
            declare @dateFrom date = '"""+str(datefrom1)+"""'
            declare @dateTo date = '"""+str(dateto1)+"""' 
            declare @LatIn float  = '"""+str(FROM_LAT1)+"""'
            declare @longIn float = '"""+str(FROM_LONG1)+"""'
            declare @EMP nvarchar(15) = '"""+str(FROM_EMPID1)+"""'
            declare @status nvarchar(8) ='"""+str(FROM_STATUS1)+"""'
            declare @FROMDATETIME  smalldatetime = '"""+str(FROM_LOCATION_DATE1)+"""'
            declare @FROM_LOCATION_NM nvarchar(255) = N'"""+str(LocationName)+"""'

            --------------AUTO CALCUCATE 50M-------------------
            declare @Lat float =   cast(cast(@LatIn as nvarchar) as float)-0.000425
            declare @Lat1 float =  cast(cast(@LatIn as nvarchar) as float)+0.000425
            declare @long float  = cast(cast(@LongIn as nvarchar)as float)-0.000425
            declare @long1 float = cast(cast(@LongIn as nvarchar)as float)+0.000425
            ----------------------------------------------

            --------------AUTO CALCUCATE 100M-------------------
            --declare @Lat float =   cast(cast(@LatIn as nvarchar) as float)-0.00085
            --declare @Lat1 float =  cast(cast(@LatIn as nvarchar) as float)+0.00085
            --declare @long float  = cast(cast(@LongIn as nvarchar)as float)-0.00085
            --declare @long1 float = cast(cast(@LongIn as nvarchar)as float)+0.00085
            ----------------------------------------------

            /* List All Check-In Transection in Risk Area */

            select @confirmid ConfimeID
            ,cast(getdate() as smalldatetime) Trace_datetime
            ,@EMP FROM_EMPID
            ,@status FROM_STATUS
            ,@Lat FROM_LAT
            ,@long FROM_LONG
            ,@FROMDATETIME FROM_LOCATIONDATETIME
            ,MN.EmployeeId TO_EMP
            ---,'B' FROM_STATUS
            ,MN.latitude TO_LAT
            ,MN.longitude TO_LONG
            ,MN.CheckinDatetime TO_LOCATIONDATETIME
            ,MN.LocationName TO_LOCATION_NM
            ,case when @FROM_LOCATION_NM ='''' then '' else @FROM_LOCATION_NM end FROM_LOCATION_NM
            from ( /* QR check in*/
            SELECT TS.[EmployeeId] 
            ,cast(TS.CreatedDateTime as date) [CheckinDate]
            ,TS.CreatedDateTime [CheckinDatetime]
            ,Loc.LocationNameTH 
            ,TS.ShopName
            ,coalesce(Loc.LocationNameTH, case when TS.ShopName = '' then '' else TS.ShopName end ) LocationName
            ,[UserLat] as [latitude] 
            ,[UserLong] as [longitude]
            FROM [SR_APP].[dbo].[TB_QR_TimeStamp] TS
            left join [SR_APP].[dbo].[TB_QR_Location] Loc on Loc.LocationId = TS.LocationId
            where cast(TS.CreatedDateTime as date) between @dateFrom and @dateTo
            and cast([UserLat] as float) between @lat and @lat1
            and cast([UserLong] as float) between @long and @long1
            and TS.EmployeeId<>cast(@EMP as nvarchar) ---ไม่เอาตัวเอง
            union /* PG check in*/
            select  A.[EmployeeId]
            ,cast(A.[CreatedDateTime] as date) as [CheckinDate]
            ,A.[CreatedDateTime] as [CheckinDatetime] 
            ,A.[ShopName] [LocationNameTH]
            ,cast(A.[ShopId] as nvarchar(20)) ShopName
            ,A.[ShopName] [LocationName]
            ,[UserLat] as [latitude] 
            ,[UserLong] as [longitude] 
            from [SR_APP].[dbo].[TB_Checkin_PG] A
            where cast(A.[CreatedDateTime] as date) between @dateFrom and @dateTo
            and cast(A.[UserLat] as float) between @lat and @lat1
            and cast(A.[UserLong] as float) between @long and @long1
            and A.EmployeeId<>cast(@EMP as float) ---ไม่เอาตัวเอง
            ) MN
            where MN.EmployeeId <>''
            """
            print(SQL)
            cursor.execute(SQL)
            data_Out = cursor.fetchall()
            for row in data_Out:
                newrow= {'ConfirmID':float(row[0]),'TRACE_DATE':row[1],'FROM_EMPID':row[2],'FROM_STATUS':row[3],'FROM_LAT':row[4],'FROM_LONG':row[5],'FROM_LOCATION_DATE':row[6],'TO_EMPID':row[7],'TO_LAT':row[8],'TO_LONG':row[9],'TO_LOCATION_DATE':row[10],'TO_LOCATION_NM':row[11].replace("'", ""),'FROM_LOCATION_NM':row[12]}
                dfout = dfout.append(newrow, ignore_index=True)
            print('B Complete ===>> ',count,' : ')
            #Output_Path = r'./Output B/'+str(filename)+'.xlsx'
            #dfout.to_excel(Output_Path,index=False)
            dfoutB = dfoutB.append(dfout,ignore_index=True)
        dfoutB.sort_values(by=['FROM_LOCATION_DATE'])
        dfoutB.to_excel(Output_PathB,index=False)
        cursor.commit()
        return(dfoutB)

def writeA(df_outA):
    dfobj = pd.DataFrame(df_outA)
    df_write = dfobj
    #df_write = dfobj.replace(np.nan,0)
    ##################################################### 1. Del Data and write from data frame
    #start_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #print ('DATE',start_datetime)
    """ database connection ={SQL Server Native Client 11.0};"""
    conn = db.connect('Driver={SQL Server Native Client 11.0};'
                        'Server=SBNDCTSREMP;'
                        'Database=TB_SR_Employee;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    ####sql_del ="delete FROM [TB_SR_Employee].[dbo].[TRACE_EMPLOYEE]"
    ####cursor.execute(sql_del)
    for index, row in df_write.iterrows():
        print(row)
        cursor.execute("""INSERT INTO TB_SR_Employee.dbo.TRACE_Covid_Confirm_TIMELINE([Confirm_ID],[STATUS],[Employeeid],[Location_Datetime],[DATE],[latitude],[longitude],[RUN_TIME],[Source],[LocationName],[MAP]) 
        values('%f',N'%s',N'%s',N'%s',N'%s','%f','%f',N'%s',N'%s',N'%s',N'%s')"""%\
            (float(row[0]),row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],str(row[9]).replace("'", ""),row[10])
        )   
    cursor.commit()

def writeB(df_outB,Status):
    dfobj = pd.DataFrame(df_outB)
    df_write = dfobj
    #df_write = dfobj.replace(np.nan,0)
    ##################################################### 1. Del Data and write from data frame
    #start_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #print ('DATE',start_datetime)
    """ database connection ={SQL Server Native Client 11.0};"""
    conn = db.connect('Driver={SQL Server Native Client 11.0};'
                        'Server=SBNDCTSREMP;'
                        'Database=TB_SR_Employee;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    ####sql_del ="delete FROM [TB_SR_Employee].[dbo].[TRACE_EMPLOYEE]"
    ####cursor.execute(sql_del)
    for index, row in df_write.iterrows():
        row[11].replace("'", "")
        print(row)
        cursor.execute("""INSERT INTO TB_SR_Employee.dbo.TRACE_EMPLOYEE([ConfirmID],[TRACE_DATE],[FROM_EMPID],[FROM_STATUS],[FROM_LAT],[FROM_LONG],[FROM_LOCATION_DATE],[TO_EMPID],[TO_LAT],[TO_LONG],[TO_LOCATION_DATE],[TO_LOCATION_NM],[FROM_LOCATION_NM]) 
        values(N'%f',N'%s',N'%s',N'%s','%f','%f',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s',N'%s')"""%\
            (float(row[0]),row[1].strftime('%Y-%m-%d %H:%M:%S'),row[2],row[3],row[4],row[5],row[6].strftime('%Y-%m-%d %H:%M:%S'),row[7],row[8],row[9],row[10].strftime('%Y-%m-%d %H:%M:%S'),row[11],row[12])
        )   
    cursor = conn.cursor()
    sql_Trace_flg ="update [TB_SR_Employee].[dbo].[TRACE_Covid_Confirm_Status] set [Trace_Flg] = 1 where [Trace_Flg]=0 and [Status_Confirm] = '"+str(Status)+"'"
    cursor.execute(sql_Trace_flg)
    cursor.commit()

def Employee_Profile():
    """database connection ={SQL Server Native Client 11.0};"""
    conn = db.connect('Driver={SQL Server Native Client 11.0};'
                        'Server=SBNDCTSREMP;'	
                        'Database=SR_APP;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    #dfout = pd.DataFrame(columns=['EmployeeId','FullName', 'CompanyName','GroupBU','ContactPhone'])
    SQL = """ 
    SELECT A.[EmployeeId]
	  ,CONCAT(A.LocalFirstName,' ',A.LocalLastName) FullName
      ,A.[CompanyName]
      ,A.[GroupBU]
      ,A.[ContactPhone]
	  ,A.[PGBU]
	  ,B.LEVEL
    FROM [SR_APP].[dbo].[TB_Employee] A
    left join [TB_SR_EMPLOYEE].[dbo].[PersonalLevel] B on A.PersonalLevel=B.PersonalLevel
    """ 
    #print(SQL)
    dfout = pd.DataFrame(data=pd.read_sql_query(SQL,conn),columns=['EmployeeId','FullName', 'CompanyName','GroupBU','ContactPhone','PGBU','LEVEL'])
    cursor.commit()
    return(dfout)
    
def COVID_APP():
    """database connection ={SQL Server Native Client 11.0};"""
    conn = db.connect('Driver={SQL Server Native Client 11.0};'
                        'Server=SBNDCTSREMP;'	
                        'Database=SR_APP;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    SQL = """ 
    /* All Checkin and COVID as Checkin definition */
    /* Edition on '2021-01-28'  */
    SET ansi_warnings OFF
    declare @fromdate date = cast(getdate() as date)
    declare @todate date = cast(getdate() as date)

    select M.COVID_EMPID EC_EMPID
      ,M.COVID_STATUS_GRP COVID_APP_STATUS
	  ,M.COVID_Datetime COVID_APP_DATETIME 

    from(

    select COVID_EMPID, COVID_STATUS_GRP,'COVID' as [ACTION], DONE_ON ,answer_id,'answer_web' as [sys_ver], COVID_Datetime, COVID_Date
    from ( select [employee_id] COVID_EMPID
      ,[covid_code] as COVID_STATUS ,[covid_code] as COVID_STATUS_GRP
      ,cast(convert(datetime, [created_date]) as datetime) COVID_Datetime, cast(convert(datetime, [created_date]) as date) COVID_Date
      ,W.id as answer_id,q5_province_id, q5_province_name q5_province_nm, q6_province_id, q6_province_name q6_province_nm,risk_area_status
      ,ROW_NUMBER() OVER ( PARTITION BY [employee_id], cast(convert(datetime, [created_date]) as date) ORDER BY convert(datetime, [created_date]) desc ) row_num
      ,'WEB' DONE_ON
  FROM [SR_APP].[dbo].TB_SR_Covid_answer_web_four W
  where Remark = 'Employee' and len([employee_id]) = 8 and cast([created_date] as date) between @fromdate and @todate
    ) AA where AA.row_num = 1

    union

    select COVID_EMPID, COVID_STATUS_GRP,'COVID' as [ACTION] ,DONE_ON ,answer_id  ,[sys_ver] ,COVID_Datetime, COVID_Date
    from (select E.[employee_id] COVID_EMPID ,A.[COVID_STATUS] ,[COVID_STATUS] as COVID_STATUS_GRP,'APP' DONE_ON
        ,A.COVID_Datetime, A.COVID_Date    ,answer_id ,[sys_ver]
        ,ROW_NUMBER() OVER ( PARTITION BY E.[employee_id] ,A.COVID_Date ORDER BY A.COVID_Datetime desc) row_num
    FROM 
    (    select ath.[id] as answer_id
        ,ath.[employee_iid] as [employee_id]
        ,case ath.[covid_status]
            when 5 then 'B1'
            when 4 then 'A'
            when 3 then 'B'
            when 2 then 'C'
            when 1 then 'D'
            end as [COVID_STATUS]
        ,cast(ath.[create_date] as date) as COVID_Date
        ,ath.[create_date] as COVID_Datetime
        ,'answer_four' as [sys_ver] 
        from (select * from [SR_APP].[dbo].[TB_SR_Covid_answer_four] where [covid_status] is not null and employee_iid <> '' and cast(create_date as date) between @fromdate and @todate) ath
    ) A
    left join [SR_APP].[dbo].[TB_SR_Covid_employee] E on A.[employee_id] = E.[iid]
    where  A.[covid_status] is not null and E.[employee_id] <> '' and len(E.[employee_id]) = 8 and A.COVID_Date between @fromdate and @todate
    ) COV_STS
	where COV_STS.row_num = 1
    ) M
    """ 
    #print(SQL)
    cursor.execute(SQL)
    data_Out = cursor.fetchall()
    dfout = pd.DataFrame(data=pd.read_sql_query(SQL,conn))
    cursor.commit()
    return(dfout)