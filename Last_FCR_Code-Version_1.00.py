#!/usr/bin/env python
# coding: utf-8

# # Create Consolidated Files for Customer and Captain

# In[2]:


import pandas as pd
import numpy as np
import xlrd
import xlsxwriter
import openpyxl
from openpyxl.worksheet.table import Table, TableStyleInfo
from datetime import datetime  
from datetime import timedelta 
import sqlalchemy  
from pyhive import presto
from sqlalchemy.engine import create_engine


# Read raw data of data sources in Pandas datafromes.
df_Customer = pd.read_csv ('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/ZD/Customer_FCR_Raw_data.csv', keep_default_na = False)
df_Voice = pd.read_csv ('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/Voice/Disposition_for_LLS_customer_2019_12_01.csv', keep_default_na = False)
df_Chat = pd.read_excel ('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/Chat/Session_time_Chat_Concurrency_2019_12_02.xlsx', keep_default_na = False)
df_Captain = pd.read_csv ('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/ZD/Captain_FCR_Raw_data.csv', keep_default_na = False)

# Read the mapping file from presto.
presto_02_fin = sqlalchemy.create_engine('presto://ishu_bhardwaj@presto-python-r-script-cluster.careem-engineering.com:8080/hive')
query = open('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Testing_Python//mapping file.txt', 'r').read()
df_Mapping_Complete = pd.read_sql(con=presto_02_fin, sql=query) 

# Choose only the required columns from mapping file along with renaming queue column name.
df_Mapping_Complete = df_Mapping_Complete.rename(columns = {"resource_name":"queue (Ticket Group)"})
df_Mapping_Needed  =  df_Mapping_Complete[['queue (Ticket Group)','domain','media','captain_consumer','for_lls_dashboard']]


# Renaming Customer data source Columns to make columns names across all data sources consistent.  
df_Customer = df_Customer.rename(columns = {"Date (Ticket Created)": "created", "Phone number.1" : "phonenumber", "User ID" : "userid", 
                              "Ticket Id" : "callid (ticket id)", "User External ID" : "Captain_ID (User external ID)",
                              "Reason (Quality/GO/Billing)" : "Disposition", "Ticket Group" : "queue (Ticket Group)",
                               "Ticket Assignee" : "agentname (Ticket Assignee)","Country" : "country" , "Booking ID" : "bookingid"})

del df_Customer['Phone number'] # deleting the second "Phone number" column.


df_Customer['data_Source'] = 'Zendesk' # Create 'data_Source' column and asign 'Zendesk' to Customer data frame. 
df_Customer['Start_Time'] = pd.to_datetime(df_Customer['created'])
df_Customer['Start_Time'] = df_Customer['Start_Time']+ timedelta(hours = 23, minutes = 59) # 23:59 Hypothesis for no time contacts.




# Renaming Captain data source Columns to make columns names across all data sources consistent.
df_Captain = df_Captain.rename(columns = {"Date (Ticket Created)": "created", "Phone number.1" : "phonenumber", "User ID" : "userid", 
                              "Ticket Id" : "callid (ticket id)", "User External ID" : "Captain_ID (User external ID)",
                              "Captain Support Reason" : "Disposition", "Ticket Group" : "queue (Ticket Group)",
                              "Ticket Assignee" : "agentname (Ticket Assignee)","Country" : "country" , "Booking ID" : "bookingid"})

del df_Captain['Phone number']

df_Captain['data_Source'] = 'Zendesk'
df_Captain['Start_Time'] = pd.to_datetime(df_Captain['created'])
df_Captain['Start_Time'] = df_Captain['Start_Time']+ timedelta(hours = 23, minutes = 59)



df_Voice['date1'] = df_Voice['date1'] + " " + df_Voice['time'] # Merge date and time columns in one column.
del df_Voice['time']

# Renaming Voice data source Columns to make columns names across all data sources consistent.  
df_Voice = df_Voice.rename(columns = {"date1": "created", "PhoneNumber" : "phonenumber", "user_id" : "userid", 
                              "callid" : "callid (ticket id)", "captain_id" : "Captain_ID (User external ID)",
                              "new_disposition" : "Disposition", "queue" : "queue (Ticket Group)",
                              "agent" : "agentname (Ticket Assignee)", "booking_id" : "bookingid", "aht" : "AHT"})

df_Voice['data_Source'] = 'Genesys' # Genesys is the source for voice calls data so we assgin Genesys data source to Voice DF. 
df_Voice['created'] = pd.to_datetime(df_Voice['created']) # Changing Created Column to Datetime type.
df_Voice['Start_Time'] = df_Voice['created']

# Required actions in Chat data source.
df_Chat = df_Chat.rename(columns = { 
                              "callid" : "callid (ticket id)",
                              "disp" : "Disposition", "queue" : "queue (Ticket Group)",
                              "agentname" : "agentname (Ticket Assignee)"})



df_Chat.drop(df_Chat[df_Chat['queue (Ticket Group)'] == 'Test_VQ'].index, inplace = True) # Excluding Test queue from our calculations.
df_Chat['data_Source'] = 'Genesys' # Genesys is the source for Chat Contacts data so we assgin Genesys data source to Voice DF.
df_Chat['created'] = pd.to_datetime(df_Chat['created'])
df_Chat['Start_Time'] = df_Chat['created']


# Consolidated file creation : Merge all data fromes together. 
df_Consolidated = pd.concat([df_Voice, df_Chat, df_Customer, df_Captain],ignore_index=True)

# Merging the Consolidated file with the mapping file based in queue column.
df_Final = df_Consolidated.merge(df_Mapping_Needed, on='queue (Ticket Group)')

df_Final = df_Final[df_Final['domain'] == 'Ride Hailing']
# Fixing an issue with the mapping file considering "CHAT_RIDE_UAE_AR_CUS_VQ" queue as Email type, we replace it with "Chat".
Mask_UAE_Queue = df_Final['queue (Ticket Group)'] == 'CHAT_RIDE_UAE_AR_CUS_VQ'
df_Final['media'][Mask_UAE_Queue == True] = 'Chat'

# Changing media type string from "zendesk_nv" to "Non_Voice", beneficial in upcoming queries.
Mask_zendesk_nv = df_Final['media'] == 'zendesk_nv'
df_Final['media'][Mask_zendesk_nv == True] = 'Non_Voice'

# Create Date and Timestamp_Form Columns. 
df_Final['Date'] = df_Final['Start_Time'].dt.date
df_Final['Timestamp_Form'] = df_Final['Start_Time'].dt.time

# Create Customer and Captain separate files.
df_Final_Customer = df_Final.loc[df_Final['captain_consumer'] == 'Customer']
df_Final_Captain = df_Final.loc[df_Final['captain_consumer'] == 'Captain']

# Asending sorting of datafromes based on "Start_Time" column.
df_Final_Customer = df_Final_Customer.sort_values('Start_Time')
df_Final_Captain = df_Final_Captain.sort_values('Start_Time')



# coming lines can be used for expoerting Customer and Captain raw data files.
#--df_array[0].to_excel('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/Final_Consolidated_Customer_File.xlsx', index =False , sheet_name='Sheet1')
#--df_array[1].to_excel('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/Final_Consolidated_Captain_File.xlsx', index =False , sheet_name='Sheet1')


# # Preprocessing Actions

# In[3]:


# Create an array contains Customer and Captain dataframes as we need to do the same actions on both of them separately.
df_array = [df_Final_Customer, df_Final_Captain] 

for i in range(0,2):
    df_array[i] = df_array[i].reset_index() # Resetting Indexes of both data frames this's will help in looping through rows.

    # Replace blanks, '-' and 'null' string with Nan as we want to deal with all of them as Nans.
    Nan_mask = (df_array[i]['bookingid'] == '') | (df_array[i]['bookingid'] == 'null')
    df_array[i]['bookingid'][Nan_mask == True] = np.nan 
    Nan_mask = (df_array[i]['userid'] == '') | (df_array[i]['userid'] == '-') | (df_array[i]['userid'] == 'null')
    df_array[i]['userid'][Nan_mask == True] = np.nan
    Nan_mask = (df_array[i]['Disposition'] == 'N/A') | (df_array[i]['Disposition'] == '')
    df_array[i]['Disposition'][Nan_mask == True] = np.nan
    Nan_mask = (df_array[i]['# Reopens'] == '')
    df_array[i]['# Reopens'][Nan_mask == True] = np.nan
    


# # Disposition Flag Column Creation

# In[4]:


# create Grid Dictionary for Dispositions.
grid_dict={}
grid_dict['Others____Internal transfer']='Transfer'
grid_dict['Call Dropped']='Dropped/Silent Calls'
grid_dict['Operations related - Dropped call']='Dropped/Silent Calls'
grid_dict['Operations related - Dropped calls']='Dropped/Silent Calls'
grid_dict['Others____Dropped\\\\ Silent call']='Dropped/Silent Calls'
grid_dict['Operations related - Silent Calls']='Dropped/Silent Calls'
grid_dict['Operations related - Silent Call']='Dropped/Silent Calls'

for i in range(0,2):
        df_array[i]['Disposition Flags'] = df_array[i]['Disposition'].map(grid_dict) # map each disposition with its flag.
                


# # New_Booking_ID Column Creation

# In[5]:


# looping through each dataframe in df_array (df_Final_Customer and df_Final_Captain) and od exactly the same actions.

for j in range(0,2): 
    df_array[j]['New_Booking_ID'] = np.nan  # Create an empty New_Booking_ID column.
    
    # Create pandas series called mask to check in each row if userid matched previous row's userid along with Nan Bookingid. 
    # Mask is an efficient way to update pandas series (Column) without looping through all cells.
    mask = df_array[j]['userid'].eq(df_array[j]['userid'].shift(1)) & pd.isnull(df_array[j]['bookingid'])


    # Filling out 'New_Booking_ID' Column based on mask value.
    df_array[j]['New_Booking_ID'][mask == False] = df_array[j]['bookingid']
    df_array[j]['New_Booking_ID'][mask == True] = df_array[j]['New_Booking_ID'].shift(1)


    # Mask is quite efficient however it does the action only one time, in cases where there're a lot of blanks under each other
    # mask will only update the first blank cell to fix it we'll only iterate on blanks cells. 
    
    Null_index= df_array[j]['New_Booking_ID'][((pd.isnull(df_array[j]['New_Booking_ID'])) == True)].index # Finding indexes of blanks cells in "New_Booking_ID". 
    for i in Null_index:
        if i !=0  and (df_array[j].loc[i,'userid'] == df_array[j].loc[i-1,'userid']) or (pd.isnull(df_array[j].loc[i,'userid']) and pd.isnull(df_array[j].loc[i-1,'userid']) == True):
            if pd.isnull(df_array[j].loc[i,'bookingid']):
                df_array[j].loc[i,'New_Booking_ID'] = df_array[j].loc[i-1,'New_Booking_ID']
            else:

                df_array[j].loc[i,'New_Booking_ID']= df_array[j].loc[i, 'bookingid']       
       
    


# # User ID availability Column Creation

# In[6]:


for j in range(0,2):
    # create 'User_id_avail' will equal to 1 if userid in this row exist otherwise it'll equal to 0
    df_array[j]['User_id_avail'] = pd.notnull(df_array[j]['userid']).apply(int)


# # Min_Threshold_Time Column Creation

# In[7]:


# Create Threshold dictionary with start (beginning of TH period) as the first key and end as the second one.

grid_dict_TH={}
grid_dict_TH["Voice"]=[0, 24]  
grid_dict_TH["Non_Voice"]= [ 0 , 24]
grid_dict_TH["Chat"]= [2, 24]

for j in range(0,2):
    # Creat an empty Min_Threshold_Time column
    df_array[j]['Min_Threshold_Time'] = np.nan
    
    # Change type of column to be daatetime.
    df_array[j]['Min_Threshold_Time'] = pd.to_datetime(df_array[j]['Min_Threshold_Time']) 
    
    # Create a list contains TH_dictionary values (two series one for start and another one for End)
    TH_dictionary_Values = pd.DataFrame(df_array[j]["media"].map(grid_dict_TH).values.tolist())
    
    # create pandas series contains only start values.
    Start_Series = TH_dictionary_Values.iloc[:,0]
    
    
    # Take the generic formula of calculating 'Min_Threshold_Time' column = Start_Time + Start_Series(begainning of TH) 
    df_array[j]['Min_Threshold_Time']= df_array[j]['Start_Time'] + pd.to_timedelta(Start_Series, 'h')

    # Create a mask equals to True when userid of a row matched previous row userid.
    mask_User = df_array[j]['userid'].eq(df_array[j]['userid'].shift(1))

     # Create pandas series contains all indexes of cells where mask_User is True.
    Condition_index = df_array[j]['Min_Threshold_Time'][mask_User == True].index 
    
    # Looping through True mask_User indexes and update values if their rows matched the right conditions.
    for i in Condition_index:
        if  i!=0 and df_array[j].loc[i,'userid'] == df_array[j].loc[i-1,'userid'] and df_array[j].loc[i,'Start_Time'] < df_array[j].loc[i-1,'Min_Threshold_Time'] :
             if (df_array[j].loc[i-1, 'Min_Threshold_Time'] - df_array[j].loc[i, 'Start_Time']) < timedelta(hours = grid_dict_TH[ df_array[j].loc[i, "media"]][0]) :
                    df_array[j].loc[i, 'Min_Threshold_Time']= df_array[j].loc[i-1, 'Min_Threshold_Time']

    


# # End_Time Column Creation

# In[8]:


for j in range(0,2):
    df_array[j]['End_Time'] = np.nan
    df_array[j]['End_Time'] = pd.to_datetime(df_array[j]['End_Time'])
    
    # create pandas series contains only End values.
    End_Series = TH_dictionary_Values.iloc[:,1]
   

    # Mask_User creation.
    mask_User = df_array[j]['userid'].eq(df_array[j]['userid'].shift(1)) 

    
    # Take the generic formula of calculating 'End_Time' column = Start_Time + End_Series.
    df_array[j]['End_Time']= df_array[j]['Start_Time'] + pd.to_timedelta(End_Series, 'h')

  
    # Looping through True mask_User indexes and update values if their rows matched the right conditions.
    Condition_index = df_array[j]['End_Time'][mask_User == True].index 
    for i in Condition_index:
        if  i!=0 and (df_array[j].loc[i,'userid'] == df_array[j].loc[i-1,'userid'] or pd.isnull(df_array[j].loc[i,'userid']) and pd.isnull(df_array[j].loc[i-1,'userid'])) and (df_array[j].loc[i,'Start_Time'] < df_array[j].loc[i-1,'End_Time']) :
            if (df_array[j].loc[i-1, 'End_Time'] - df_array[j].loc[i, 'Start_Time']) < timedelta(hours = grid_dict_TH[ df_array[j].loc[i, "media"]][1]) :
                   df_array[j].loc[i, 'End_Time']= df_array[j].loc[i-1, 'End_Time']


# # Multiple_Contacts Column creation

# In[19]:


for j in range(0,2):
    # create Start_Time pandas series
    Start_Time = pd.Series(df_array[j]['Start_Time'])
    # Pandas deals with nans as nan not equal to nan we need all nans to equal each other.
    mask = (df_array[j]['userid'].eq(df_array[j]['userid'].shift(1)) | (pd.isnull(df_array[j]['userid']) & pd.isnull(df_array[j]['userid'].shift(1))) ) & (Start_Time < df_array[j]['End_Time'].shift(1)) & (df_array[j]['New_Booking_ID'].eq(df_array[j]['New_Booking_ID'].shift(1)) | (pd.isnull(df_array[j]['New_Booking_ID']) & pd.isnull(df_array[j]['New_Booking_ID'].shift(1)))) 

    df_array[j]['Multiple_Contacts'] = mask.apply(int)



# #  Multiple_Contacts (Thres Adjusted) Column Creation

# In[20]:


for j in range(0,2):
    Start_Time = pd.Series(df_array[j]['Start_Time'])
    df_array[j]['Multiple_Contacts (Thres Adjusted)'] = np.nan
    mask = (df_array[j]['media'].eq(df_array[j]['media'].shift(1))) & (Start_Time < df_array[j]['Min_Threshold_Time'].shift(1)) & df_array[j]['Multiple_Contacts']==1
    df_array[j]['Multiple_Contacts (Thres Adjusted)'][mask == True] = 0
    df_array[j]['Multiple_Contacts (Thres Adjusted)'][mask == False] = df_array[j]['Multiple_Contacts']
    


# # Disposition_Category Column Creation

# In[21]:


for j in range(0,2):
    df_array[j]['Disposition_Category'] = np.nan
#     mask = (df_array[j]['userid'] != df_array[j]['userid'].shift(1)) & (pd.notnull(df_array[j]['userid'])) & (pd.notnull(df_array[j]['userid'].shift(1))) & (pd.isnull(df_array[j]['Disposition Flags']) == False)  & (df_array[j]['Test_Multiple_Contacts'].shift(-1)!=1)
    mask = (df_array[j]['userid'].fillna(0) != (df_array[j]['userid'].shift(1)).fillna(0)) & (pd.isnull(df_array[j]['Disposition Flags']) == False)  & (df_array[j]['Multiple_Contacts'].shift(-1)!=1)

    df_array[j]['Disposition_Category'][mask == False] = np.nan
    df_array[j]['Disposition_Category'][mask == True] = "Single Contact -" + df_array[j]['Disposition Flags']
    



# # ZD_Category Column Creation

# In[22]:


for j in range(0,2):

    df_array[j]['ZD_Category'] = np.nan

    Reopen_mask = (df_array[j]['data_Source'] == 'Zendesk') & ((df_array[j]['# Tickets Reopened']==1) | (df_array[j]['# Reopens'] >= 1))
    Hold_mask = (df_array[j]['data_Source'] == 'Zendesk') & ((df_array[j]['# On Hold Tickets'] == 1) | (df_array[j]['Ticket Status'] == "Hold")) 
    Pending_mask = (df_array[j]['data_Source'] == 'Zendesk') & ((df_array[j]['# Pending Tickets'] == 1) | (df_array[j]['Ticket Status'] == "Pending")) 
    Solved_mask = (df_array[j]['data_Source'] == 'Zendesk') & ((df_array[j]['# Tickets Solved'] == 1) | (df_array[j]['# Tickets Solved (but not closed)'] == 1) | (df_array[j]['# Tickets Solved From Hold - Pankaj'] == 1) | (df_array[j]['# Tickets Solved With English Tag'] == 1) | (df_array[j]['Ticket Status'] == "Solved")) 
    Not_Worked_mask = (df_array[j]['data_Source'] == 'Zendesk') & (pd.isnull(df_array[j]['Disposition'])== True)

    df_array[j].loc[Reopen_mask == True, 'ZD_Category'] ="2_Zendesk_Reopen"
    df_array[j].loc[(Reopen_mask == False) & (Hold_mask == True), 'ZD_Category'] ="3_Zendesk_On_Hold"
    df_array[j].loc[(Reopen_mask == False) & (Hold_mask == False) & (Pending_mask == True), 'ZD_Category'] = "4_Zendesk_Pending"
    df_array[j].loc[(Reopen_mask == False) & (Hold_mask == False) & (Pending_mask == False) & (Solved_mask == True), 'ZD_Category'] = "5_Zendesk_Solved" 
    df_array[j].loc[(Reopen_mask == False) & (Hold_mask == False) & (Pending_mask == False) & (Solved_mask == False) & (Not_Worked_mask == True), 'ZD_Category'] = "6_Zendesk_Not_Worked"
    df_array[j].loc[(Reopen_mask == False) & (Hold_mask == False) & (Pending_mask == False) & (Solved_mask == False) & (Not_Worked_mask == False) & (df_array[j]['data_Source'] == 'Zendesk'), 'ZD_Category'] = '7_Zendesk_Worked_New_Open'
    df_array[j].loc[(Reopen_mask == False) & (Hold_mask == False) & (Pending_mask == False) & (Solved_mask == False) & (Not_Worked_mask == False) & (df_array[j]['data_Source'] != 'Zendesk'), 'ZD_Category'] = 'Live_Channels'



        
            
                 
                


# # Final_Category Column Creation

# In[23]:


for j in range(0,2):
    Mask_Multiple = (df_array[j]['Multiple_Contacts (Thres Adjusted)'] ==1) | (df_array[j]['Multiple_Contacts (Thres Adjusted)'].shift(-1)==1)
    Mask_Trasfered_Dropped = pd.notnull(df_array[j]['Disposition_Category'])
    Mask_ZD_Category = df_array[j]['data_Source'] == 'Zendesk'

    df_array[j].loc[Mask_Multiple == True, 'Final_Category'] = "0_Multiple_Contacts"
    df_array[j].loc[(Mask_Multiple == False) & (Mask_Trasfered_Dropped == True) , 'Final_Category'] = "1_Transfers_Dropped_Silent"
    df_array[j].loc[(Mask_Multiple == False) & (Mask_Trasfered_Dropped == False) & (Mask_ZD_Category == True)  , 'Final_Category'] = df_array[j]['ZD_Category']
    df_array[j].loc[(Mask_Multiple == False) & (Mask_Trasfered_Dropped == False )& (Mask_ZD_Category == False)  , 'Final_Category'] = '8_Live_Channel_Single_Contact'


        


# # Consider Column Creation

# In[24]:


grid_dict_Consider={}
grid_dict_Consider["0_Multiple_Contacts"]=['2nd One Reopen', 'Within threshold to be excluded'] 
grid_dict_Consider["1_Transfers_Dropped_Silent"]=[1, 1] 
grid_dict_Consider["2_Zendesk_Reopen"]=[1, 1] 
grid_dict_Consider["3_Zendesk_On_Hold"]=[0, 0] 
grid_dict_Consider["4_Zendesk_Pending"]=[1, 1]
grid_dict_Consider["5_Zendesk_Solved"]=[0, 1]
grid_dict_Consider["6_Zendesk_Not_Worked"]=[0, 0]
grid_dict_Consider["7_Zendesk_Worked_New_Open"]=[0, 1] 
grid_dict_Consider["8_Live_Channel_Single_Contact"]=[0, 1] 
for j in range(0,2):
    df_array[j]['Consider'] = np.nan
    Mask = (df_array[j]['Multiple_Contacts'] == 1) & (df_array[j]['Multiple_Contacts (Thres Adjusted)']== 0) 
    Converted_DF = pd.DataFrame(df_array[j]["Final_Category"].map(grid_dict_Consider).values.tolist())
    Consider_Only = Converted_DF.iloc[:,1]

    df_array[j]['Consider'] = Consider_Only
    df_array[j].loc[(df_array[j]['Final_Category'] == "0_Multiple_Contacts") & (Mask == True), 'Consider'] = 0
    df_array[j].loc[(df_array[j]['Final_Category'] == "0_Multiple_Contacts") & (Mask == False), 'Consider'] = 1



            
  
            
    


# # Reopen Column Creation

# In[25]:


for j in range(0,2):
    Converted_DF = pd.DataFrame(df_array[j]["Final_Category"].map(grid_dict_Consider).values.tolist())
    Reopen_Only = Converted_DF.iloc[:,0]
    df_array[j].loc[(df_array[j]['Final_Category'] == "0_Multiple_Contacts") & (df_array[j]['Consider'] == 0), 'Reopen'] = 0
    df_array[j].loc[(df_array[j]['Final_Category'] == "0_Multiple_Contacts") & (df_array[j]['Consider'] == 1), 'Reopen'] = df_array[j]['Multiple_Contacts (Thres Adjusted)']
    df_array[j].loc[(df_array[j]['Final_Category'] != "0_Multiple_Contacts"), 'Reopen'] = Reopen_Only
    # df_array[j]['Reopen']


# # Contact_Count Column Creation

# In[26]:


for j in range(0,2):
    df_array[j]['Contact_Count']=((df_array[j]['Multiple_Contacts'] == 1) & (df_array[j]['Multiple_Contacts (Thres Adjusted)'] == 0)).apply(lambda x: 1 if x == False else 0)


# # Final_Reopen Column Creation

# In[27]:


for j in range(0,2):
    Mask_0_contact = df_array[j]['Contact_Count'] == 0
    Mask_Disposition = pd.isnull(df_array[j]['Disposition_Category']) == False
    df_array[j]['Final_Reopen'] = np.nan
    df_array[j]['Final_Reopen'].loc[Mask_0_contact == True] = 0
    df_array[j].loc[(Mask_0_contact == False) & (Mask_Disposition == True), 'Final_Reopen'] = 1
    df_array[j].loc[(Mask_0_contact == False) & (Mask_Disposition == False), 'Final_Reopen'] = df_array[j]['Multiple_Contacts (Thres Adjusted)'] 

  


# # Export Customer and Captain files with all Columns

# In[ ]:


# df_array[0].to_excel('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/Validation/Customer_Code_Result.xlsx', index =False , sheet_name='Sheet1')
# df_array[1].to_excel('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/Validation/Captain_Code_Result.xlsx', index =False , sheet_name='Sheet1')


# # Export Customer and Captain FCR Results to Excel file 

# In[53]:


country_Group = df_array[0].groupby('media')
country_Series = country_Group['Reopen'].sum() / country_Group['Consider'].sum()
FCR = country_Series*100
writer = pd.ExcelWriter('C:/Users/momen.jamil/Desktop/Careem/Work/Lovish/FCR/Week 48/Validation/FCR_Results.xlsx', engine='xlsxwriter')
FCR.to_excel(writer, sheet_name='Customer_FCR', index=True)
country_Group = df_array[1].groupby('media')
country_Series = country_Group['Reopen'].sum() / country_Group['Consider'].sum()
FCR = country_Series*100
FCR.to_excel(writer, sheet_name='Captain_FCR', index=True)
writer.save()

