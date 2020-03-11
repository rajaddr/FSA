import pandas as pd
import os

import pymysql.cursors
import pymysql

Path = os.getcwd()
Path = "D:\\BABI\\Capstone\\DataSet Full"





#print(Path)

files = []
for r, d, f in os.walk(Path):
    for file in f:
        if '.xls' in file:
            if 'DL_Dashboard_AY' in file:
                files.append(os.path.join(r, file))

F_Dt = []


for f in files:
    #print(f)
    print(f)
    if '-' in f:
        rpt_Year = os.path.splitext(os.path.basename(f))[0].split('-')[1]
        rpt_Q = os.path.splitext(os.path.basename(f))[0].split('-')[2].replace('q', '').replace('Q', '')
    else:
        rpt_Year = os.path.splitext(os.path.basename(f))[0].split('_')[3]
        rpt_Q = os.path.splitext(os.path.basename(f))[0].split('_')[4].replace('q', '').replace('Q', '')
    

    data_H = pd.read_excel(f,sheet_name ='Quarterly Activity', skiprows=4)
    data = pd.read_excel(f,sheet_name ='Quarterly Activity', skiprows=5)
    df = pd.DataFrame(data=data)
    
    head_List = list(data_H.columns.values)
    
    for t in head_List:
        if t.find("Unnamed"):
            valLoc = int(head_List.index(t))
            df.head(10)
            for loopIndex, loopData in list(df.iterrows()):  
                #print (loopData)
                try:
                    F_Dt.append((loopData[0], loopData[1], loopData[2], loopData[3], loopData[4], int(rpt_Year), int(rpt_Q), t, loopData[0 + valLoc], loopData[1 + valLoc ], loopData[2 + valLoc], loopData[3 + valLoc], loopData[4 + valLoc]))
                except:
                    print(loopData)
                    exit()
                #exit()
    

#new_Data = pd.DataFrame(F_Dt, columns =  ['OPE ID', 'School', 'State', 'Zip Code', 'School Type', 'Year', 'Quarter', 'Loan Type','Recipients', '# of Loans Originated', '$ of Loans Originated', '# of Disbursements', '$ of Disbursements'])

new_Data = pd.DataFrame(F_Dt, columns =  ['Code', 'School', 'State', 'Zip_Code', 'School_Type', 'Year', 'Quarter', 'Loan_Type', 'Recipients', 'No_Originated', 'Value_Originated', 'No_Disbursements', 'Value_Disbursements'])


new_Data.to_csv('file1.csv') 



db = pymysql.connect(host = "remotemysql.com", db = "jTwkfX19tK", user = "jTwkfX19tK", passwd = "NnGQ8Sd5tl")
cursor = db.cursor()
cursor.execute("TRUNCATE TABLE main_tbl")
db.commit()

#mysql -h database.cluster-csmseumkhkfj.ap-south-1.rds.amazonaws.com -u admin --port 3306 -p Rajaddr123$
#mysql -h database.cluster-csmseumkhkfj.ap-south-1.rds.amazonaws.com -u admin -p 

#exit()
    
SQL = """
    INSERT INTO `main_tbl`(`Code`, `School`, `State`, `Zip_Code`, `School_Type`, `Year`, `Quarter`, `Loan_Type`, `Recipients`, `No_Originated`, `Value_Originated`, `No_Disbursements`, `Value_Disbursements`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """ 


new_Data["State"].fillna(" ", inplace = True) 
new_Data["Zip_Code"].fillna(" ", inplace = True) 

F_Dt1=[]
for loopIndex, loopData in list(new_Data.iterrows()):  
    F_Dt1.append((loopData[0], loopData[1], loopData[2], loopData[3], loopData[4], loopData[5], loopData[6], loopData[7], loopData[8], loopData[9], loopData[10], loopData[11], loopData[12]))

cursor.executemany( SQL,F_Dt1)
db.commit()

exit()


t5 = 0
for loopF_Dt in F_Dt1:
    cursor.execute( SQL,(loopF_Dt))
    print(t5)
    t5 += 1
    
    
    
    
"""

    
    t5 += 1
    if t5 == 100:
        db.commit()
        t5 = 0
        print(1)


db.close()
exit()


"""