import pandas as pd 
import pyTigerGraph as tg 

host = "https://3314d527106244578c3eff59e7a1ce42.i.tgcloud.io"
graphname = "MLCC_Lab"
username = "user_2"
password = "Tb1Yb8Kc6Vt6Jf3_"

conn = tg.TigerGraphConnection(host=host, graphname=graphname, username=username, password=password)
conn.apiToken = conn.getToken(secret)
print("TOKEN: ", conn.apiToken)

data = pd.read_csv("/Users/gideoncrawley/MSBA/MSBA/Machine Learning 5505/Chapter_1_cleaned_data.csv")
data = data.head()

def create_month_vertex(month):
    month_id = f"{month}"
    attributes = {
        "name": f"{month}"
    }
    conn.upsertVertex("Month", month_id, attributes)
    return(month_id)

def create_billing_vertex(bill_id, bill_amt):
    billing_id = f"{bill_id}"
    attributes = {
        "bill_amt": bill_amt
    }
    conn.upsertVertex("Billing", billing_id, attributes)
    return(billing_id)

def populate_month(month):
    month_id = month
    name = month
    month_id = create_month_vertex(month)
    return(month_id)

def populate_billing(data, month):
    billing_id = f"{data['ID']}-{month}"
    if month == "April":
        bill_amt = int(data['BILL_AMT6'])
    elif month == "May":
        bill_amt = int(data['BILL_AMT5'])
    elif month == "June":
        bill_amt = int(data['BILL_AMT4'])
    elif month == "July":
        bill_amt = int(data['BILL_AMT3'])
    elif month == "August":
        bill_amt = int(data['BILL_AMT2'])
    elif month == "September":
        bill_amt = int(data['BILL_AMT1'])

    bill_id = create_billing_vertex(billing_id, bill_amt)
    conn.upsertEdge("Month", f"{month}", "invoiced", "Billing", f"{billing_id}")
    return(billing_id)


def populate(data):
    month_list = ['April', 'May', 'June', 'July', 'August', 'September']
    for month in month_list:
        month = populate_month(month)
        for index, row in data.iterrows():
            billing_id = populate_billing(row, month)

populate(data)