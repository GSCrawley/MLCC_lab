import pyTigerGraph as tg

host = "https://3314d527106244578c3eff59e7a1ce42.i.tgcloud.io"
graphname = "MLCC_Lab"
username = "user_1"
password = "Tb1Yb8Kc6Vt6Jf3_"
secret = "s800no94cutspdqlaae55qfurvr7hsf1"

conn = tg.TigerGraphConnection(host=host, graphname=graphname, username=username, password=password)
conn.apiToken = conn.getToken(secret)
print("TOKEN: ", conn.apiToken)

def create_account_vertex():
    account_id = "798fc410-45c1"
    attributes = {
        "limit_bal": 0,
        "status" : True
    }
    conn.upsertVertex("Account", account_id, attributes)
    return(account_id)           


def create_month_vertex():
    month_id = "April"
    attributes = {
        "name": "April"
    }
    conn.upsertVertex("Month", month_id, attributes)
    return(month_id)

def create_billing_vertex():
    billing_id =  "April-1"
    attributes = {
        "bill_amt": 0
    }
    conn.upsertVertex("Billing", billing_id, attributes)
    return(billing_id)

def create_edge():
    month_id = create_month_vertex()
    billing_id = create_billing_vertex()
    account_id = create_account_vertex()
    conn.upsertEdge("Billing", f"{billing_id}" , "billed", "Account", f"{account_id}")

# create_account_vertex()
# create_month_vertex()
# create_billing_vertex()
create_edge()