import pandas as pd 
import pyTigerGraph as tg 

host = "https://3314d527106244578c3eff59e7a1ce42.i.tgcloud.io"
graphname = "MLCC_Lab"
username = "user_2"


conn = tg.TigerGraphConnection(host=host, graphname=graphname, username=username, password=password)
conn.apiToken = conn.getToken(secret)
print("TOKEN: ", conn.apiToken)

data = pd.read_csv("/Users/gideoncrawley/MSBA/MSBA/Machine Learning 5505/data/Chapter_1_cleaned_data.csv")
data = data.head()

def create_month_vertex(month):
    month_id = f"{month}"
    attributes = {
        "name": f"{month}"
    }
    conn.upsertVertex("Month", month_id, attributes)
    return(month_id)

def create_account_vertex(account_id, limit_bal, status):
    account_id = f"{account_id}"
    attributes = {
        "limit_bal": limit_bal,
        "status" : status
    }
    conn.upsertVertex("Account", account_id, attributes)
    return(account_id)

def create_customer_vertex(customer_id, sex, age):
    customer_id = f"{customer_id}"
    attributes = {
        "sex": "male" if sex == 1 else "female",
        "age": age,
    }
    conn.upsertVertex("Customer", customer_id, attributes)
    return(customer_id)

def create_billing_vertex(bill_id, bill_amt):
    billing_id = f"{bill_id}"
    attributes = {
        "bill_amt": bill_amt
    }
    conn.upsertVertex("Billing", billing_id, attributes)
    return(billing_id)

def create_payment_vertex(payment_id, pay_amt):
    payment_id = f"{payment_id}"
    attributes = {
        "pay_amt": pay_amt
    }
    conn.upsertVertex("Payment", payment_id, attributes)
    return(payment_id)

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

def populate_customer(data):
    customer_id = f"{data['ID']}"
    sex = f"{data['SEX']}"
    age = int(data['AGE'])
    customer_id = create_customer_vertex(customer_id, sex, age)
    return(customer_id)

def populate_account(data):
    account_id = f"{data['ID']}"
    limit_bal = int(data['LIMIT_BAL'])
    status = bool(data['default payment next month'])
    account_id = create_account_vertex(account_id, limit_bal, status)
    return(account_id)

    payment_id = f"{data['ID']}-{month}"
    if month == "April":
        pay_amt = int(data['PAY_AMT6'])
    elif month == "May":
        pay_amt = int(data['PAY_AMT5'])
    elif month == "June":
        pay_amt = int(data['PAY_AMT4'])
    elif month == "July":
        pay_amt = int(data['PAY_AMT3'])
    elif month == "August":
        pay_amt = int(data['PAY_AMT2'])
    elif month == "September":
        pay_amt = int(data['PAY_AMT1'])
    payment_id = create_payment_vertex(payment_id, pay_amt)
    return(payment_id)

def populate_event(sender_id, receiver_id, sender_type, receiver_type, t):
    event_id = str(t)

    if sender_type =="Month" and receiver_type == "Billing":
        attributes = {
            "event_type": "Monthly Billing Event",
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "sender_type": sender_type,
            "receiver_type": receiver_type
        }
        conn.upsertVertex("Event", event_id, attributes)
        conn.upsertEdge("Event", event_id, "month_event", "Month", sender_id)
        conn.upsertEdge("Event", event_id, "billing_event", "Billing", receiver_id)

    if sender_type =="Billing" and receiver_type == "Customer":
        attributes = {
            "event_type": "Customer Billing Event",
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "sender_type": sender_type,
            "receiver_type": receiver_type
        }
        conn.upsertVertex("Event", event_id, attributes)
        conn.upsertEdge("Event", event_id, "billing_event", "Billing", sender_id)
        conn.upsertEdge("Event", event_id, "customer_event", "Customer", receiver_id)
    
    if sender_type =="Billing" and receiver_type == "Account":
        attributes = {
            "event_type": "Account Billing Event",
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "sender_type": sender_type,
            "receiver_type": receiver_type
        }
        conn.upsertVertex("Event", event_id, attributes)
        conn.upsertEdge("Event", event_id, "billing_event", "Billing", sender_id)
        conn.upsertEdge("Event", event_id, "account_event", "Account", receiver_id)

    if sender_type =="Customer" and receiver_type == "Account":
        attributes = {
            "event_type": "Account Customer Event",
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "sender_type": sender_type,
            "receiver_type": receiver_type
        }
        conn.upsertVertex("Event", event_id, attributes)
        conn.upsertEdge("Event", event_id, "customer_event", "Customer", sender_id)
        conn.upsertEdge("Event", event_id, "account_event", "Account", receiver_id)

    if sender_type =="Account" and receiver_type == "Payment":
        attributes = {
            "event_type": "Payment Account Event",
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "sender_type": sender_type,
            "receiver_type": receiver_type
        }
        conn.upsertVertex("Event", event_id, attributes)
        conn.upsertEdge("Event", event_id, "account_event", "Account", sender_id)
        conn.upsertEdge("Event", event_id, "payment_event", "Payment", receiver_id)

    if t != 0:
        prev_id = int(t)-1
        conn.upsertEdge("Event", event_id, "event_edge", "Event", str(prev_id))


def populate(data):
    month_list = ['April', 'May', 'June', 'July', 'August', 'September']
    payment_id = ""
    t = 0
    for month in month_list:
        month = populate_month(month)
        for index, row in data.iterrows():
            # Populate data for each entity
            # Billing Event
            billing_id = populate_billing(row, month)
            populate_event(month, billing_id, "Month", "Billing", t)
            t +=- 1

            # Customer Event
            customer_id = populate_customer(row, billing_id)
            populate_event(billing_id, customer_id, "Billing", "Customer", t)
            t += 1

            # Account Event
            account_id = populate_account(row, customer_id, billing_id)
            populate_event(billing_id, account_id, "Billing", "Account", t)
            t += 1
            populate_event(customer_id, account_id, "Customer", "Account", t)
            t += 1

            # Payment Event
            payment_id = populate_payment(row, account_id, month)
            populate_event(account_id, payment_id, "Account", "Payment", t)
            t += 1

def check_data(account_id, month, pay, pay_amt):
    if month == "May":
        prev_payment = conn.getVerticesbyId("Payment", f"(account_id)-April")
        prev_billing = conn.getVerticesbyId("Billing", f"{account_id}-April")
        curr_billing = conn.getVerticesbyId("Billing", f"{account_id}-May")
    elif month == "June":
        prev_payment = conn.getVerticesbyId("Payment", f"(account_id)-May")
        prev_billing = conn.getVerticesbyId("Billing", f"{account_id}-May")
        curr_billing = conn.getVerticesbyId("Billing", f"{account_id}-June")
    elif month == "July":
        prev_payment = conn.getVerticesbyId("Payment", f"(account_id)-June")
        prev_billing = conn.getVerticesbyId("Billing", f"{account_id}-June")
        curr_billing = conn.getVerticesbyId("Billing", f"{account_id}-July")
    elif month == "August":
        prev_payment = conn.getVerticesbyId("Payment", f"(account_id)-July")
        prev_billing = conn.getVerticesbyId("Billing", f"{account_id}-July")
        curr_billing = conn.getVerticesbyId("Billing", f"{account_id}-August")
    elif month == "September":
        prev_payment = conn.getVerticesbyId("Payment", f"(account_id)-August")
        prev_billing = conn.getVerticesbyId("Billing", f"{account_id}-August")
        curr_billing = conn.getVerticesbyId("Billing", f"{account_id}-September")
    
    print(prev_billing)    

def correct_data(pay, pay_amt):
    # for index, row in data.iterrows():
    if prev_billing[0]['attributes']['Bill_Amt'] == 0 and curr_billing[0]['attributes']['Bill_Amt'] == 0:
        pay = -2
    elif curr_billing[0]['attributes']['Bill_Amt'] == 0:
        pay = -1
    # correct_data = {
    #     "pay": pay,
    #     "pay_amt": pay_amt
    # }

populate(data)

