import requests.auth
import binascii
import hashlib
import hmac
import pyodbc
from pathlib import Path
from datetime import datetime
import time

#Creds contains api keys and other sensitive info so is not included
import creds

class UnleashedAPI(requests.auth.AuthBase):
    def __init__(self):
        self.api_key = creds.api_key.encode(
            'utf-8')
        self.api_id = creds.api_id
        self.api_url = 'https://api.unleashedsoftware.com'

    def get_query(self, url):
        parts = url.split('?')
        if len(parts) > 1:
            return parts[1]
        else:
            return ""

    def __call__(self, r):
        query = self.get_query(r.url)

        hashed = hmac.new(self.api_key, query.encode('utf-8'), hashlib.sha256)
        signature = binascii.b2a_base64(hashed.digest())[:-1]
        r.headers['api-auth-signature'] = signature
        r.headers['api-auth-id'] = self.api_id

        return r

    def _get_request(self, method, params=None):
        params = params or {}
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json',
        }

        resp = requests.get(
            self.api_url + '/' + method,
            headers=headers,
            params=params,
            auth=self
        )
        return resp

    def _post_request(self, method, data):
        headers = {
            'content-type': 'application/json',
            'accept': 'application/json',
        }

        resp = requests.post(
            self.api_url + '/' + method,
            data,
            headers=headers,
            auth=self
        )

        return resp

    #Page numbers are passed into the url so "invoices"/"customers"/etc then "/pagenumber" then add paramters for page size (1000 max)
    def get_credits(self, page_no):
        resp = self._get_request('CreditNotes/' + str(page_no), params=dict(pageSize=1000))
        json_parsed = resp.json()
        return json_parsed['Items']

    def get_invoices(self, page_no):
        resp = self._get_request('Invoices/' + str(page_no), params=dict(pageSize=1000))
        json_parsed = resp.json()
        return json_parsed['Items']

class Logger:
    def __init__(self):
        self.file_name = 'logs.txt'
        self.file_path = Path(__file__).resolve().parent
        self.full_path = str(self.file_path) + "\\" + self.file_name

        self.log("Running at " + str(datetime.now()))
        self.start_time = time.time()
        
    def log(self, log):
        with open(self.full_path, "a") as f: 
            f.write(str(log) + "\n")

    def stop_time(self):
        self.stop_time = time.time()
        elapsed = self.stop_time - self.start_time
        self.log("Took " + str(elapsed) + "s")
        self.log("---------------------------------")


class DBConnection:
    def __init__(self):
        self.server = creds.server
        self.database = creds.database
        self.username = creds.username
        self.password = creds.password
        self.cnn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.server + ';DATABASE='+self.database+';UID='+self.username+';PWD=' + self.password)
        self.cursor = self.cnn.cursor()

    def get_version(self):
        self.cursor.execute("SELECT @@version;")
        row = self.cursor.fetchone()
        while row:
            print(row[0])
            row = self.cursor.fetchone()

    def get_all_guid_invoices(self):
        self.cursor.execute("SELECT Guid FROM UnleashedInvoices;")
        self.invoice_guids = []
        row = self.cursor.fetchone()
        while row:
            self.invoice_guids.append(row[0])
            row = self.cursor.fetchone()

    def get_all_guid_credit(self):
        self.cursor.execute("SELECT Guid FROM UnleashedCreditNotes;")
        self.credit_guids = []
        row = self.cursor.fetchone()
        while row:
            self.credit_guids.append(row[0])
            row = self.cursor.fetchone()

    def insert_credit(self, data, _logger):
        rows = 0
        for i in range(0, len(data)):
            for credit_line in data[i]["CreditLines"]:
                if credit_line["Guid"] not in self.credit_guids:
                    self.cursor.execute("""
                    INSERT INTO UnleashedCreditNotes (CreditNoteNumber, InvoiceNumber, CreditStatus, CustomerCode, CustomerName, Total, ProductCode, CreditQuantity, CreditDate, Guid) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", data[i]["CreditNoteNumber"], data[i]["InvoiceNumber"], data[i]["Status"], data[i]["Customer"]["CustomerCode"], data[i]["Customer"]["CustomerName"],
                    data[i]["Total"], credit_line["Product"]["ProductCode"], credit_line["CreditQuantity"], data[i]["CreditDate"], credit_line["Guid"])
                    self.cnn.commit()
                    rows += 1
        _logger.log("Inserted " + str(rows) + " rows into UnleashedCreditNotes")

    def insert_invoices(self, data, _logger):
        rows = 0
        for i in range(0, len(data)):
            for invoice_line in data[i]["InvoiceLines"]:
                if invoice_line["Guid"] not in self.invoice_guids:
                    self.cursor.execute("""
                    INSERT INTO UnleashedInvoices (InvoiceNumber, OrderNumber, InvoiceDate, InvoiceStatus, CustomerCode, CustomerName, Total, ProductCode, OrderQuantity, UnitPrice, DiscountRate, Guid)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", data[i]["InvoiceNumber"], data[i]["OrderNumber"], data[i]["InvoiceDate"], data[i]["InvoiceStatus"], data[i]["Customer"]["CustomerCode"], data[i]["Customer"]["CustomerName"],
                    data[i]["Total"], invoice_line["Product"]["ProductCode"], invoice_line["OrderQuantity"], invoice_line["UnitPrice"],
                    invoice_line["DiscountRate"], invoice_line["Guid"])
                    self.cnn.commit()
                    rows += 1
        _logger.log("Inserted " + str(rows) + " rows into UnleashedInvoices")

#These two functions handle getting all pages
#It will iterate through pages checking the object count returned
#If there is 1000 objects it is a full page and will move to the next
#If there's less than 1000 then it's the last page and it'll break the loop
#This then runs the function to insert the data from each page into our db
def run_credits(_api, _db, _logger):
    credits = []

    page = 1
    credit = _api.get_credits(page)
    while True:
        credits.append(credit)
        if len(credit) == 1000:
            page += 1
            credit = _api.get_credits(page)
        else:
            break

    for credit in credits:
        _db.insert_credit(credit, _logger)

def run_invoices(_api, _db, _logger):
    invoices = []

    page = 1
    invoice = _api.get_invoices(page)
    while True:
        invoices.append(invoice)
        if len(invoice) == 1000:
            page += 1
            invoice = _api.get_invoices(page)
        else: 
            break

    for invoice in invoices:
        _db.insert_invoices(invoice, _logger)

if __name__ == '__main__':
    _api = UnleashedAPI()
    _db = DBConnection()
    _logger = Logger()

    _db.get_version()
    _db.get_all_guid_credit()
    _db.get_all_guid_invoices()

    run_credits(_api, _db, _logger)
    run_invoices(_api, _db, _logger)
    
    _logger.stop_time()
    
