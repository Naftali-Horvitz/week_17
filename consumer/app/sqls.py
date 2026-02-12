from mysql_connection import get_conn
import logging

mydb = get_conn()
mycursor = mydb.cursor()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_customer_table():
    return """
    CREATE TABLE IF NOT EXISTS customers 
        (
        type VARCHAR(255),
        customerNumber int NOT NULL PRIMARY KEY,
        customerName VARCHAR(255),
        contactLastName VARCHAR(255),
        contactFirstName VARCHAR(255),
        phone VARCHAR(255),
        addressLine1 VARCHAR(255),
        addressLine2 VARCHAR(255),
        city VARCHAR(255),
        state VARCHAR(255),
        postalCode VARCHAR(255),
        country VARCHAR(255),
        salesRepEmployeeNumber int,
        creditLimit VARCHAR(255)    
        )
    """

def create_order_table():
    return """
    CREATE TABLE IF NOT EXISTS orders 
        (
        type VARCHAR(255),
        orderNumber int not null,
        orderDate VARCHAR(255),
        requiredDate VARCHAR(255),
        shippedDate VARCHAR(255),
        status VARCHAR(255),
        comments VARCHAR(255),
        customerNumber int not null
        )
        """

def create_sql_customers():
    return """
INSERT INTO customers 
(type, orderNumber, orderDate, requiredDate, shippedDate, status, comments)
VALUES (%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s, %s, %s,%s)
"""

def create_sql_orders():
    return """INSERT INTO orders
        (
        type, customerNumber, customerName, contactLastName, contactFirstName,
        phone, addressLine1, addressLine2, city, state, postalCode, country,
        salesRepEmployeeNumber 
        creditLimit
        )
        VALUES (%s, %s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s, %s,%s)
        """

def create_val_(item: dict):
    values = []
    for _, v in item.items():
        values.append(v)
    return tuple(values)

def insert_item(item):
    my_conn = get_conn()
    cursor = my_conn.cursor()
    
    if item["type"] == "order":
        sql = create_sql_orders()
    else:
        sql = create_sql_customers()
        
    value = create_val_(item)

    cursor.execute(sql, value)
    my_conn.commit()
    my_conn.close()