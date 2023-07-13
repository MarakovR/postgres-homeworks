"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv


def open_file(name_file):
    file = open(f'../homework-1/north_data/{name_file}.csv', encoding='utf-8')
    r_file = csv.DictReader(file)
    return r_file


conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="roman",
    password="13243546"
)

for i in open_file('employees_data'):
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)',
        (i['employee_id'], i['first_name'], i['last_name'], i['title'], i['birth_date'], i['notes']))
    cur.execute('SELECT * FROM employees')
    conn.commit()
    rows = cur.fetchall()

for i in open_file('customers_data'):
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO customers VALUES (%s, %s, %s)',
        (i['customer_id'], i['company_name'], i['contact_name']))
    cur.execute('SELECT * FROM customers')
    conn.commit()
    rows = cur.fetchall()

for i in open_file('orders_data'):
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
        (i['order_id'], i['customer_id'], i['employee_id'], i['order_date'], i['ship_city']))
    cur.execute('SELECT * FROM orders')
    conn.commit()
    rows = cur.fetchall()
