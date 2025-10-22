import mysql.connector as msc
import pandas as pd
from sqlalchemy import create_engine

oltp_config = {
    'host' : '35.222.34.151',
    'user' : 'team3',
    'password' : 't3ssss_DP',
    'database' : 'OLTP'
}

olap_config = {
    'host' : '35.222.34.151',
    'user' : 'team3',
    'password' : 't3ssss_DP',
    'database' : 'Team3'
}

def get_connection(config):
    return msc.connect(**config)

def injest_data():
    con = get_connection(oltp_config)
    cur = con.cursor()

    cur.execute(
    '''
    select * from customers
    '''
    )
    customers = pd.DataFrame(cur.fetchall(), columns = ['customer_id', 'first_name', 'last_name', 'email', 'phone', 'phone_norm', 'city', 'state', 'signup_date', 'is_active', 'external_id', 'guest_flag', 'created_at', 'updated_at'])
    print(customers)


    cur.execute(
    '''
    select * from products
    '''
    )
    products = pd.DataFrame(cur.fetchall(), columns = ['product_id', 'product_name', 'product_category_raw', 'product_category', 'list_price', 'cost_price', 'is_active', 'created_at', 'updated_at'])
    print(products)

    cur.execute(
    '''
    select * from orders
    '''
    )
    orders = pd.DataFrame(cur.fetchall(), columns = ['order_id', 'customer_id', 'order_date', 'status', 'order_placed_at', 'payment_initiated_at', 'payment_completed_at', 'shipped_at', 'delivered_at', 'channel', 'user_agent', 'device_type', 'utm_source', 'coupon_code', 'applied_promo_id', 'order_value', 'num_items', 'ship_address_id', 'bill_address_id', 'created_at', 'updated_at' ])
    print(orders)


    cur.execute(
    '''
    select * from order_items
    '''
    )
    order_items = pd.DataFrame(cur.fetchall(), columns = ['order_item_id', 'order_id', 'product_id', 'quantity','unit_price','line_total', 'is_bundle_item', 'bundle_id'])
    print(order_items)


    cur.execute(
    '''
    select * from returns
    '''
    )
    returns = pd.DataFrame(cur.fetchall(), columns = ['return_id', 'order_id', 'order_item_id', 'returned_date', 'reason_code','refund_amount', 'processed_by', 'processed_at'])
    print(returns)


    cur.execute(
    '''
    select * from payments
    '''
    )
    payments = pd.DataFrame(cur.fetchall(), columns = ['payment_id', 'order_id', 'payment_method', 'amount', 'payment_date', 'is_refunded', 'refund_txn_id'])
    print(payments)
    return customers, products, orders, order_items, returns, payments

def clean_phone(ph):
    s = ''
    for c in ph:
        if c.isdigit():
            s+= c
    s = s[-10:]
    return s


category_map = {'ks' : 'books', 'eau':'beauty', 'me':'home', 'ery': 'grocery', 'rts': 'sports', 'ect': 'electronics', 'ys': 'toys', 'rel': 'apparel'}
def clean_category(ctg):
    for k,v in category_map.items():
        if k.lower() in ctg.lower():
            return v

def transform_data(customers, products):
    customers['phone_new'] = customers['phone'].apply(clean_phone)
    products['category_new'] = products['product_category_raw'].apply(clean_category)
    return customers, products


def load_data(customers, products,  orders, order_items, returns, payments):
    con = get_connection(olap_config)
    dim_customers = customers[['customer_id', 'first_name', 'last_name', 'email', 'phone_new','city', 'state', 'signup_date']]
    dim_products = products[['product_id', 'product_name', 'category_new', 'list_price', 'cost_price', 'is_active', 'created_at']]
    order_fact = pd.merge(orders, order_items, on = ['order_id'], how='left')
    order_fact = pd.merge(order_fact, returns, on = ['order_id'], how='left')
    order_fact = pd.merge(order_fact, payments, on = ['order_id'], how='left')
    print(order_fact)

    engine = create_engine('mysql+pymysql://team3:t3ssss_DP@35.222.34.151:3306/Team3')
    dim_products.to_sql('dim_products', engine, if_exists='append', index=False)
    dim_customers.to_sql('dim_customers', engine, if_exists='append', index=False)
    order_fact = order_fact[['order_id', 'customer_id', 'order_date', 'status', 'order_placed_at', 'channel', 'order_value', 'num_items', 'order_item_id', 'product_id', 'quantity', 'unit_price', 'line_total', 'payment_id', 'payment_method', 'amount', 'payment_date', 'is_refunded', 'returned_id', 'return_date', 'reason_code', 'refund_amount']]
    order_fact.to_sql('order_fact', engine, if_exists='append', index=False)
    print('Loading Complete!   ')

customers, products, orders, order_items, returns, payments = injest_data()
customers, products = transform_data(customers, products)
load_data(customers, products, orders, order_items, returns, payments)
