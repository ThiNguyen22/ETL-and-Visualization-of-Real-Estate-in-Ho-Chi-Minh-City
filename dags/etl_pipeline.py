import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow import DAG
from airflow.operators.python import PythonOperator

# Extract data from website
def crawl_data():
    base_url = "https://batdongsan.com.vn/ban-can-ho-chung-cu-tp-hcm"
    max_pages = 5
    chrome_driver_path = r"C:\Users\Lenovo\Downloads\chromedriver-win64\chromedriver.exe"
    service = Service(executable_path=chrome_driver_path)
    driver = webdriver.Chrome(service=service)

    data = []
    
    for page_number in range(1, max_pages + 1):
        url = f"{base_url}/p{page_number}" if page_number > 1 else base_url
        driver.get(url)
        print(f"Scraping page {page_number}")
        
        try:
            WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "re__card-info"))
            )
        except TimeoutException:
            print(f"Page {page_number} took too long to load, skipping...")
            continue

        listings = driver.find_elements(By.CLASS_NAME, "re__card-info")
        for listing in listings:
            try:
                title = listing.find_element(By.CSS_SELECTOR, "h3.re__card-title").text
                price = listing.find_element(By.CSS_SELECTOR, "span.re__card-config-price").text
                area = listing.find_element(By.CSS_SELECTOR, "span.re__card-config-area").text
                price_per_m2 = listing.find_element(By.CSS_SELECTOR, "span.re__card-config-price_per_m2").text
                bedroom = listing.find_element(By.CSS_SELECTOR, "span.re__card-config-bedroom").text
                bathroom = listing.find_element(By.CSS_SELECTOR, "span.re__card-config-toilet").text
                location = listing.find_element(By.CSS_SELECTOR, "div.re__card-location span:nth-child(2)").text
                date_element = listing.find_element(By.CLASS_NAME, "re__card-published-info-published-at")
                date = date_element.get_attribute("aria-label")

                data.append({
                    'Tiêu đề': title,
                    'Giá': price,
                    'Diện Tích': area,
                    'Giá/m2': price_per_m2,
                    'Số phòng ngủ': bedroom,
                    'Số phòng tắm': bathroom,
                    'Địa chỉ': location,
                    'Thời gian': date
                })
            except Exception as e:
                print(f"Error retrieving data: {e}")
                continue

    driver.quit()
    df = pd.DataFrame(data)
    df.to_csv("raw_data_realestate.csv")
    return df


def convert_price(price):
    price = str(price)

    if 'triệu' in price:
        price = float(price.replace('triệu', '').replace(',', '.').strip())
        return price/1000
    if 'tỷ' in price:
        price = float(price.replace('tỷ', '').replace(',', '.').strip())
        return price
    if 'tr/m²' in price:
        price = float(price.replace('tr/m²', '').replace(',', '.').strip())
        return price
    if 'nghìn/m²' in price:
        price = float(price.replace('nghìn/m²', '').replace(',', '.').strip())
        return price/1000
    if 'nghìn' in price:
        price = float(price.replace('nghìn', '').replace(',', '.').strip())
        return price/1000000


def transform():
    df = pd.read_csv("raw_data_realestate.csv")
    df = df.drop_duplicates()
    df = df.dropna(subset=['Số phòng tắm'])
    df['Giá'] = df['Giá'].apply(convert_price)
    df['Giá/m2'] = df['Giá/m2'].apply(convert_price)
    df['Diện Tích'] = df['Diện Tích'].str.replace('m²', '').str.replace(',', '.').astype(float)
    df['Số phòng ngủ'] = df['Số phòng ngủ'].astype(float)
    df['Số phòng tắm'] = df['Số phòng tắm'].astype(float)
    df['Thời gian'] = pd.to_datetime(df['Thời gian'], format='%d/%m/%Y')
    df['Thời gian'].fillna(method='ffill', inplace=True)
    df.rename(columns={'Giá': 'Giá (tỷ)', 'Giá/m2': 'Giá/m2 (tr/m2)', 'Diện Tích': 'Diện tích (m2)'}, inplace = True)
    df.to_csv("cleaned_data_realestate.csv")
    return df

# Load data to mysql

def load_to_mysql():
    conn = create_engine('mysql+mysqlconnector://root:hongthi2305@localhost:3306/RealEstate')
    df = pd.read_csv("cleaned_data_realestate.csv")
    df.to_sql(name='realestate', con=conn , index=False, if_exists='append')
   
default_args = {
    'email': ['nguyenthihongthi.230502@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'real_estate_etl',
    default_args = default_args,
    description = 'A DAG for Real Estate ETL',
    schedule_interval = '@daily',
    start_date=datetime(2024, 10, 8),
    catchup = False
)

crawl_operator = PythonOperator(
    task_id = 'crawl_data',
    python_callable = crawl_data,
    dag = dag
)

transform_operator = PythonOperator(
    task_id = 'transform_data',
    python_callable = transform,
    dag = dag
)

load_to_mysql_operator = PythonOperator(
    task_id = 'load_to_mysql',
    python_callable = load_to_mysql,
    dag = dag
)

crawl_operator >> transform_operator >> load_to_mysql_operator




 

