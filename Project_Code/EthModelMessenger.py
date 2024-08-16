#Import necessary libraries

import datetime
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from transformers import BertTokenizer, BertForSequenceClassification
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from telegram import Bot
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_batch
import psycopg2
import mplfinance as mpf
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
import aiofiles
import time
import matplotlib.pyplot as plt
from PIL import Image
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import asyncio
import schedule

#Load environment variables and configure database connection

load_dotenv()
host = 'localhost'
port = 5432 
user = 'postgres'
password = os.getenv('PASSWORD_DB')
db = 'postgres' 
conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db)
connection_string = f"postgresql://{user}:{password}@{host}/{db}"
engine = create_engine(connection_string)


# Define the function to send the daily report

def send_daily_report():

    BASE_URL = os.getenv('BASE_URL_Current')
    def get_current_price(symbol):
        params = {
            'symbol': symbol
        }
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return float(data['price'])
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None

    symbol = 'ETHUSDT'
    current_price = get_current_price(symbol)
        
    sql = "SELECT * FROM df_teleg"
    df = pd.read_sql(sql, engine)

    if df.iloc[-1]['pred'] == 0 and df.iloc[-1]['open'] >= current_price:
        df.at[df.index[-1], 'status'] = 1
    elif df.iloc[-1]['pred'] == 0 and df.iloc[-1]['open'] < current_price:
        df.at[df.index[-1], 'status'] = 0
    elif df.iloc[-1]['pred'] == 1 and df.iloc[-1]['open'] <= current_price:
        df.at[df.index[-1], 'status'] = 1
    elif df.iloc[-1]['pred'] == 1 and df.iloc[-1]['open'] > current_price:
        df.at[df.index[-1], 'status'] = 0

    scsess = len(df[df['status'] == 1])
    failed = len(df[df['status'] == 0])
    current_d = pd.Timestamp.now().strftime('%B %d, %Y')

    report_message = f"📅 Daily Report : {current_d }\n\n" \
                     f"✅Successful Predictions : {scsess}\n" \
                     f"❌Failed Predictions : {failed}\n"
    
    df.to_sql(name = 'df_teleg'
            , con = engine
            , index = False
            , if_exists ='replace')
    
    send_message(report_message)

BOT_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

def send_message(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("Message sent successfully")
    else:
        print(f"Failed to send message. Status code: {response.status_code}")

# Schedule the daily report at 12:00 PM
schedule.every().day.at("12:00").do(send_daily_report)



while True:
        
        #Fetch data from the database and display it

        sql = "SELECT * FROM df_teleg"
        df = pd.read_sql(sql,engine)
        print(df)


        #Define function to send messages via Telegram

        BOT_TOKEN = os.getenv('TELEGRAM_TOKEN')
        CHAT_ID = os.getenv('CHAT_ID')

        def send_message(message):
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            payload = {
                'chat_id': CHAT_ID,
                'text': message
            }
            response = requests.post(url, data=payload)
            if response.status_code == 200:
                print("Message sent successfully")
            else:
                print(f"Failed to send message. Status code: {response.status_code}")

        #Set up Selenium WebDriver for web scraping

        driver_path = "C:\\chromedriver-win64\\chromedriver.exe"
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service)
        links_dic = {"text": [], "time": []}

        #Define function to get links from CoinDesk

        def get_links(limit=372):
            page_links = []
            for page_number in range(1, limit + 1):
                try:
                    driver.get(f"https://www.coindesk.com/tag/ethereum/{page_number}")
                    elements = driver.find_elements(By.CLASS_NAME, "card-title")

                    for element in elements:
                        link = element.get_attribute("href")
                        if link and "video" not in link:
                            page_links.append(link)
                except WebDriverException as e:
                    continue
            return page_links

        #Define function to extract content and publication time from links

        def get_content(link):
            try:
                response = requests.get(link)
                soup = BeautifulSoup(response.content, 'html.parser')

                base_content = soup.find(attrs={"data-module-name": "article-body-no-right-rail"})
                if base_content:
                    paragraphs = base_content.find_all('p')
                    all_text = "\n".join(paragraph.get_text() for paragraph in paragraphs)
                else:
                    all_text = "Content not found"

                time_element = soup.find(class_="iOUkmj")
                time = time_element.get_text() if time_element else "Time not found"

                links_dic['text'].append(all_text)
                links_dic["time"].append(time)

            except Exception as e:
                print(f"Error fetching content from {link}: {e}")

        #Get links and extract content from the first page

        def get_contents(page_links):
            print(page_links[0])
            get_content(page_links[0])

        page_links = get_links(1)
        get_contents(page_links)

        last = pd.DataFrame(links_dic)
        driver.quit()

        #Clean and process the extracted content

        last = last[~last['text'].str.contains('Error fetching content from')]

        def convert_date(date_str):
            date_str_cleaned = date_str.split(' UTC')[0].strip()
            
            date_str_cleaned = date_str_cleaned.replace('p.m.', 'PM').replace('a.m.', 'AM')
            
            formats = [
                '%b %d, %Y at %I:%M %p',
                '%b %d, %Y at %H:%M %p'
            ]
            
            for fmt in formats:
                try:
                    date_obj = datetime.datetime.strptime(date_str_cleaned, fmt)
                    return date_obj.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            return None

        last['time'] = last['time'].apply(convert_date)

        last.dropna(inplace=True)

        last['time'] = pd.to_datetime(last['time'], format='%Y-%m-%d %H:%M:%S')
        last.rename(columns= {"time":"timestamp" , "text" : "content"}, inplace = True)

        #Define function to get OHLC price data from Binance API

        API_KEY = os.getenv('API_KEY_OHLC')
        BASE_URL = os.getenv('BASE_URL_OHLC')

        def get_binance_ohlc(symbol, interval, start_time, end_time):
            params = {
                'symbol': symbol,
                'interval': interval,
                'startTime': int(start_time.timestamp() * 1000),
                'endTime': int(end_time.timestamp() * 1000),
                'limit': 10000
            }
            response = requests.get(BASE_URL, params=params, headers={'X-MBX-APIKEY': API_KEY})
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error: {response.status_code}")
                print(response.text)
                return None

        #Define function to format OHLC data

        def format_data(data):
            formatted_data = []
            for item in data:
                timestamp = item[0] / 1000
                date_time = datetime.datetime.fromtimestamp(timestamp)
                formatted_data.append({
                    'datetime': date_time,
                    'open': float(item[1]),
                    'high': float(item[2]),
                    'low': float(item[3]),
                    'close': float(item[4])
                })
            return formatted_data

        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(hours=16)

        data = get_binance_ohlc('ETHUSDT', '1h', start_date, end_date)

        #Fetch and format OHLC price data

        if data:
            formatted_data = format_data(data)
            ohlc_df_m = pd.DataFrame(formatted_data)
        else:
            print("No data available or error in response.")

        ohlc_df_m.rename(columns={"datetime": "timestamp"}, inplace=True)

        #Merge news and OHLC data

        last = pd.merge_asof(last.sort_values('timestamp'), 
                                ohlc_df_m.sort_values('timestamp'), 
                                on='timestamp', 
                                direction='forward')


        #Define function to get current price from Binance API

        BASE_URL = os.getenv('BASE_URL_Current')

        def get_current_price(symbol):
            params = {
                'symbol': symbol
            }
            response = requests.get(BASE_URL, params=params)
            
            if response.status_code == 200:
                data = response.json()
                return float(data['price'])
            else:
                print(f"Error: {response.status_code}")
                print(response.text)
                return None

        #Get current price and print it

        symbol = 'ETHUSDT'
        current_price = get_current_price(symbol)

        print(current_price)

        #Check and update the dataset with new news

        def check(df, last):
            new_news = [] 
            if df["timestamp"].max() < last["timestamp"].min():
                new_news = last["content"].to_list()
                df = pd.concat([df, last], ignore_index=True)
            return df, new_news

        df, new_news = check(df, last)
        print(new_news)

        if len(new_news) > 0:

            #Tokenize and predict news sentiment using a trained model

            tokenizer = AutoTokenizer.from_pretrained('./final_model')
            model = AutoModelForSequenceClassification.from_pretrained('./final_model')
            inputs = tokenizer(new_news, return_tensors="pt", padding=True, truncation=True)
            outputs = model(**inputs)
            logits = outputs.logits
            predictions = logits.argmax(dim=-1)
            
            for pred in predictions:

                df.at[df.index[-1], 'pred'] =  pred.item()


                if pd.isna(df.iloc[-2]['status']):
                    if df.iloc[-2]['pred'] == 0 and df.iloc[-2]['open'] >= current_price:
                        df.at[df.index[-2], 'status'] = 1
                    elif df.iloc[-2]['pred'] == 0 and df.iloc[-2]['open'] < current_price:
                        df.at[df.index[-2], 'status'] = 0
                    elif df.iloc[-2]['pred'] == 1 and df.iloc[-2]['open'] <= current_price:
                        df.at[df.index[-2], 'status'] = 1
                    elif df.iloc[-2]['pred'] == 1 and df.iloc[-2]['open'] > current_price:
                        df.at[df.index[-2], 'status'] = 0

                #Plot OHLC data and send it via Telegram

                direction = "down 📉" if pred.item() == 0 else "up 📈"
                ohlc_df_m['Date'] = pd.to_datetime(ohlc_df_m['timestamp'])
                ohlc_df_m.set_index('Date', inplace=True)
                
                plot_filename = 'ohlc_plot.png'
                fig, ax = plt.subplots()

                mpf.plot(
                    ohlc_df_m,
                    type='candle',
                    style='charles',
                    ylabel='Price (USD)',
                    show_nontrading=True,
                    ax=ax,
                    datetime_format='%H:%M'
                )

                plt.text(0.5, 1.05, pd.Timestamp.now().strftime('%Y-%m-%d'), ha='center', va='center', transform=ax.transAxes, fontsize=12)

                up_icon = Image.open('upp.png')
                down_icon = Image.open('do.png')

                def add_icon(ax, image, position, zoom=1):
                    imagebox = OffsetImage(image, zoom=zoom)
                    ab = AnnotationBbox(imagebox, position, frameon=False, xycoords='axes fraction')
                    ax.add_artist(ab)

                if pred.item() == 0:
                    add_icon(ax, down_icon, (-0.08, 0.4), zoom=0.2)
                    plt.text(-0.08, 0.55, 'Prediction', ha='center', va="center", transform=ax.transAxes, fontsize=10)
                else:
                    add_icon(ax, up_icon, (-0.08, 0.6), zoom=0.2)
                    plt.text(-0.08, 0.45, 'Prediction', ha='center', va="center", transform=ax.transAxes, fontsize=10)

                plt.savefig(plot_filename)

                PHOTO_PATH = 'ohlc_plot.png'
                bot = Bot(token=BOT_TOKEN)
                dp = Dispatcher(bot)

                async def send_plot():
                    async with aiofiles.open(PHOTO_PATH, 'rb') as photo:
                        await bot.send_photo(CHAT_ID, photo)
                            
                asyncio.run(send_plot())
                    
                #Send market prediction message via Telegram

                message = f"📊The market is expected to go {direction}. 💰Current price: ${current_price:.2f}"
                print(message)
                send_message(message) 

        else:
            message = "No new news to process."
            print(message)
            send_message(message) 

        #Save updated data to the database

        df.to_sql(name = 'df_teleg'
                    , con = engine
                    , index = False
                    , if_exists ='replace')


        schedule.run_pending()
        time.sleep(10)
