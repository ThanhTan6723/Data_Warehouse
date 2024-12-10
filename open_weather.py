import requests
import mysql.connector
from mysql.connector import Error

# Hàm kết nối đến cơ sở dữ liệu MySQL
def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host="localhost",            # Địa chỉ server MySQL (thay đổi nếu khác)
            database="weather_staging",  # Tên cơ sở dữ liệu
            user="root",                 # Tên người dùng MySQL
            password="6723"              # Mật khẩu MySQL
        )
        if conn.is_connected():
            print("Connected to MySQL database")
        return conn
    except Error as e:
        print(f"Error: {e}")
        return None

# Hàm lưu dữ liệu vào bảng weather_data trong cơ sở dữ liệu weather_staging
def save_weather_data_to_staging(city_name, temperature, humidity, pressure, description, wind_speed):
    conn = get_db_connection()
    if conn is not None:
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO weather_data (city_name, temperature, humidity, pressure, weather_description, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (city_name, temperature, humidity, pressure, description, wind_speed))

        conn.commit()
        print(f"Data for {city_name} has been saved successfully to the database.")
        cursor.close()
        conn.close()
    else:
        print("Unable to connect to the database. Data not saved.")

# Hàm lấy dữ liệu thời tiết từ OpenWeatherMap API
def fetch_weather_data(city_name, api_key):
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric'
    response = requests.get(url)
    data = response.json()

    # Kiểm tra mã phản hồi từ API
    if data.get("cod") != "404":
        main_data = data["main"]
        weather_description = data["weather"][0]["description"]
        wind_speed = data["wind"]["speed"]

        temperature = main_data["temp"]
        humidity = main_data["humidity"]
        pressure = main_data["pressure"]

        # Lưu dữ liệu vào cơ sở dữ liệu
        save_weather_data_to_staging(city_name, temperature, humidity, pressure, weather_description, wind_speed)
    else:
        print(f"City {city_name} not found or error fetching data.")

# API key của bạn từ OpenWeatherMap (hãy đảm bảo key hợp lệ)
api_key = "46a0980fcdfa3610bbd80e0eb1b6fd8f"
city_name = "Hanoi"

# Lấy và lưu dữ liệu vào cơ sở dữ liệu
fetch_weather_data(city_name, api_key)
