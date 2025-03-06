# Stock Price Prediction using Snowflake & Airflow

## 📌 Overview  
This project automates **stock price prediction** using **Apache Airflow** for ETL workflows and **Snowflake ML** for forecasting. It fetches **Netflix (NFLX) & Pinterest (PINS)** stock data from **yfinance**, processes it in **Snowflake**, and predicts **7-day trends**.

## 🚀 Features  
- **Automated Data Pipeline** – Fetches stock data daily using **Airflow DAGs**.  
- **Machine Learning Forecasting** – Utilizes **Snowflake ML** for stock trend predictions.  
- **Cloud-Based Storage & Processing** – Efficiently handles large datasets in **Snowflake**.  
- **SQL & Python Integration** – Uses **SQL for storage & ML**, **Python for ETL workflows**.  

## 🛠️ Tech Stack  
- **Snowflake** – Data warehouse & ML forecasting  
- **Apache Airflow** – ETL orchestration  
- **yfinance API** – Stock data extraction  
- **SQL & Python** – Data processing & automation  

## 📂 Project Structure  
```
📁 stock-price-prediction  
 ├── 📂 dags/                     # Airflow DAG scripts  
 ├── 📂 sql/                      # SQL scripts for Snowflake  
 ├── 📂 notebooks/                # Jupyter Notebooks (optional)  
 ├── README.md                    # Documentation  
 ├── requirements.txt              # Dependencies  
```

## ⚡ Installation & Setup  

### 1️⃣ Clone the repository:  
```sh  
git clone https://github.com/yourusername/stock-price-prediction.git  
cd stock-price-prediction  
```

### 2️⃣ Install dependencies:  
```sh  
pip install -r requirements.txt  
```

### 3️⃣ Set up Airflow:  
```sh  
airflow db init  
airflow webserver & airflow scheduler  
```

### 4️⃣ Configure Snowflake credentials:  
- Set up **Snowflake connection** in Airflow.  
- Deploy **DAGs** to fetch data and train ML models.  

## 📊 How It Works  
1. **Airflow DAGs** fetch stock data daily from **yfinance**.  
2. Data is stored in **Snowflake** and preprocessed for analysis.  
3. **Snowflake ML** generates **7-day stock price forecasts**.  
4. Predictions are stored and updated weekly in **Snowflake tables**.  

## 🔄 Running the Pipeline  
- Start **Airflow Webserver & Scheduler** to trigger DAGs.  
- Monitor DAG execution via **Airflow UI**.  
- Query stock data and forecasts in **Snowflake**.  

## 🏆 Contributions  
Contributions are welcome! Feel free to **fork**, submit **pull requests**, or open **issues**.  


---  
🚀 **Let's predict stock prices using Snowflake & Airflow!**  

