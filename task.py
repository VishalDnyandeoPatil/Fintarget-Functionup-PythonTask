import websocket
import json
import csv
from datetime import datetime

# socket URL
socketURL = "wss://functionup.fintarget.in/ws?id=fintarget-functionup"

# OLHC data
olhcData = {"Nifty": {}, "Banknifty": {}, "Finnifty": {}}

# Moving average
maWindow = 3
maData = {"Nifty": [], "Banknifty": [], "Finnifty": []}

# CSV file
csvFile = "OLHC_Data.csv"

def on_message(ws, message):
    try:
        data = json.loads(message)
        for key, value in data.items():
            timestamp = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            currMinute = datetime.now().minute

            # Initialize OLHC data 
            if currMinute not in olhcData[key]:
                olhcData[key][currMinute] = [timestamp, value, value, value, value]

                # Calculating Simple Moving Average
                if len(olhcData[key]) >= maWindow:
                    close_prices = [float(item[4]) for item in olhcData[key].values()]
                    ma = sum(close_prices) / maWindow
                    maData[key].append([timestamp, ma])

                    # Save OLHC data to CSV
                    with open(csvFile, 'a', newline='') as csvfile:
                        csvWriter = csv.writer(csvfile)
                        csvWriter.writerow([key] + olhcData[key][currMinute])

                    # Save SMA data to CSV
                    if len(maData[key]) >= 3:
                        with open("SMA_Data.csv", 'a', newline='') as smafile:
                            smaWriter = csv.writer(smafile)
                            smaWriter.writerow([key] + maData[key][-1])

    except Exception as e:
        print(f"Error processing message: {e}")

def on_open(ws):
    print("Open")

def on_close(ws, close_status_code, close_msg):
    print("Close")

def on_error(ws, error):
    print(f"Error: {error}")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(socketURL,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open

    ws.run_forever()
