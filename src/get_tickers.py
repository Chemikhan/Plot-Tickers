import requests
from datetime import datetime, timezone
import typing 
import queue
import threading
import pandas as pd
import concurrent.futures
import os

user_agent_key = "User-Agent"
user_agent_value = "Mozilla/5.0"
headers = {user_agent_key: user_agent_value}

class TickerSaver():
    def __init__(self, tickers_list_file:str,start_date:str,end_date: str,interval: str = "1wk") -> None:
        '''
        Object to process tickers list and get data for each ticker from start_date 
        till end_date and save this data in csv files.

        :param tickers_list_file:str, file name, should be located in the same directory and have extention *.txt
        :param start_date: str, period start date in the format 'dd.mm.yy'.
        :param end_date: str, period end date in the format 'dd.mm.yy'.
        :param interval: str, time interval (week, day, etc.) (optional, default '1wk' is one week).
        '''
        self.source_file = f'{tickers_list_file}'
        self.folder_to_save = tickers_list_file[:-4]
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.file_queue = queue.Queue()

    def _get_ticker(self) -> typing.Generator[str, None, None]:
        '''
        Generate sequence of tickers from source_file

        :yield: str, ticker name
        '''
        with open(self.source_file) as file:
            for line in file:
                ticker = line.strip()
                yield ticker

    def _get_history_data(self,ticker: str):
        """
        Retrieves historical data for the specified asset ticker.

        :param ticker: str, asset ticker.
        :return: (str,str), tuple of ticker name and JSON string with historical data.
        """
        per2 = int(datetime.strptime(self.end_date, '%d.%m.%y').replace(tzinfo=timezone.utc).timestamp())
        per1 = int(datetime.strptime(self.start_date, '%d.%m.%y').replace(tzinfo=timezone.utc).timestamp())
        params = {"period1": str(per1), "period2": str(per2),
                "interval": self.interval, "includeAdjustedClose": "true"}
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        response = requests.get(url, headers=headers, params=params)
        return (ticker,response.json())


    def _save_to_file(self)->None:
        """
        Save results from file queue to csv files

        """
        if not os.path.exists(f'{self.folder_to_save}'): 
            # if the folder_to_save directory is not present then create it. 
            os.makedirs(f'{self.folder_to_save}') 
        while True:
            ticker,result = self.file_queue.get()
            adjclose = result['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
            timestamp = result['chart']['result'][0]['timestamp']
            df = pd.DataFrame({"timestamp": timestamp, f"{ticker}": adjclose})
            df.to_csv(f'{self.folder_to_save}//{ticker}.csv',index=False)
            self.file_queue.task_done()

    def _handle_task_done(self,future:concurrent.futures.Future):
        """
        Handle done task 

        :param future: concurrent.futures.Future, Future object.
        :param ticker: str, ticker name.
        """
        exception = future.exception()
        if exception is not None:
            print(f'Ошибка запроса получения исторических данных: {exception}')
            return
        try:
            self.file_queue.put(future.result())
        except Exception as err:
            print(f"Unnown error while handling data, {err}")


    def start(self):
        save_to_file_thr = threading.Thread(target=self._save_to_file, daemon=True)
        save_to_file_thr.start()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for ticker in self._get_ticker():
                future = executor.submit(self._get_history_data, ticker)
                future.add_done_callback(self._handle_task_done)
        self.file_queue.join()

def main():
    start_date = '01.01.20'
    end_date = '26.10.23'
    ticker = TickerSaver('tickers.txt',start_date, end_date)
    ticker.start()

if __name__ == '__main__':
    main()