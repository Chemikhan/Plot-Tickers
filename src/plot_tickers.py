import pandas as pd
import queue
import threading
import os
pd.options.plotting.backend = "plotly"

class Plotter():
    def __init__(self, folder:str) -> None:
        '''
        Plotter of the ticker values from csv files

        :params folder:str, folder with csv files
        '''
        self.folder = folder
        self.files_list = os.listdir(folder)
        self.resulting_df = None
        self.dfs_queue = queue.Queue()
        self.files_queue = queue.Queue()
        self.barrier = threading.Barrier(len(self.files_list)+1) 

    @staticmethod
    def _normalize(series:pd.core.series.Series,standard_scale:float=100)->pd.core.series.Series:
        '''
        Normalize all values from series to standard scale(default:100)

        :param pd.core.series.Series series: data series to normalize
        :param int standad_scale: value to which series should be normalized
        '''
        return series/(series[0]/standard_scale)


    def _get_normalized_data_from_csv(self,ticker_file:str)->None:
        '''
        Read data from csv_file with ticker name from folder 'tickers'
        '''
        try:
            df = pd.read_csv(f'{self.folder}//{ticker_file}')
            ticker = df.columns[1]
            df[f'{ticker}'] = self._normalize(df[f'{ticker}'])
            self.dfs_queue.put(df)
        except FileNotFoundError as e:
            print(e)
        except Exception as e:
            print(e)
        self.barrier.wait()


    def _merge_dfs(self)->pd.DataFrame: 
        '''
        Merge every new dataframe in the dfs_queue with resulting_df.
        '''
        resulting_df = self.dfs_queue.get()
        while not self.dfs_queue.empty():
            new_df = self.dfs_queue.get()
            resulting_df = pd.merge(resulting_df, new_df, on="timestamp", how="inner")
        return resulting_df


    def _plot_df(self)->None:
        '''
        Plot data in 'line' style. 
        X-axis - timestamp converted to datetime
        Y-axis - values from all other columns.
        '''
        df = self.resulting_df
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        fig = df.plot(x='timestamp',
                      y=df.columns,
                      kind='line',
                      title=f"Normalized adjusted price for tickers from {df['timestamp'].dt.date[0]} to {df['timestamp'].dt.date.iloc[-1]}")
        fig.show()

    def _get_dfs_from_files(self):
        '''
        Getting normalized dataframes from csv files. 
        Seaprated thread is created for each file.
        Each dataframe is put to the dataframe queue dfs_queue
        '''
        for file_name in (self.files_list):
            threading.Thread(target=self._get_normalized_data_from_csv,args=(file_name,), daemon=True).start()

    def start(self):
        self._get_dfs_from_files()
        self.barrier.wait()
        self.resulting_df = self._merge_dfs()
        self._plot_df()

def main():
    plotter = Plotter('tickers')
    plotter.start()

if __name__ == '__main__':
    main()