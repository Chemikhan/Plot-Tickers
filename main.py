'''
Project aimed to get and plot data for tickers.
'''
from src.get_tickers import TickerSaver
from src.plot_tickers import Plotter

def main():
    start_date = '01.01.20'
    end_date = '26.10.23'
    file_name = 'tickers.txt'
    ticker = TickerSaver(file_name,start_date, end_date)
    ticker.start()
    plotter = Plotter(file_name[:-4])
    plotter.start()

if __name__ == '__main__':
    main()