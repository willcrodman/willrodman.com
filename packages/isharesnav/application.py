import threading, time, pandas, os, logging, sys, datetime, ntplib, pytz, getpass


class Application:
    access_permission = 0o755

    def __init__(self):
        logging.getLogger()
        self.master_dir = f'/Users/{getpass.getuser()}/Desktop/isharesnavrequest/'
        os.system('clear')
        print('''
***********************************
***** IShares NAV Application *****
***********************************

Copyright (C) 2020 Will Rodman
        ''')

    def __call__(self, *args, **kwargs):
        time.sleep(3)
        os.system('clear')
        print(f'''
Application master directory: {self.master_dir}
_____________________________
[1] Use current master directory
[2] Use new master directory
        ''')
        statement = input()
        self.execute_statement(self.programs, self.write_master_dir, statement=statement)

    class NTP:
        @staticmethod
        def dt():
            return datetime.datetime.now(pytz.timezone('US/Eastern'))

        @staticmethod
        def timedelta():
            client = ntplib.NTPClient()
            response = client.request('pool.ntp.org')
            timedelta = abs(response.tx_time - response.orig_time)
            if timedelta > 1:
                logging.warning('Network datetime not in-sync')
            return round(timedelta, 3)

    class Request(NTP):
        def __init__(self, tickers, master_dir):
            self.master_dir = master_dir

        def __call__(self, *args, **kwargs):
            dataframe = pandas.read_csv('securities.csv')
            for index, row in dataframe.iterrows():
                if row['ticker'] in self.tickers:
                    timestamp = int(round(super().dt().timestamp(), 0))
                    filename = '{}.csv'.format(timestamp)
                    print(f"\nCompleted request for {row['ticker']}", end='\n'
                            f"Request {1 + self.tickers.index(row['ticker'])} of {len(self.tickers)}")
                    time.sleep(1)
            return True

    class Srape:
        pass

    class Clean:
        pass

    @classmethod
    def module_warning(cls):
        logging.getLogger()
        name = Application.__name__
        module = Application.__module__
        logging.warning(f'{name} is not running from {module}.py')
        return cls()

    def write_master_dir(self):
        os.system('clear')
        print(f'''
Enter new master directory:
___________________________
                ''')
        statement = input()
        if os.path.exists(statement):
            self.master_dir = statement
            self.programs()
        else:
            logging.error('path does not exist')
            self.__call__()

    def execute_statement(self, *funcs, statement):
        try:
            statement = abs(int(statement))
            funcs[statement - 1]()
        except (TypeError, SyntaxError, IndexError) as error:
            logging.error(error)
            self.__call__()
        except:
            logging.error('unknown error')
            sys.exit(2)

    def programs(self):
        os.system('clear')
        print(f'''
Application programs:
_____________________
[1] Concatenate NAVs
[2] Request NAVs
[3] Clean {self.master_dir}
        ''')
        statement = input()
        self.execute_statement(self.concat, self.request, self.clean, statement=statement)

    def write_tickers(self):
        os.system('clear')
        print(f'''
Enter array of ETF tickers to process:
(Enter empty array to process all ETFs)
_______________________________________
            ''')
        statement = input()
        try:
            statement = list(eval(statement))
            if statement == []:
                return pandas.read_csv('securities.csv')['ticker'].tolist()
            else:
                return statement
        except (SyntaxError, NameError) as error:
            logging.error(error)
            self.__call__()
        except:
            logging.error('cannot accept list of tickers')
            sys.exit(3)

    def request(self):
        tickers = self.write_tickers()
        dt = self.NTP.dt()
        requests = self.Request(tickers=list(tickers), master_dir=self.master_dir)
        close = dt.replace(hour=16, minute=0, second=0)
        os.system('clear')
        print(f'''
***************************
* Running Request Program *
***************************

Network Timedelta (seconds): {self.NTP.timedelta()}
            ''', end='\n')
        while True:
            if close < dt:
                print("\n       *** Fetching Requests ***\n")
                while requests():
                    print('\n       *** Requests Complete ***')
                    self.__call__()
            else:
                print("\n       Program will fetch requests after 16:00:00 EST"
                      f"\n      Market Clock: {dt.strftime('%X')} EST")
                time.sleep(60)

    def concat(self):
        def batch_list(l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]

        def header():
            os.system('clear')
            print(f'''
*********************************
* Running Concatenation Program *
*********************************
                    ''', end='\n')

        batchs = list(batch_list(self.write_tickers(), 45))
        header()
        threads = list()
        tasks_complete = [0] * len(batchs)
        for idx, batch in enumerate(batchs):
            def target(*args):
                global percent
                for ticker in batch:
                    time.sleep(1)
                    #if self.Srape(ticker=ticker, master_dir=self.master_dir).concatenate_dataframes():
                    tasks_complete[args[0]] = batch.index(ticker)
                del threads[args[0]]
            threads.append(threading.Thread(target=target, args=(idx,)))
            print(f"    THREAD {idx + 1} queued {len(batch)} tasks to memory")
        for t in threads: t.start()
        while threads:
            time.sleep(2)
            header()
            for idx, no in enumerate(tasks_complete):
                print(f"    THREAD {idx + 1}: {no} tasks completed")
        self.__call__()

    def clean(self):
        tickers = self.write_tickers()
        os.system('clear')
        print(f'''
**************************************
* Running Directory Cleaning Program *
**************************************
                            ''', end='\n')
        time.sleep(2)
        print('\n       *** Requests Complete ***')
        self.__call__()


if __name__ == '__main__':
    execute = Application()
    execute()
else:
    Application.module_warning()
