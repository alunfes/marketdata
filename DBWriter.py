import psycopg2
from psycopg2.extras import execute_values
import SystemFlg
import DBData
import threading
import time


class DBWriter:
    @classmethod
    def initialize(cls):
        cls.lock = threading.Lock()
        try:
            cls.conn = psycopg2.connect("dbname=MarketData user=postgres")
        except psycopg2.Error as e:
            print('Error when connecting to DB :'+e)

    @classmethod
    def testInsert(cls):
        with cls.lock:
            cur = cls.conn.cursor()
            cur.execute("INSERT INTO public.executions"
                        "(id, size, exec_date, side, buy_child_order_acceptance_id, sell_child_order_acceptance_id, price)"
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        (39360, 0.01, '2015-07-07T10:44:33.547', 'SELL', 'JRF20150707-014356-184990', 'JRF20150707-104433-186048', 700000))
            cls.conn.commit()
            cur.close()
            cls.conn.close()

    @classmethod
    def insert_execution_data(cls, data_list):
        with cls.lock:
            try:
                succes_flg = True
                cur = cls.conn.cursor()
                sql = "INSERT INTO public.executions (id, size, exec_date, side, buy_child_order_acceptance_id, sell_child_order_acceptance_id, price) VALUES %s"
                val_list = []
                for d in data_list:
                    t = (d[0], d[1], d[2], d[3], d[4], d[5], d[6])
                    val_list.append(t)
                execute_values(cur, sql, val_list)
                cls.conn.commit()
            except psycopg2.Error as e:
                print("Error on DBWriter-insertExecutionData! :"+e)
                succes_flg = False
            finally:
                return succes_flg

    @classmethod
    def write_execution_data_thread(cls):
        while SystemFlg.SystemFlg.dbwriter_flg:
                if DBData.BTCExecutionData.get_num_data() >= 100:
                    data = DBData.BTCExecutionData.get_all_executions()
                    if cls.insert_execution_data(data):
                        DBData.BTCExecutionData.initialize()
                    else:
                        print('error')
                time.sleep(0.1)

    @classmethod
    def start_write_execution_data(cls):
        print('started DBWriter')
        cls.initialize()
        th = threading.Thread(target=cls.write_execution_data_thread())
        th.start()




if __name__ == '__main__':
    DBWriter.initialize()
