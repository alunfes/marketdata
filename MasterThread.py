import asyncio
import time
import FlyerWebSocket
import DBWriter
import DBData
import SystemFlg

class MasterThread:

    @classmethod
    def start(cls):
        SystemFlg.SystemFlg.all_on()
        DBData.BTCExecutionData.initialize()
        fws = FlyerWebSocket.FlyerWebSocket('lightning_executions_', 'FX_BTC_JPY')
        while fws.is_connected() != True:
            time.sleep(0.1)
        DBWriter.DBWriter.start_write_execution_data()

        while SystemFlg.SystemFlg.master_flg:
            print('num data='+DBData.BTCExecutionData.get_num_data())
            time.sleep(0.5)



if __name__ == '__main__':
    MasterThread.start()






