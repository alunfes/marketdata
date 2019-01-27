import websocket
import threading
import time
import json
import DBData


class FlyerWebSocket:
    def __init__(self, channel, symbol):
        self.symbol = symbol
        self.ticker = None
        self.channel = channel
        self.connect()

    def connect(self):
        self.ws = websocket.WebSocketApp('wss://ws.lightstream.bitflyer.com/json-rpc',
            on_message = self.on_message,
            on_error = self.on_error, on_close = self.on_close)
        #self.ws = websocket.WebSocketApp(
        #    'wss://ws.lightstream.bitflyer.com/json-rpc', header=None,
        #    on_open = self.on_open, on_message = self.on_message,
        #    on_error = self.on_error, on_close = self.on_close)
        self.ws.keep_running = True 
        websocket.enableTrace(True)
        self.ws.on_open = self.on_open
        self.thread = threading.Thread(target=lambda: self.ws.run_forever())
        self.thread.daemon = True
        self.thread.start()

    def is_connected(self):
        return self.ws.sock and self.ws.sock.connected

    def disconnect(self):
        print('disconnected')
        self.ws.keep_running = False
        self.ws.close()


    def get(self):
        return self.ticker

    def on_message(self, ws, message):
        message = json.loads(message)['params']
        self.ticker = message['message']
        if self.ticker is not None:
            for t in self.ticker:
                DBData.BTCExecutionData.add_execution(dict(t))

    def on_error(self, ws, error):
        print('error')
        self.disconnect()
        time.sleep(3)
        self.connect()

    def on_close(self, ws):
        print('Websocket disconnected')

    def on_open(self, ws):
        ws.send(json.dumps( {'method':'subscribe',
            'params':{'channel':self.channel + self.symbol}} ))
        time.sleep(1)
        print('Websocket connected')


if __name__ == '__main__':
    getter = FlyerWebSocket('lightning_executions_','FX_BTC_JPY')
    time.sleep(3)
    while True:
        time.sleep(0.1)
        print(DBData.BTCExecutionData.get_num_data())