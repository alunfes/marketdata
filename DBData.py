import threading


class BTCExecutionData:
    lock = threading.Lock()
    id = []
    side = []
    price = []
    size = []
    exec_date = []
    buy_child_order_acceptance_id = []
    sell_child_order_acceptance_id = []

    @classmethod
    def initialize(cls):
        cls.lock = threading.Lock()
        cls.id = []
        cls.side = []
        cls.price = []
        cls.size = []
        cls.exec_date = []
        cls.buy_child_order_acceptance_id = []
        cls.sell_child_order_acceptance_id = []

    @classmethod
    def add_execution(cls, data):
        with cls.lock:
            cls.id.append(data['id'])
            cls.side.append(data['side'])
            cls.price.append(data['price'])
            cls.size.append(data['size'])
            cls.exec_date.append(data['exec_date'])
            cls.buy_child_order_acceptance_id.append(data['buy_child_order_acceptance_id'])
            cls.sell_child_order_acceptance_id.append(data['sell_child_order_acceptance_id'])

    @classmethod
    def get_first_data(cls):
        with cls.lock:
            if len(cls.id) > 0:
                res = (cls.id.pop(0), cls.side.pop(0), cls.price.pop(0), cls.size.pop(0), cls.exec_date.pop(0),
                       cls.buy_child_order_acceptance_id.pop(0), cls.sell_child_order_acceptance_id.pop(0))
                return res
            else:
                return None

    @classmethod
    def get_num_data(cls):
        with cls.lock:
            num = len(cls.id)
            return num

    @classmethod
    def get_all_executions(cls):
        with cls.lock:
            try:
                res = [cls.id, cls.side, cls.price, cls.size, cls.exec_date, cls.buy_child_order_acceptance_id,
                       cls.sell_child_order_acceptance_id]
            finally:
                return res
