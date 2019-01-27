import threading


class SystemFlg:
    @classmethod
    def initialize(cls):
        cls.dbwriter_lock = threading.Lock()
        cls.dbwriter_flg = False
        cls.flyerws_flg = False
        cls.master_flg = False

    @classmethod
    def all_on(cls):
        cls.dbwriter_flg = True
        cls.flyerws_flg = True
        cls.master_flg = True


    @classmethod
    def getDBWriterFLG(cls):
        return cls.dbwriter_flg

    @classmethod
    def setDBWriterFLG(cls, flg):
        cls.dbwriter_flg = flg
