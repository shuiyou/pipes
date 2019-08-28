from abc import ABCMeta, abstractmethod


class Generate(object):
    __metaclass__ = ABCMeta

    def __init__(self)->None:
        super().__init__()
        self.request = {}
        self.reponse = {}


    def shack_hander(self,request=None):
        """
        第一次交互
        :param request:
        :return:
        """
        self.input(request)
        self.shack_hander_process()
        return self.reponse

    def call_strategy(self,request=None):
        """
        第二次交互
        :param request:
        :return:
        """
        self.input(request)
        self.strategy_process()
        return self.reponse

    def input(self, request):
        self.request = request

    @abstractmethod
    def shack_hander_process(self):
        """
        defensor第一次调用处理逻辑
        :return:
        """
        pass

    @abstractmethod
    def strategy_process(self):
        """
        defensor第二次调用处理逻辑
        :return:
        """
        pass



