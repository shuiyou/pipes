from abc import ABCMeta, abstractmethod


class Generate(object):
    __metaclass__ = ABCMeta

    def __init__(self)->None:
        super().__init__()
        self.request = {}
        self.reponse = {}

    def shake_hand(self,request=None):
        """
        第一次交互
        :param request:
        :return:
        """
        self.input(request)
        self.shake_hand_process()
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
    def shake_hand_process(self):
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



