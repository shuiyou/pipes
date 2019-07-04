from abc import ABCMeta, abstractmethod
import os

class Process(object):
    __metaclass__ = ABCMeta

    def __init__(self) -> None:
        super().__init__()
        self.read_path = None
        self.write_path = None

    def run(self, read_file_name = None):
        write_file_name = read_file_name.split('.')[0]+'_result'+'.'+read_file_name.split('.')[1]
        read_path = os.path.join(os.path.abspath('.'), 'input', read_file_name)
        write_path = os.path.join(os.path.abspath('.'), 'output', write_file_name)
        self.input(read_path, write_path)
        self.do_process_case()

    def input(self, read_path, write_path):
        self.read_path = read_path
        self.write_path = write_path


    @abstractmethod
    def do_process_case(self):
        """
        处理测试用例方法

        """
        pass
