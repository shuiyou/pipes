from mapping.module_processor import ModuleProcessor

class TransFlow(ModuleProcessor):

    def __init__(self):
        super().__init__()
        # self.db = self._db()
        self.variables = {}