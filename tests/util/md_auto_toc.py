# @Time : 3/15/21 5:47 PM 
# @Author : lixiaobo
# @File : md_auto_toc.py.py 
# @Software: PyCharm
import os


class MdAutoToc(object):
    IGNORE_FILES = ["README.md", "guide.md"]

    def __init__(self, p):
        self.md_path = p
        self.repo = {}
        self.results = ""

    def gen_toc(self, root_path):
        files = os.listdir(root_path)
        if not files or len(files) == 0:
            return

        files = list(filter(lambda x: os.path.isdir(root_path + "/" + x), files)) + list(
            filter(lambda x: os.path.isfile(root_path + "/" + x), files))
        for f in files:
            if self.is_empty(os.path.join(root_path, f)):
                continue
            if f.startswith(".") \
                    or f.startswith("_") \
                    or f.endswith("assets") \
                    or f in MdAutoToc.IGNORE_FILES:
                continue
            abs_path = root_path + "/" + f
            relative_path = abs_path.replace(self.md_path, "")

            varray = relative_path.split("/")
            prefix = self.gen_tab(len(varray))
            if os.path.isdir(abs_path):
                self.append_result(prefix, "* ", varray[-1].replace(' ', ''))
                self.gen_toc(abs_path)
            elif f.endswith(".md"):
                self.append_result(prefix, "* ", "[", varray[-1].replace(".md", "").replace(' ', ''), "](",
                                   relative_path.replace(' ', "%20"), ")")

    def write(self):
        print(self.results)
        with open(os.path.join(self.md_path, "README.md"), "w") as f:
            f.write(self.results)

    @staticmethod
    def gen_tab(sections):
        v = ""
        if sections > 1:
            for _ in range(sections - 1):
                v = v + "    "
        return v

    def append_result(self, *info):
        v = ""
        for i in info:
            v = v + i
        self.results = self.results + "\n" + v

    @staticmethod
    def is_empty(param):
        if os.path.isfile(param):
            return False
        f = os.listdir(param)
        if not f or len(f) == 0:
            return True
        if len(list(filter(lambda x: os.path.isdir(param + "/" + x), f))) > 0:
            return False

        return len(list(filter(lambda x: x.endswith(".md"), f))) == 0


if __name__ == "__main__":
    md_path = "/Users/xiaoboli/Sources/gdp-docset"
    if md_path.endswith("/"):
        md_path = md_path[:-1]
    mg = MdAutoToc(md_path + "/")
    mg.gen_toc(md_path)
    mg.write()
    print("finished.")
