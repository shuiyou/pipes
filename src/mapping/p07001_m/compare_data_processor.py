from mapping.module_processor import ModuleProcessor


# 和CCS数据比较相关的变量清洗


class CompareDataProcessor(ModuleProcessor):
    def process(self):
        print("CompareDataProcessor process")
        '''
            if_name	与ccs姓名比对
            phone_alt	与ccs手机号比对
            if_cert_no	与ccs身份证号比对
            if_marriage	与ccs婚姻状况比对
            if_postal_addr	与ccs通讯地址比对
            if_residence_addr	与ccs户籍地址比对
            if_live_addr	与ccs居住地址比对
            if_employee	是否是员工
            if_official	是否是公检法人员
            if_spouse_name	与ccs配偶姓名匹配
            if_spouse_cert_no	与ccs配偶身份证匹配
        '''

