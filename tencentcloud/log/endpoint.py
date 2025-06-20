# encoding: utf-8

class Region(object):
    BEIJING = 'ap-beijing'
    GUANGZHOU = 'ap-guangzhou'
    SHANGHAI = 'ap-shanghai'
    HONGKONG = 'ap-hongkong'
    SILICONVALLEY = 'na-siliconvalley'
    ASHBURN = 'na-ashburn'
    SINGAPORE = 'ap-singapore'
    BANGKOK = 'ap-bangkok'
    FRANKFURT = 'eu-frankfurt'
    TOKYO = 'ap-tokyo'
    SEOUL = 'ap-seoul'
    JAKARTA = 'ap-jakarta'
    SAOPAULO = 'sa-saopaulo'
    SHENZHEN_FSI = 'ap-shenzhen-fsi'
    SHANGHAI_FSI = 'ap-shanghai-fsi'
    BEIJING_FSI = 'ap-beijing-fsi'
    SHANGHAI_ADC = 'ap-shanghai-adc'

    def __setattr__(self, *_):
        raise AttributeError("Cannot modify attributes of Region class")


class NetworkType:
    INTRANET = 'cls.tencentyun.com'
    EXTRANET = 'cls.tencentcs.com'

    def __setattr__(self, *_):
        raise AttributeError("Cannot modify attributes of NetworkType class")


class EndpointBuilder:
    @staticmethod
    def createEndpoint(prefix, suffix):
        return "{0}.{1}".format(prefix, suffix)