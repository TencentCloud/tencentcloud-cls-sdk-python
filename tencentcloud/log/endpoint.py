from dataclasses import dataclass
from typing import ClassVar

class Region:
    BEIJING: ClassVar['Region'] = 'ap-beijing'
    GUANGZHOU: ClassVar['Region'] = 'ap-guangzhou'
    SHANGHAI: ClassVar['Region'] = 'ap-shanghai'
    HONGKONG: ClassVar['Region'] = 'ap-hongkong'
    SILICONVALLEY: ClassVar['Region'] = 'na-siliconvalley'
    ASHBURN: ClassVar['Region'] = 'na-ashburn'
    SINGAPORE: ClassVar['Region'] = 'ap-singapore'
    BANGKOK: ClassVar['Region'] = 'ap-bangkok'
    FRANKFURT: ClassVar['Region'] = 'eu-frankfurt'
    TOKYO: ClassVar['Region'] = 'ap-tokyo'
    SEOUL: ClassVar['Region'] = 'ap-seoul'
    JAKARTA: ClassVar['Region'] = 'ap-jakarta'
    SAOPAULO: ClassVar['Region'] = 'sa-saopaulo'
    SHENZHEN_FSI: ClassVar['Region'] = 'ap-shenzhen-fsi'
    SHANGHAI_FSI: ClassVar['Region'] = 'ap-shanghai-fsi'
    BEIJING_FSI: ClassVar['Region'] = 'ap-beijing-fsi'
    SHANGHAI_ADC: ClassVar['Region'] = 'ap-shanghai-adc'

    def __setattr__(self, *_):
        """模拟frozen=True的行为，防止修改属性"""
        raise AttributeError("Cannot modify attributes of Region class")

class NetworkType:
    INTRANET: ClassVar['NetworkType'] = 'cls.tencentyun.com'
    EXTRANET: ClassVar['NetworkType'] = 'cls.tencentcs.com'

    def __setattr__(self, *_):
        """模拟frozen=True的行为，防止修改属性"""
        raise AttributeError("Cannot modify attributes of NetworkType class")
@dataclass(frozen=True)
class EndpointBuilder:
    """通过前缀和后缀构建完整endpoint"""
    @staticmethod
    def createEndpoint(prefix, suffix):
        return f"{prefix}.{suffix}"