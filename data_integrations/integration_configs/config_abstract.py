# This files contains the integration specific configurations.
# Each configuration is a class that contains urls, headers, 
# endpoint payloads, and other information.
# The configuration class is then imported into the integration
# file and used to create the integration.

from abc import ABCMeta, abstractmethod


class ConfigAbstract(metaclass=ABCMeta):
    """
    Abstract class for configs.
    """
    
    
    @property
    @abstractmethod
    def header(self):
        pass

    @header.setter
    @abstractmethod
    def header(self, header):
        pass


    @property
    @abstractmethod
    def endpoint(self):
        pass

    @endpoint.setter
    @abstractmethod
    def endpoint(self, endpoint_name):
        pass