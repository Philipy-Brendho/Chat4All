from abc import ABC, abstractmethod

class BaseConnector(ABC):
    @abstractmethod
    def send(self, event: dict):
        ...
