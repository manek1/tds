from abc import ABC, abstractmethod


class GlueRunner(ABC):
    @property
    @abstractmethod
    def __module_name__(self):
        pass

    @abstractmethod
    def __run__(self, *args, **kwargs):
        pass
