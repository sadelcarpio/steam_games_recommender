from abc import ABC, abstractmethod


class RecommenderModel(ABC):
    @abstractmethod
    def fit(self, data):
        pass

    @abstractmethod
    def predict(self, data):
        pass

    @abstractmethod
    def evaluate(self, data):
        pass

    @abstractmethod
    def save(self, path: str):
        pass

    @classmethod
    def load(cls, path: str):
        pass
