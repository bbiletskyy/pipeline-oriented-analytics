from enum import Enum, auto


class Phase(Enum):
    """Represents different phases of Machine Learning: prediction and training.
    """
    train = auto()
    predict = auto()

    def is_train(self) -> bool:
        return Phase.train.value == self.value

    def is_predict(self) -> bool:
        return Phase.predict.value == self.value
