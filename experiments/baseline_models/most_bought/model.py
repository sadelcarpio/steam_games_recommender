import pickle

import numpy as np

from libs.model import RecommenderModel


class MostBoughtGameModel(RecommenderModel):
    """
    Simplest model to use as a baseline. Predicts the most bought game and recommends it
    """
    def __init__(self, k: int):
        self.k = k
        self.most_bought_games = None

    def fit(self, data):
        most_bought_games = data[data['activity'] == 'purchase'][['appid', 'name', 'activity']].\
            groupby(['appid', 'name']).\
            count().reset_index()
        self.most_bought_games = most_bought_games.sort_values(by='activity', ascending=False).head(self.k)

    def predict(self, data):
        return np.tile(self.most_bought_games['appid'].to_numpy(), (len(data), 1))

    def evaluate(self, data):
        """
        Calculate top-k categorical accuracy, meaning, it scores the percentage of k predictions that are inside the
        actual user - item interactions
        :param data:
        :return:
        """
        predictions = self.predict(data['user_id'].unique())
        true_labels = data[['appid', 'user_id']].groupby('user_id')['appid'].apply(np.array).reset_index(name='appids')
        true_labels = true_labels['appids'].to_numpy()
        scores = np.zeros(len(predictions))
        for i, (label, pred) in enumerate(zip(true_labels, predictions)):
            correct_labels = np.intersect1d(label, pred)
            score = len(correct_labels) / self.k
            scores[i] = score
        return scores.mean()

    def save(self, path: str):
        with open(path, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, path: str):
        with open(path, 'rb') as f:
            obj = pickle.load(f)
        return obj
