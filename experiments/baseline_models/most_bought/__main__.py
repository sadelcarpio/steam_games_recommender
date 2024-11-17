import pandas as pd

from most_bought.model import MostBoughtGameModel

data = pd.read_csv('../../data/preprocessed/user_games_data.csv')
model = MostBoughtGameModel(k=5)
model.fit(data)
model.predict(data=data['user_id'].unique())
score = model.evaluate(data)
print(f'top-K categorical accuracy: {score}')
