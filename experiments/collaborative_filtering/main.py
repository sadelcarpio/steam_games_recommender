from collaborative_filtering.retrieval.mf import MatrixFactorization
import tensorflow as tf
from utils.data import DatasetBuilder
from config import NUM_GAMES, NUM_USERS

train_ds, test_ds = DatasetBuilder(
    {'user_index': tf.int64,
     'game_index': tf.int64,
     'game_name': tf.string}
).load_tfrecord_train_test_split(tfrecord_dir="../data/tfrecords/interactions",
                                 batch_size=1024,
                                 split_ratio=0.9)
# train_ds = train_ds.shuffle(10000).cache()
# test_ds = test_ds.cache()
# Necesitamos todo el dataset de items para los candidatos
games_ds, _ = DatasetBuilder(
    {'game_id': tf.int64,
     'game_index': tf.int64,
     'game_name': tf.string,
     'game_review_score': tf.int64,
     'game_short_description': tf.string,
     'game_required_age': tf.int64}
).load_tfrecord_train_test_split(tfrecord_dir="../data/tfrecords/items",
                                 split_ratio=1)
games_ds = games_ds.map(lambda x: {'game_index': x['game_index']})

retrieval_model = MatrixFactorization(num_users=NUM_USERS, num_items=NUM_GAMES, candidate_items=games_ds)
retrieval_model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.001))
retrieval_model.fit(train_ds, validation_data=test_ds, epochs=1, verbose=1)
retrieval_model.evaluate(test_ds, return_dict=True)
