import tensorflow_recommenders as tfrs
import tensorflow as tf


class MatrixFactorization(tfrs.Model):
    def __init__(self,
                 num_users: int,
                 num_items: int,
                 candidate_items: tf.data.Dataset,
                 user_feat: str = 'user_index',
                 item_feat: str = 'game_index',
                 embedding_dimension: int = 16):
        super().__init__()
        self.user_feat = user_feat
        self.item_feat = item_feat
        self.embedding_dimension = embedding_dimension
        self.num_users = num_users
        self.num_items = num_items
        self.user_model: tf.keras.Model = tf.keras.Sequential([
            tf.keras.layers.Embedding(self.num_users + 1, self.embedding_dimension)
        ])
        self.item_model: tf.keras.Model = tf.keras.Sequential([
            tf.keras.layers.Embedding(self.num_items + 1, self.embedding_dimension)
        ])
        self.task: tf.keras.layers.Layer = tfrs.tasks.Retrieval(
            metrics=tfrs.metrics.FactorizedTopK(
                candidate_items.map(lambda x: self.item_model(x[self.item_feat]))
            )
        )

    def compute_loss(self, features, training=False):
        user_embeddings = self.user_model(features[self.user_feat])
        item_embeddings = self.item_model(features[self.item_feat])
        return self.task(user_embeddings, item_embeddings)  # aquí ocurre MF
