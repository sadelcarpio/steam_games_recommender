import glob
import os
import random

import tensorflow as tf
import tensorflow_io as tfio


class DatasetBuilder:
    def __init__(self,
                 fields_mapping: dict = None):
        self.fields_mapping = fields_mapping

    def load_tfrecord_dataset(self, files, feature_spec, batch_size=64):
        dataset = tf.data.TFRecordDataset(files)
        dataset = dataset.map(self.make_parser(feature_spec), num_parallel_calls=tf.data.AUTOTUNE)
        dataset = dataset.batch(batch_size).prefetch(tf.data.AUTOTUNE)
        return dataset

    @staticmethod
    def make_parser(feature_spec):
        def _parse(example_proto):
            return tf.io.parse_single_example(example_proto, feature_spec)

        return _parse

    @staticmethod
    def write_sharded_tfrecord_from_dataset(ds: tf.data.Dataset,
                                            output_dir: str,
                                            base_name: str = "data",
                                            num_shards: int = 5):
        os.makedirs(output_dir, exist_ok=True)

        writers = [
            tf.io.TFRecordWriter(os.path.join(output_dir, f"{base_name}_{i:05d}-of-{num_shards:05d}.tfrecord"))
            for i in range(num_shards)
        ]

        def serialize_example(record):
            features = {}
            for key, val in record.items():
                val = tf.convert_to_tensor(val)
                if val.dtype == tf.string:
                    features[key] = tf.train.Feature(bytes_list=tf.train.BytesList(value=[val.numpy()]))
                elif val.dtype.is_integer:
                    features[key] = tf.train.Feature(int64_list=tf.train.Int64List(value=[val.numpy()]))
                elif val.dtype.is_floating:
                    features[key] = tf.train.Feature(float_list=tf.train.FloatList(value=[val.numpy()]))
            return tf.train.Example(features=tf.train.Features(feature=features)).SerializeToString()

        for i, record in enumerate(ds):
            print(f"Writing record {i} to shard")
            shard = i % num_shards
            serialized = serialize_example(record)
            writers[shard].write(serialized)

        for w in writers:
            w.close()

        print(f"✅ Wrote {num_shards} TFRecord shard files to: {output_dir}")

    @staticmethod
    def get_sharded_tfrecord_splits(output_dir, split_ratio=0.2, shuffle=True):
        tfrecord_files = sorted(glob.glob(os.path.join(output_dir, "*.tfrecord")))
        if shuffle:
            random.shuffle(tfrecord_files)

        train_size = int(len(tfrecord_files) * split_ratio)
        train_files = tfrecord_files[:train_size]
        test_files = tfrecord_files[train_size:]

        return train_files, test_files

    def write_tfrecord_from_parquet(self,
                                    filepath: str,
                                    tfrecord_dir: str,
                                    num_shards: int,
                                    ):
        dataset = tfio.IODataset.from_parquet(filepath)
        dataset = dataset.map(lambda x: {k: x[k.encode()] for k in self.fields_mapping.keys()})
        self.write_sharded_tfrecord_from_dataset(dataset, output_dir=tfrecord_dir, num_shards=num_shards)


    def load_tfrecord_train_test_split(self, tfrecord_dir, batch_size: int = 64, split_ratio=0.2):
        train_files, test_files = self.get_sharded_tfrecord_splits(tfrecord_dir, split_ratio=split_ratio)
        feature_spec = {
            k: tf.io.FixedLenFeature([], v) for k, v in self.fields_mapping.items()
        }
        train_ds = self.load_tfrecord_dataset(train_files, feature_spec, batch_size)
        test_ds = self.load_tfrecord_dataset(test_files, feature_spec, batch_size)
        return train_ds, test_ds


if __name__ == '__main__':
    # builder = DatasetBuilder(fields_mapping={
    #     'user_index': tf.int64,
    #     'game_index': tf.int64,
    #     'game_name': tf.string
    # })
    # builder.write_tfrecord_from_parquet(filepath='../../data/marts/game_features_with_index.parquet',
    #                                     tfrecord_dir='../../data/tfrecords/items',
    #                                     num_shards=10)
    builder = DatasetBuilder(fields_mapping={
        'game_id': tf.int64,
        'game_index': tf.int64,
        'game_name': tf.string,
        'game_review_score': tf.int64,
        'game_short_description': tf.string,
        'game_required_age': tf.int64
    })
    builder.write_tfrecord_from_parquet(filepath='../../data/marts/game_features_with_index.parquet',
                                        tfrecord_dir='../../data/tfrecords/items',
                                        num_shards=10)
