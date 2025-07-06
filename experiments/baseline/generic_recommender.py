from utils.data import DatasetBuilder

train_ds, test_ds = DatasetBuilder().load_tfrecord_train_test_split(tfrecord_dir="../data/tfrecords",
                                                                    split_ratio=0.2)

