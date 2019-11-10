import unittest

from consistent_hash import ConsistentHash


class ConsistentHashTest(unittest.TestCase):
    def test_something(self):
        shards = [u'redis1', u'redis2', u'redis3', u'redis4']
        users = [u'-84942321036308', u'-76029520310209', u'-68343931116147', u'-54921760962352']
        sharder = ConsistentHash(shards)
        expected = ['redis4', 'redis4', 'redis2','redis3']
        actual = []
        for user in users:
            shard = sharder.get_shard(user)
            actual.append(shard)
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
