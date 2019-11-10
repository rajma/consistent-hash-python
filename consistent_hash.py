import hashlib
import bisect


def get_md5_hash(value):
    """Return an integer hash"""
    if value:
        hasher = hashlib.md5(value.encode('utf-8'))
        return int(hasher.hexdigest(), 16)
    else:
        return None


class ConsistentHash (object):
    """Implements a consistent hashing sharding Algorithm"""

    def __init__(self, nodes=None, replicas = 100):
        self.replicas = replicas
        self.ring = dict()
        self._sorted_keys = []
        self._not_sorted = True
        if nodes:
            self.nodes = nodes
        else:
            self.nodes = {}
        self._generate_circle()

    def _generate_circle(self):
        """Generates the circle with shards"""
        for node in self.nodes:
            self.add_node(node)

    def add_node(self, node):
        """Add a node to the sharding ring """
        if node:
            # Any time we are inserting a node, we are disturbing the key sort order
            self._not_sorted = True
            for i in range(self.replicas):
                node_replica = "{}{}".format(node,i)
                node_hash = get_md5_hash(node_replica)
                if node_hash in self.ring:
                    raise ValueError('Value {} already exists'.format(node_hash))
                self.ring[node_hash] = node
                self._sorted_keys.append(node_hash)

    def remove_node(self, node):
        """Remove a node from the sharding ring """
        if node:
            # Any time we are inserting a node, we are disturbing the key sort order
            self._not_sorted = True
            for i in range(self.replicas):
                node_replica = "{}{}".format(node,i)
                node_hash = get_md5_hash(node_replica)
                del self.ring[node_hash]
                self._sorted_keys.remove(node_hash)

    def get_shard(self, key):
        """Given a key, allocate the shard for it"""
        if key and len(self.ring):
            # Sort if not sorted. Don't have to do it on add
            if self._not_sorted:
                self._sorted_keys.sort()
                self._not_sorted = False
            key_hash = get_md5_hash(key)
            insert_index = bisect.bisect(self._sorted_keys, key_hash)
            if insert_index == len(self._sorted_keys):
                insert_index = 0
            return self.ring[self._sorted_keys[insert_index]]
        else:
            return None
