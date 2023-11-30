from collections import defaultdict

class VersionVector:
    def __init__(self):
        self.vector = defaultdict(int)

    def increment(self, replica_id):
        self.vector[replica_id] += 1

    def merge(self, other_vector):
        for replica_id, version in other_vector.vector.items():
            self.vector[replica_id] = max(self.vector[replica_id], version)

    def dominates(self, other_vector):
        # Check if 'self' dominates 'other'
        for replica_id, version in other_vector.vector.items():
            if self.vector[replica_id] < version:
                return False
        return True

    def __repr__(self):
        return repr(dict(self.vector))