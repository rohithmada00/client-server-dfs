from enum import Enum

class LockingStates(Enum):
    LOCKED : 'LOCKED'
    UNLOCKED : 'UNLOCKED'
    LOCK_GRANTED : 'LOCK_GRANTED'
    LOCK_NOT_GRANTED : 'LOCK_NOT_GRANTED'