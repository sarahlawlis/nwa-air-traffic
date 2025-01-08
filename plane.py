from collections import deque

class Plane:
    def __init__(self, aircraft_id):
        self.aircraft_id = aircraft_id
        # possible states: on_ground, in_air, transitioning, start with on_ground
        self.state = 'on_ground'
        self.gs = None
        self.alt_baro = None
        self.alt_geom = None
        self.last_snapshotid = None
        # history of states for better analysis
        self.state_history = deque(maxlen=5)

    def update_state(self, gs, alt_baro, alt_geom, snapshotid):
        self.gs = gs
        self.alt_baro = alt_baro
        self.alt_geom = alt_geom
        self.last_snapshotid = snapshotid

        # Determine state
        if gs is not None:
            if gs >= 30:
                self.state = 'in_air' if self.state == 'transitioning' else 'transitioning'
            elif gs < 30:
                self.state = 'on_ground' if self.state == 'transitioning' else 'transitioning'
        else:
            self.state = 'on_ground'

        # Track state history
        self.state_history.append(self.state)

    def is_landing(self):
        # check if transitioning to on_ground
        return list(self.state_history)[-2:] == ['transitioning', 'on_ground']

    def is_takeoff(self):
        # check if transitioning to in_air
        return list(self.state_history)[-2:] == ['transitioning', 'in_air']

    def has_snapshot_gap(self, new_snapshotid):
        if self.last_snapshotid is None:
            return False
        try:
            current_snapshotid = int(self.last_snapshotid)
            new_snapshotid_int = int(new_snapshotid)
            return new_snapshotid_int > current_snapshotid + 1
        except ValueError:
            print(f"Invalid snapshotid(s): last_snapshotid={self.last_snapshotid}, new_snapshotid={new_snapshotid}")
            return False