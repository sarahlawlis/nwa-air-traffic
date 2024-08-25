class Plane:
    def __init__(self, aircraft_id):
        self.aircraft_id = aircraft_id
        self.state = 'on_ground'  # Possible states: 'on_ground', 'in_air', 'transitioning'
        self.gs = None
        self.alt_baro = None
        self.alt_geom = None
        self.last_snapshotid = None  # Assuming this is a string

    def update_state(self, gs, alt_baro, alt_geom, snapshotid):
        self.gs = gs
        self.alt_baro = alt_baro
        self.alt_geom = alt_geom
        self.last_snapshotid = snapshotid

        # Transition logic
        if gs is not None:
            if gs >= 30:
                if self.state == 'on_ground':
                    self.state = 'transitioning'
                elif self.state == 'transitioning':
                    self.state = 'in_air'
            elif gs < 30:
                if self.state == 'in_air':
                    self.state = 'transitioning'
                elif self.state == 'transitioning':
                    self.state = 'on_ground'

    def is_landing(self):
        return self.state == 'transitioning' and self.gs is not None and self.gs < 30

    def is_takeoff(self):
        return self.state == 'transitioning' and self.gs is not None and self.gs >= 30

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