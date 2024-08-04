class Plane:
    def __init__(self, aircraft_id):
        self.aircraft_id = aircraft_id
        self.is_on_ground = True

    def update_state(self, gs, alt_baro, alt_geom):
        if gs is None or gs < 30 or alt_baro is None or alt_baro < 2000 or alt_geom is None or alt_geom < 2000:
            self.is_on_ground = True
        else:
            self.is_on_ground = False