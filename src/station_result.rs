#[derive(Debug, Clone, Copy, Default)]
pub struct StationResult {
    pub count: u32,
    pub temps: f32,
    pub min: f32,
    pub max: f32,
}

impl StationResult {
    pub fn print(&self, name: &str) {
        print!("{}={:.1}/{:.1}/{:.1}", name, self.min, self.avg(), self.max);
    }
    pub fn avg(&self) -> f32 {
        self.temps / self.count as f32
    }
    pub fn add_reading(&mut self, reading: f32) {
        if reading < self.min {
            self.min = reading;
        }
        if reading > self.max {
            self.max = reading;
        }
        self.count += 1;
        self.temps += reading;
    }
}
