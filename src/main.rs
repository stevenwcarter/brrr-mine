use anyhow::{Result, bail};
use hashbrown::HashMap;

// Marseille;-20.4
// Yakutsk;-0.1
// Ouagadougou;38.3
// Palmerston North;23.2
// Copenhagen;-9.2
// Philadelphia;-0.2
// Nuuk;-10.5
// Da Lat;2.0
// Johannesburg;40.6
// Napoli;29.9

use std::{env, fs::File, os::fd::AsRawFd, ptr, slice};

const PROT_READ: i32 = 0x1;
const MAP_PRIVATE: i32 = 0x02;
const MAP_FAILED: *mut std::ffi::c_void = !0 as *mut std::ffi::c_void;

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

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let filename = if args.len() > 1 {
        args[1].as_str()
    } else {
        "measurements.txt"
    };

    let mmap = Mmap::new(filename)?;

    let slice: &[u8] = &mmap;

    aggregate(one(slice));

    Ok(())
}

fn aggregate(data: HashMap<&[u8], StationResult>) {
    let mut station_names: Vec<&str> = data
        .keys()
        .map(|b| std::str::from_utf8(b).unwrap())
        .collect();
    station_names.sort();
    let last = station_names.last().unwrap();

    // let mut stdout = std::io::stdout();

    print!("{{");
    for name in &station_names[..station_names.len() - 1] {
        let result = data.get(name.as_bytes()).unwrap();
        result.print(name);
        print!(", ")
    }
    let result = data.get(station_names.last().unwrap().as_bytes()).unwrap();
    result.print(last);

    println!("}}");
}

fn one(slice: &[u8]) -> HashMap<&[u8], StationResult> {
    let mut results = HashMap::new();

    let slice_length = slice.len();
    let mut position = 0;
    loop {
        let slice = &slice[position..];
        let next_line_pos = slice.iter().position(|c| *c == b'\n');
        if next_line_pos.is_none() {
            break;
        }
        let next_line_pos = next_line_pos.unwrap();
        if next_line_pos >= slice_length {
            break;
        }
        let line = &slice[..next_line_pos];
        position += next_line_pos + 1;
        let semi_pos = line.iter().position(|c| *c == b';').unwrap();
        let (name, temp) = line.split_at(semi_pos);
        let temp = std::str::from_utf8(&temp[1..]).unwrap();
        let temp = temp.parse::<f32>().unwrap();

        let result: &mut StationResult = results.entry(name).or_default();
        result.add_reading(temp);
    }

    results
}

unsafe extern "C" {
    fn mmap(
        addr: *mut std::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut std::ffi::c_void;
    fn munmap(addr: *mut std::ffi::c_void, len: usize) -> i32;
}

struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    pub fn new(filename: &str) -> Result<Self> {
        let file = File::open(filename)?;
        let len = file.metadata()?.len() as usize;
        let fd = file.as_raw_fd();

        if len == 0 {
            bail!("file is empty");
        }

        let ptr = unsafe {
            mmap(
                ptr::null_mut(), // Address (null = let OS choose)
                len,
                PROT_READ,
                MAP_PRIVATE,
                fd,
                0,
            )
        };

        if ptr == MAP_FAILED {
            bail!("Mapping failed");
        }

        Ok(Self {
            ptr: ptr as *mut u8,
            len,
        })
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            munmap(self.ptr as *mut std::ffi::c_void, self.len);
        }
    }
}

impl std::ops::Deref for Mmap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}
