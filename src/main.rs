use anyhow::Result;
use hashbrown::HashMap;

use std::{
    env,
    sync::mpsc,
    thread::available_parallelism,
    time::{Duration, Instant},
};

mod mmap;
mod station_result;
use mmap::*;
use station_result::*;

fn main() -> Result<()> {
    let start = Instant::now();
    let args: Vec<String> = env::args().collect();
    let filename = if args.len() > 1 {
        args[1].as_str()
    } else {
        "measurements.txt"
    };

    let mmap = Mmap::new(filename)?;

    let slice: &[u8] = mmap.leak();

    let nthreads = available_parallelism()?.get();
    let (tx, rx) = mpsc::channel();
    let (time_tx, time_rx) = mpsc::channel();

    let mut position = 0;
    let total_len = slice.len();
    let chunk_size = total_len / nthreads;
    (0..nthreads).for_each(|_| {
        let tx = tx.clone();
        let time_tx = time_tx.clone();
        let start = Instant::now();
        let len = slice.len();

        let end = position + chunk_size;
        dbg!(end);
        let end = if end < len {
            let end = find_line_pos(&slice[end..]);
            dbg!(end);
            if let Some(pos) = end {
                position + chunk_size + pos
            } else {
                len
            }
        } else {
            len
        };
        dbg!(end);
        println!();

        let slice = &slice[position..end];
        position = end + 1;

        std::thread::spawn(move || {
            let result = one(slice);
            tx.send(result).unwrap();
            time_tx.send(start.elapsed()).unwrap();
            drop(tx);
        });
    });
    eprintln!("Spawn time: {:?}", start.elapsed());

    drop(tx);
    drop(time_tx);

    let mut aggregated: HashMap<&[u8], StationResult> = HashMap::new();
    let mut duration: Duration = Duration::new(0, 0);
    while let Ok(data) = rx.recv() {
        let start = Instant::now();
        aggregate_from_parts(&mut aggregated, data);
        duration += start.elapsed();
    }
    eprintln!("Aggregation time: {:?}", duration);
    let mut duration: Duration = duration;
    while let Ok(dur) = time_rx.recv() {
        duration += dur;
        eprintln!("Thread time: {:?}", dur);
    }
    eprintln!("Total thread time: {:?}", duration);

    aggregate(aggregated);

    Ok(())
}

fn aggregate_from_parts(
    dst: &mut HashMap<&'static [u8], StationResult>,
    data: HashMap<&'static [u8], StationResult>,
) {
    for (key, value) in data {
        let entry = dst.entry(key).or_default();
        if value.min < entry.min {
            entry.min = value.min;
        }
        if value.max > entry.max {
            entry.max = value.max;
        }
        entry.count += value.count;
        entry.temps += value.temps;
    }
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
        let next_line_pos = find_line_pos(slice);
        if next_line_pos.is_none() {
            break;
        }
        let next_line_pos = next_line_pos.unwrap();
        if next_line_pos >= slice_length {
            break;
        }

        let line = &slice[..next_line_pos];
        position += next_line_pos + 1;

        let semi_pos = find_semi_pos(line);

        let (name, temp) = line.split_at(semi_pos);
        let temp = std::str::from_utf8(&temp[1..]).unwrap();
        let temp = temp.parse::<f32>().unwrap();

        let result: &mut StationResult = results.entry(name).or_default();
        result.add_reading(temp);
    }

    results
}

fn find_line_pos(slice: &[u8]) -> Option<usize> {
    slice.iter().position(|c| *c == b'\n')
}
fn find_semi_pos(line: &[u8]) -> usize {
    line.iter().position(|c| *c == b';').unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_finds_semicolons() {
        assert_eq!(find_semi_pos(b"Triangulation;-123.3\n"), 13);
    }
    #[test]
    fn it_finds_newlines() {
        assert_eq!(find_line_pos(b"Triangulation;-123.3\n"), Some(20));
    }
    #[test]
    fn it_aggregates_properly() {
        let mut station: StationResult = StationResult::default();
        station.add_reading(-100.1);
        station.add_reading(100.1);

        assert_eq!(station.min, -100.1);
        assert_eq!(station.max, 100.1);
        assert_eq!(station.avg(), 0.0);
    }
    #[test]
    fn it_aggregates_properly_and_rounds() {
        let mut station: StationResult = StationResult::default();
        station.add_reading(-5.0);
        station.add_reading(10.05);

        assert_eq!(station.min, -5.0);
        assert_eq!(station.max, 10.05);
        assert_eq!(station.avg(), 2.525);
    }
}
