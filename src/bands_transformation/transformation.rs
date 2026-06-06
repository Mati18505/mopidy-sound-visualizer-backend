use lerp::Lerp;

use super::{ffi_spectrum, log_partition};

#[derive(Debug)]
pub struct Config {
    /// Should be the same as in spectrum (negative value in db).
    pub db_threshold: f32,
    /// Should be the same as in spectrum (e.g. 44100, 96000).
    pub sampling_rate: u32,
}

pub fn transform_bands(bands: Vec<f32>, config: Config) -> Vec<f32> {
    assert!(config.db_threshold < 0.0);

    let bands: Vec<f32> = bands
        .iter()
        .map(|amp| *amp + -config.db_threshold)
        .collect();
    let bands: Vec<Option<f32>> = transform_ffi_to_log_scale(bands, config.sampling_rate);
    interpolate_empty(bands)
}

fn none_to_zero(data: Vec<Option<f32>>) -> Vec<f32> {
    data.iter().map(|band| band.unwrap_or_default()).collect()
}

// For each None band search for its nearest Some bands into left and right.
// Interpolate with them.
// i = x, band.val = y
fn interpolate_empty(data: Vec<Option<f32>>) -> Vec<f32> {
    let mut nearest_left: Option<(usize, f32)> = None;

    data.iter()
        .enumerate()
        .map(|(i, band)| match band {
            Some(band) => {
                nearest_left = Some((i, *band));
                *band
            }
            None => {
                let nearest_right: Option<(usize, f32)> = data[i + 1..data.len()]
                    .iter()
                    .position(|e| e.is_some())
                    .map(|idx| (i + 1 + idx, data.get(i + 1 + idx).unwrap().unwrap()));

                match (nearest_left, nearest_right) {
                    (None, None) => 0.0,
                    (None, Some(rv)) => rv.1,
                    (Some(lv), None) => lv.1,
                    (Some(lv), Some(rv)) => {
                        let li = lv.0 as f32;
                        let ri = rv.0 as f32;
                        let i = i as f32;

                        let t = (i - li) / (ri - li);

                        debug_assert!((0.0..=1.0).contains(&t));
                        lv.1.lerp(rv.1, t)
                    }
                }
            }
        })
        .collect()
}

#[derive(Default, Clone, Copy)]
struct LogBandAggregator {
    sum_ffi_amplitudes: f32,
    ffi_bands_count: u32,
}

impl LogBandAggregator {
    fn add_ffi_band(&mut self, amplitude: f32) {
        self.sum_ffi_amplitudes += amplitude;
        self.ffi_bands_count += 1;
    }

    fn get_avg(&self) -> Option<f32> {
        if self.ffi_bands_count > 0 {
            Some(self.sum_ffi_amplitudes / self.ffi_bands_count as f32)
        } else {
            None
        }
    }
}

// Use two pointers technique to optimize to O(ffi_bands).
// This assumes that ffi_band_freq(n+1) > ffi_band_freq(n)
fn transform_ffi_to_log_scale(ffi_bands: Vec<f32>, sampling_rate: u32) -> Vec<Option<f32>> {
    let ffi_bands_count: usize = ffi_bands.len();
    let log_bands_count: usize = 100;
    let bw = ffi_spectrum::band_width(sampling_rate, ffi_bands_count as u32);

    let log_freq_ranges = log_partition::create_freq_tuples(log_bands_count as u32);

    let mut log_band_aggregators: Vec<LogBandAggregator> =
        vec![Default::default(); log_freq_ranges.len()];
    let mut ffi_idx: usize = 0;
    let mut log_idx: usize = 0;

    while ffi_idx < ffi_bands_count && log_idx < log_bands_count {
        let ffi_band_freq = ffi_spectrum::get_freq_for_band_n(bw, ffi_idx as u32);
        let curr_log_freq_range = log_freq_ranges[log_idx];

        if log_partition::freq_in_range(curr_log_freq_range, ffi_band_freq) {
            log_band_aggregators[log_idx].add_ffi_band(ffi_bands[ffi_idx]);
            ffi_idx += 1;
        } else if ffi_band_freq < curr_log_freq_range.0 {
            ffi_idx += 1;
        } else {
            log_idx += 1;
        }
    }

    log_band_aggregators
        .iter()
        .map(|log_band_aggregator| log_band_aggregator.get_avg())
        .collect()
}
