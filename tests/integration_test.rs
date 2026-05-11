use mopidy_sound_visualizer::{ffi_spectrum, log_partition};


#[test]
fn ffi_spectrum_log_partition_integration() {
    let sampling_rate: u32 = 44100;
    let bands_count: u32 = 2000;
    let log_partition_bands: u32 = 20;

    let bw = ffi_spectrum::band_width(sampling_rate, bands_count);

    for ffi_band_n in 0..bands_count {
        let ffi_band_freq =
            ffi_spectrum::get_freq_for_band_n(bw, ffi_band_n);
        let appropiate_log_band = log_partition::get_tuple_containing_freq(
            &log_partition::create_freq_tuples(log_partition_bands),
            ffi_band_freq,
        );

        dbg!(ffi_band_freq);
        dbg!(appropiate_log_band);

        if let Some((min, max)) = appropiate_log_band {
            assert!(ffi_band_freq >= min && ffi_band_freq <= max);
        }
    }
}
