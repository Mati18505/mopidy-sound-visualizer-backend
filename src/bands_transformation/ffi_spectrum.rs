pub fn band_width(sampling_rate: u32, bands_count: u32) -> f32 {
    assert!(sampling_rate > 0);
    assert!(bands_count > 0);

    let nyquist: f32 = sampling_rate as f32 / 2.0;

    nyquist / bands_count as f32
}

pub fn get_freq_for_band_n(band_width: f32, n: u32) -> u32 {
    f32::round(n as f32 * band_width) as u32
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_ffi_spectrum() {
        let sampling_rate = 44100;
        let bands_count = 2000;
        let expected_width = 11.025;

        let cases: HashMap<u32, u32> = HashMap::from([
            (0, 0),
            (10, 110),
            (20, 221),
            (200, f32::round(expected_width * 200.0) as u32),
            (bands_count, sampling_rate / 2),
            (bands_count / 2, sampling_rate / 4),
        ]);

        assert_eq!(band_width(sampling_rate, bands_count), expected_width);

        for (band, expected) in cases {
            let bw = band_width(sampling_rate, bands_count);
            let result = get_freq_for_band_n(bw, band);
            assert_eq!(result, expected);
        }
    }
}
