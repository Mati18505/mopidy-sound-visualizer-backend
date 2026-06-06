pub fn create_freq_tuples(len: u32) -> Vec<(u32, u32)> {
    assert!(len > 0);

    let frequencies: Vec<u32> = create_freq_arr(len + 1);
    create_tuples(frequencies)
}

pub fn get_tuple_containing_freq(tuple_list: &[(u32, u32)], freq: u32) -> Option<(u32, u32)> {
    tuple_list
        .iter()
        .find(|freq_range| freq_in_range(**freq_range, freq))
        .copied()
}

pub fn get_tuple_index_containing_freq(tuple_list: &[(u32, u32)], freq: u32) -> Option<usize> {
    tuple_list
        .iter()
        .position(|freq_range| freq_in_range(*freq_range, freq))
}

pub fn freq_in_range(tuple: (u32, u32), freq: u32) -> bool {
    (tuple.0..=tuple.1).contains(&freq)
}

fn create_freq_arr(len: u32) -> Vec<u32> {
    assert!(len > 1);

    (0..len)
        .map(|n| n as f32 / (len - 1) as f32)
        .map(f)
        .collect()
}

fn f(t: f32) -> u32 {
    assert!(t <= 1.0);

    let min = 20.0;
    let max = 20000.0;

    f32::round(min * f32::powf(max / min, t)) as u32
}

fn create_tuples(list: Vec<u32>) -> Vec<(u32, u32)> {
    list.windows(2).map(|w| (w[0], w[1])).collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_f() {
        assert_eq!(f(0.0), 20);
        assert_eq!(f(0.5), 632);
        assert_eq!(f(1.0), 20000);
    }

    #[test]
    fn test_create_freq_arr() {
        let arr: Vec<u32> = vec![20, 200, 2000, 20000];

        assert_eq!(create_freq_arr(4), arr);
    }

    #[test]
    fn test_create_freq_tuples() {
        let tuples: Vec<(u32, u32)> = vec![(20, 112), (112, 632), (632, 3557), (3557, 20000)];

        assert_eq!(create_freq_tuples(4), tuples);
    }
}
