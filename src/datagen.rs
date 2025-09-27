// arrow array generation code, from arrow-rs

use arrow::array::{ArrowPrimitiveType, GenericStringArray, OffsetSizeTrait, PrimitiveArray};

use rand::{
    Rng, SeedableRng,
    distr::{Alphanumeric, Distribution, StandardUniform},
    prelude::StdRng,
};
/// Creates a [`PrimitiveArray`] of a given `size` and `null_density`
/// filling it with random numbers generated using the provided `seed`.
pub fn create_primitive_array_with_seed<T>(
    size: usize,
    null_density: f32,
    seed: u64,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    StandardUniform: Distribution<T::Native>,
{
    let mut rng = StdRng::seed_from_u64(seed);

    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.random())
            }
        })
        .collect()
}

/// Creates a random (but fixed-seeded) array of rand size with a given max size, null density and length
pub fn create_string_array_with_max_len<Offset: OffsetSizeTrait>(
    size: usize,
    null_density: f32,
    max_str_len: usize,
    seed: u64,
) -> GenericStringArray<Offset> {
    let mut rng = StdRng::seed_from_u64(seed);

    let rng = &mut rng;
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                let str_len = rng.random_range(0..max_str_len);
                let value = rng.sample_iter(&Alphanumeric).take(str_len).collect();
                let value = String::from_utf8(value).unwrap();
                Some(value)
            }
        })
        .collect()
}
