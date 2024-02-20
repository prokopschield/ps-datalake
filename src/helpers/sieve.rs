// this prime sieve was adapted from https://github.com/PlummersSoftwareLLC/Primes/blob/drag-race/PrimeRust/solution_2/src/prime_object.rs

pub fn get_le_prime(limit: u32) -> u32 {
    let q = (limit as f32).sqrt() as u32;
    let mut factor = 3;
    let mut bits: Vec<bool> = vec![true; (limit as usize + 1) >> 1];

    while factor < q {
        let mut num = factor;

        while num < q {
            if bits[num as usize >> 1] {
                factor = num;
                break;
            }

            num += 2;
        }

        num = factor * factor;

        while num <= limit {
            bits[num as usize >> 1] = false;
            num += factor * 2;
        }

        factor += 2;
    }

    let mut r = limit as usize;

    if r % 2 == 0 {
        r -= 1
    }

    while r > 2 {
        if bits[r >> 1] {
            return r as u32;
        }

        r -= 2;
    }

    2
}
