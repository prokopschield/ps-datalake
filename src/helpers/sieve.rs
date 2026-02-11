// this prime sieve was adapted from https://github.com/PlummersSoftwareLLC/Primes/blob/drag-race/PrimeRust/solution_2/src/prime_object.rs

#[must_use]
pub fn get_le_prime(limit: u32) -> u32 {
    let q = limit.isqrt();
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

    let mut r = limit;

    if r.is_multiple_of(2) {
        r -= 1;
    }

    while r > 2 {
        if bits[r as usize >> 1] {
            return r;
        }

        r -= 2;
    }

    2
}
