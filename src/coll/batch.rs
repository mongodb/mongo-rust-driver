// Splits off elements from `all` so that the sum of sizes in `all` is not greater than
// `max_batch_size`. Any remaining elements will be returned in a separate vector.
pub(crate) fn split_off_batch<T>(
    all: &mut Vec<T>,
    max_batch_size: usize,
    get_size: impl Fn(&T) -> usize,
) -> Vec<T> {
    if all.is_empty() {
        return Vec::new();
    }

    let mut batch_size = get_size(&all[0]);

    for i in 1..all.len() {
        let elem_size = get_size(&all[i]);

        if batch_size + elem_size > max_batch_size {
            return all.split_off(i);
        }

        batch_size += elem_size;
    }

    Vec::new()
}

#[cfg(test)]
mod test {
    use super::split_off_batch;

    #[test]
    fn split_empty_batch() {
        let mut all: Vec<i32> = Vec::new();

        assert!(split_off_batch(&mut all, 10, |_| 1).is_empty());
    }

    #[test]
    fn split_single_batch() {
        let mut all = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        assert!(split_off_batch(&mut all, 10, |_| 1).is_empty());
    }

    #[test]
    fn split_multi_batch() {
        let mut all = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let rest = split_off_batch(&mut all, 3, |_| 1);

        assert_eq!(all, vec![1, 2, 3]);
        assert_eq!(rest, vec![4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn split_batches_until_empty() {
        let mut batches = Vec::new();
        let mut all = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        loop {
            let batch = split_off_batch(&mut all, 3, |_| 1);
            if batch.is_empty() {
                break;
            }
            batches.push(std::mem::replace(&mut all, batch));
        }

        assert_eq!(all, vec![10]);
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], vec![1, 2, 3]);
        assert_eq!(batches[1], vec![4, 5, 6]);
        assert_eq!(batches[2], vec![7, 8, 9]);
    }

    #[test]
    fn split_batch_with_too_large_element() {
        let mut all = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let rest = split_off_batch(&mut all, 3, |_| 5);

        assert_eq!(all, vec![1]);
        assert_eq!(rest, vec![2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }
}
