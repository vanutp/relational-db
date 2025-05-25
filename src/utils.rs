pub fn escape(s: &str) -> String {
    s.replace("'", "''")
}

#[cfg(test)]
pub(crate) mod test {
    use rand::Rng;

    pub fn random_string() -> String {
        let mut rng = rand::rng();
        let len = rng.random_range(1..100);
        rng.sample_iter(&rand::distr::Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }
}
