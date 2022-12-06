#[cfg(test)]
mod example;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn local_rules() -> anyhow::Result<()> {
        super::example::local_rules::example().await
    }

    #[tokio::test]
    async fn server_side_enforcement() -> anyhow::Result<()> {
        super::example::server_side_enforcement::example().await
    }
}
