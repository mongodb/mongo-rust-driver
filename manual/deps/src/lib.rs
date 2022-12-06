#[cfg(test)]
mod example;

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn examples() -> anyhow::Result<()> {
        super::example::local_rules::example().await?;
        super::example::server_side_enforcement::example().await?;
        Ok(())
    }
}
