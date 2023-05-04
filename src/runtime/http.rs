use reqwest::{Method, Response, IntoUrl};
use serde::Deserialize;

#[derive(Clone, Debug, Default)]
pub(crate) struct HttpClient {
    inner: reqwest::Client,
}

impl HttpClient {
    /// Executes an HTTP GET request and deserializes the JSON response.
    pub(crate) async fn get_and_deserialize_json<'a, T>(
        &self,
        uri: impl IntoUrl,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> reqwest::Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let value = self
            .request(Method::GET, uri, headers)
            .await?
            .json()
            .await?;

        Ok(value)
    }

    /// Executes an HTTP GET request and returns the response body as a string.
    #[allow(unused)]
    pub(crate) async fn get_and_read_string<'a>(
        &self,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> reqwest::Result<String> {
        self.request_and_read_string(Method::GET, uri, headers)
            .await
    }

    /// Executes an HTTP PUT request and returns the response body as a string.
    #[allow(unused)]
    pub(crate) async fn put_and_read_string<'a>(
        &self,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> reqwest::Result<String> {
        self.request_and_read_string(Method::PUT, uri, headers)
            .await
    }

    /// Executes an HTTP request and returns the response body as a string.
    #[allow(unused)]
    pub(crate) async fn request_and_read_string<'a>(
        &self,
        method: Method,
        uri: &str,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> reqwest::Result<String> {
        let text = self.request(method, uri, headers).await?.text().await?;

        Ok(text)
    }

    /// Executes an HTTP request and returns the response.
    pub(crate) async fn request<'a>(
        &self,
        method: Method,
        uri: impl IntoUrl,
        headers: impl IntoIterator<Item = &'a (&'a str, &'a str)>,
    ) -> reqwest::Result<Response> {
        let response = headers
            .into_iter()
            .fold(self.inner.request(method, uri), |request, (k, v)| {
                request.header(*k, *v)
            })
            .send()
            .await?;

        Ok(response)
    }
}
