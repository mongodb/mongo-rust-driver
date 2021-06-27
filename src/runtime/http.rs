#[cfg(feature = "tokio-runtime")]
use hyper::{
    body::{self, Buf},
    client::HttpConnector,
    header::LOCATION,
    Body,
    Client as HyperClient,
    Method,
    Request,
    Response,
    Uri,
};
#[cfg(feature = "tokio-runtime")]
use serde::Deserialize;
#[cfg(feature = "tokio-runtime")]
use std::{error::Error, future::Future, pin::Pin, str::FromStr};

#[derive(Clone, Debug, Default)]
pub(crate) struct HttpClient {
    #[cfg(feature = "tokio-runtime")]
    inner: HyperClient<HttpConnector>,
}

#[cfg(feature = "tokio-runtime")]
impl HttpClient {
    /// Executes an HTTP GET request and deserializes the JSON response.
    pub(crate) async fn get_and_deserialize_json<'a, T>(
        &self,
        uri: &str,
        headers: &'a [(&'a str, &'a str)],
    ) -> Result<T, Box<dyn Error>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let res = self
            .request(Method::GET, Uri::from_str(uri)?, headers)
            .await?;

        let mut buf = body::aggregate(res.into_body()).await?;
        let mut bytes = vec![0; buf.remaining()];
        buf.copy_to_slice(&mut bytes);

        let result = serde_json::from_slice(&bytes)?;
        Ok(result)
    }

    /// Executes an HTTP GET request and returns the response body as a string.
    pub(crate) async fn get_and_read_string<'a>(
        &self,
        uri: &str,
        headers: &'a [(&'a str, &'a str)],
    ) -> Result<String, Box<dyn Error>> {
        self.request_and_read_string(Method::GET, uri, headers)
            .await
    }

    /// Executes an HTTP PUT request and returns the response body as a string.
    pub(crate) async fn put_and_read_string<'a>(
        &self,
        uri: &str,
        headers: &'a [(&'a str, &'a str)],
    ) -> Result<String, Box<dyn Error>> {
        self.request_and_read_string(Method::PUT, uri, headers)
            .await
    }

    /// Executes an HTTP request and returns the response body as a string.
    pub(crate) async fn request_and_read_string<'a>(
        &self,
        method: Method,
        uri: &str,
        headers: &'a [(&'a str, &'a str)],
    ) -> Result<String, Box<dyn Error>> {
        let res = self.request(method, Uri::from_str(uri)?, headers).await?;

        let mut buf = body::aggregate(res.into_body()).await?;
        let mut bytes = vec![0; buf.remaining()];
        buf.copy_to_slice(&mut bytes);

        let text = String::from_utf8(bytes)?;
        Ok(text)
    }

    #[allow(clippy::type_complexity)]
    /// Executes an HTTP equest and returns the response.
    pub(crate) fn request<'a>(
        &'a self,
        method: Method,
        uri: Uri,
        headers: &'a [(&'a str, &'a str)],
    ) -> Pin<Box<dyn Future<Output = Result<Response<Body>, Box<dyn Error>>> + 'a + Send>> {
        Box::pin(async move {
            let mut request = Request::builder().uri(&uri).method(&method);

            for header in headers {
                request = request.header(header.0, header.1);
            }

            let request = request.body(Body::empty())?;
            let response = self.inner.request(request).await?;

            if response.status().is_redirection() {
                if let Some(Ok(location)) = response.headers().get(LOCATION).map(|u| u.to_str()) {
                    if let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) {
                        let uri = Uri::builder()
                            .scheme(scheme)
                            .authority(authority.as_str())
                            .path_and_query(location)
                            .build()?;
                        return self.request(method, uri, headers).await;
                    }
                }
            }

            Ok(response)
        })
    }
}
