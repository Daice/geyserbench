use {
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    std::{convert::TryInto, path::PathBuf},
    tonic::{
        Request, Response, Status,
        codec::Streaming,
        metadata::{AsciiMetadataValue, errors::InvalidMetadataValue},
        service::interceptor::InterceptedService,
        transport::{ClientTlsConfig, Endpoint, channel::Channel},
    },
};

#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(unix)]
use tonic::transport::Uri;

use crate::proto::geyser::{
    SubscribeDeshredRequest, SubscribeRequest, SubscribeUpdate, SubscribeUpdateDeshred,
    geyser_client::GeyserClient,
};

#[derive(Clone, Debug)]
pub struct InterceptorXToken {
    pub x_token: Option<AsciiMetadataValue>,
}

impl tonic::service::Interceptor for InterceptorXToken {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", token);
        }
        Ok(request)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcClientError {
    #[error("gRPC status: {0}")]
    TonicStatus(#[from] Status),
    #[error("Failed to send subscribe request: {0}")]
    SubscribeSendError(#[from] mpsc::SendError),
}

pub type GeyserGrpcClientResult<T> = Result<T, GeyserGrpcClientError>;

pub struct GeyserGrpcClient {
    geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>,
}

impl GeyserGrpcClient {
    pub fn build_from_shared(
        endpoint: impl Into<Bytes>,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcBuilder> {
        Ok(GeyserGrpcBuilder::new(Endpoint::from_shared(endpoint)?))
    }

    pub fn build_from_static(endpoint: &'static str) -> GeyserGrpcBuilder {
        GeyserGrpcBuilder::new(Endpoint::from_static(endpoint))
    }

    pub async fn subscribe(
        &mut self,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        self.subscribe_with_request(None).await
    }

    pub async fn subscribe_with_request(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdate, Status>>,
    )> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            subscribe_tx
                .send(request)
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        }
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    pub async fn subscribe_deshred(
        &mut self,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeDeshredRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdateDeshred, Status>>,
    )> {
        self.subscribe_deshred_with_request(None).await
    }

    pub async fn subscribe_deshred_with_request(
        &mut self,
        request: Option<SubscribeDeshredRequest>,
    ) -> GeyserGrpcClientResult<(
        impl Sink<SubscribeDeshredRequest, Error = mpsc::SendError>,
        impl Stream<Item = Result<SubscribeUpdateDeshred, Status>>,
    )> {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            subscribe_tx
                .send(request)
                .await
                .map_err(GeyserGrpcClientError::SubscribeSendError)?;
        }
        let response: Response<Streaming<SubscribeUpdateDeshred>> =
            self.geyser.subscribe_deshred(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    fn new(geyser: GeyserClient<InterceptedService<Channel, InterceptorXToken>>) -> Self {
        Self { geyser }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GeyserGrpcBuilderError {
    #[error("Failed to parse x-token: {0}")]
    MetadataValueError(#[from] InvalidMetadataValue),
    #[error("gRPC transport error: {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[cfg(not(unix))]
    #[error("Unix domain sockets are only supported on unix platforms")]
    UnsupportedUdsPlatform,
}

pub type GeyserGrpcBuilderResult<T> = Result<T, GeyserGrpcBuilderError>;

pub struct GeyserGrpcBuilder {
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
}

impl GeyserGrpcBuilder {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            x_token: None,
        }
    }

    pub async fn connect(self) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let channel = self.endpoint.connect().await?;
        self.build(channel)
    }

    #[cfg(unix)]
    pub async fn connect_uds(
        self,
        path: impl Into<PathBuf>,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let path = path.into();
        let channel = Endpoint::from_static("http://[::]:0")
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                let path = path.clone();
                async move {
                    let stream = UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await?;
        self.build(channel)
    }

    #[cfg(not(unix))]
    pub async fn connect_uds(
        self,
        _path: impl Into<PathBuf>,
    ) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let _ = self;
        Err(GeyserGrpcBuilderError::UnsupportedUdsPlatform)
    }

    fn build(self, channel: Channel) -> GeyserGrpcBuilderResult<GeyserGrpcClient> {
        let interceptor = InterceptorXToken {
            x_token: self.x_token,
        };
        let geyser = GeyserClient::with_interceptor(channel, interceptor);
        Ok(GeyserGrpcClient::new(geyser))
    }

    pub fn x_token<T>(mut self, x_token: Option<T>) -> GeyserGrpcBuilderResult<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        self.x_token = x_token.map(|value| value.try_into()).transpose()?;
        Ok(self)
    }

    pub fn tls_config(mut self, tls_config: ClientTlsConfig) -> GeyserGrpcBuilderResult<Self> {
        self.endpoint = self.endpoint.tls_config(tls_config)?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::GeyserGrpcClient;

    #[test]
    fn test_channel_http_success() {
        let endpoint = "http://127.0.0.1:10000";
        let x_token = "1234567891012141618202224268";

        let res = GeyserGrpcClient::build_from_shared(endpoint);
        assert!(res.is_ok());

        let res = res.unwrap().x_token(Some(x_token));
        assert!(res.is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_channel_uds_missing_socket_returns_error() {
        let res = GeyserGrpcClient::build_from_static("http://[::]:0")
            .connect_uds("/tmp/geyserbench-missing-yellowstone.sock")
            .await;
        assert!(res.is_err());
    }
}
