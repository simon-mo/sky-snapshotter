use std::{
    collections::{HashMap, HashSet},
    ops::DerefMut,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};

use containerd_snapshots as snapshots;
use containerd_snapshots::{api, Info, Kind, Usage};
use futures::TryStreamExt;
use oci_spec::image::{Descriptor, ImageManifest};
use snapshots::tonic::transport::Server;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::Stream;
use tracing::{info, info_span, warn, Instrument};

pub struct WalkStream {
    pub infos: Vec<Info>,
}
impl WalkStream {
    pub fn new() -> Self {
        WalkStream { infos: Vec::new() }
    }
}

impl Stream for WalkStream {
    type Item = Result<Info, snapshots::tonic::Status>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Poll::Ready(None)
        let next: Option<Info> = self.deref_mut().infos.pop();
        match next {
            Some(info) => Poll::Ready(Some(Ok(info))),
            None => Poll::Ready(None),
        }
    }
}
