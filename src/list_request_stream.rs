use std::{
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

use containerd_snapshots as snapshots;
use containerd_snapshots::Info;

use tokio_stream::Stream;

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
