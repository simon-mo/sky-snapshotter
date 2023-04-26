use bytes::Bytes;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, ReadBuf};

pin_project! {
    struct PrefetchReader<T: AsyncRead> {
        #[pin]
        inner_reader: T,
        buff_producer: ringbuf::HeapProducer<u8>,
        buff_consumer: ringbuf::HeapConsumer<u8>,
    }
}

impl<T> PrefetchReader<T>
where
    T: AsyncRead,
{
    fn new(inner_reader: T) -> Self {
        let buff = ringbuf::HeapRb::<u8>::new(64 * 1024);
        let (buff_producer, buff_consumer) = buff.split();
        PrefetchReader {
            inner_reader,
            buff_producer,
            buff_consumer,
        }
    }
}

impl<T> AsyncRead for PrefetchReader<T>
where
    T: AsyncRead,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        info!("requested readbuf {:?}", buf);
        let read = self.as_mut().project().inner_reader.poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = read {
            info!("after poll readbuf {:?}", buf);
        }
        read
    }
}

pin_project! {
    struct PrefetchStream<T>
    where
        T: Stream<Item = reqwest::Result<Bytes>>,
    {
        #[pin]
        inner_stream: T,
    }
}

impl<T> PrefetchStream<T>
where
    T: Stream<Item = reqwest::Result<Bytes>>,
{
    fn new(inner_stream: T) -> Self {
        PrefetchStream { inner_stream }
    }
}

impl<T> Stream for PrefetchStream<T>
where
    T: Stream<Item = reqwest::Result<Bytes>>,
{
    type Item = reqwest::Result<Bytes>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<reqwest::Result<Bytes>>> {
        let mut bytes_pulled = Vec::<u8>::with_capacity(516096);
        let mut inner_stream = self.project().inner_stream;

        loop {
            match inner_stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    bytes_pulled.extend_from_slice(&bytes);
                    // if bytes_pulled.len() > 10 {
                    //     info!("pulled {} bytes", bytes_pulled.len());
                    //     return Poll::Ready(Some(Ok(bytes_pulled.freeze())));
                    // }
                }
                Poll::Ready(Some(Err(e))) => {
                    // TODO: we should figure what to do with the existing bytes_pulled buffer
                    assert!(bytes_pulled.is_empty());
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    if bytes_pulled.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        // info!("Poll::Ready(None) pulled {} bytes", bytes_pulled.len());
                        return Poll::Ready(Some(Ok(bytes_pulled.into())));
                    }
                }
                Poll::Pending => {
                    if bytes_pulled.is_empty() {
                        return Poll::Pending;
                    } else {
                        // info!("Poll::Pending pulled {} bytes", bytes_pulled.len());
                        return Poll::Ready(Some(Ok(bytes_pulled.into())));
                    }
                }
            }
        }
    }
}
