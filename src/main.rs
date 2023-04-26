use std::{
    collections::{HashMap, HashSet},
    ops::DerefMut,
    os::fd::{FromRawFd, IntoRawFd},
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};

use bytes::{Bytes, BytesMut};
use containerd_snapshots as snapshots;
use containerd_snapshots::{api, Info, Kind, Usage};
use futures::TryStreamExt;
use log::info;
use pin_project_lite::pin_project;
use snapshots::tonic::transport::Server;
use std::io::{Read, Write};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, ReadBuf};
use tokio::net::UnixListener;
use tokio::{sync::Mutex, time::Instant};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tracing::warn;

struct DataStore {
    info_vec: Vec<Info>,
    sha_fetched: HashSet<String>,
    // TODO: implement this for perisstent store
    _db_conn: tokio_rusqlite::Connection,
}

impl DataStore {
    async fn new(db_path: &str) -> Self {
        DataStore {
            info_vec: Vec::new(),
            sha_fetched: HashSet::new(),
            _db_conn: tokio_rusqlite::Connection::open(db_path).await.unwrap(),
        }
    }
}

struct SkySnapshotter {
    client: reqwest::Client,
    data_store: Arc<Mutex<DataStore>>,
    snapshot_dir: String,
}

#[derive(Debug)]
struct ImageRef {
    host: String,
    image: String,
    tag: String,
    manifest_url: String,
    blob_url_prefix: String,
}

fn parse_container_image_url(image_ref: &String) -> ImageRef {
    // TODO: handle more cases. This just handle forms like localhost:5000/image:latest case
    // docker tag is far more complex
    let parsed = regex::Regex::new(r"^(?P<host>[^/]+)/(?P<image>[^:]+):(?P<tag>.+)$")
        .unwrap()
        .captures(image_ref)
        .unwrap();
    let host = parsed.name("host").unwrap().as_str();
    let image = parsed.name("image").unwrap().as_str();
    let tag = parsed.name("tag").unwrap().as_str();
    let manifest_url = format!("http://{}/v2/{}/manifests/{}", host, image, tag);
    return ImageRef {
        host: host.to_string(),
        image: image.to_string(),
        tag: tag.to_string(),
        manifest_url: manifest_url,
        blob_url_prefix: format!("http://{}/v2/{}/blobs/", host, image),
    };
}
pin_project! {
    struct PrefetchReader<T: AsyncRead> {
        #[pin]
        inner_reader: T,
        // buff: ringbuf::HeapRb<u8>,
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
        let (mut buff_producer, mut buff_consumer) = buff.split();
        PrefetchReader {
            inner_reader: inner_reader,
            // buff: buff,
            buff_producer: buff_producer,
            buff_consumer: buff_consumer,
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
        return read;
        // match read {
        //     Poll::Ready(Ok(n)) => {
        //         // self.as_mut().project().buff_producer.push_slice(&buf[..n]);
        //         Poll::Ready(Ok(n))
        //     }
        //     Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        //     Poll::Pending => {
        //         Poll::Pending
        //         // let mut buff_consumer = self.as_mut().project().buff_consumer;
        //         // let n = buff_consumer.pop_slice(buf);
        //         // if n == 0 {
        //         //     Poll::Pending
        //         // } else {
        //         //     Poll::Ready(Ok(n))
        //         // }
        //     }
        // }

        // let n = futures::ready!(reader.as_mut().poll_read(cx, buf))?;
        // self.buf.extend_from_slice(&buf[..n]);
        // Poll::Ready(Ok(n))

        // let n = futures::ready!(reader.as_mut().poll_read(cx, buf))?;

        // start
    }
}

pin_project! {
    struct WrapperStream<T>
    where
        T: Stream<Item = reqwest::Result<Bytes>>,
    {
        #[pin]
        inner_stream: T,
    }
}

impl<T> WrapperStream<T>
where
    T: Stream<Item = reqwest::Result<Bytes>>,
{
    fn new(inner_stream: T) -> Self {
        WrapperStream {
            inner_stream: inner_stream,
        }
    }
}

impl<T> Stream for WrapperStream<T>
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

struct BlockingReader<T>
where
    T: Read,
{
    inner_reader: T,
}

impl<T> BlockingReader<T>
where
    T: Read,
{
    fn new(inner_reader: T) -> Self {
        BlockingReader {
            inner_reader: inner_reader,
        }
    }
}
impl<T> Read for BlockingReader<T>
where
    T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner_reader.read(buf) {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // spin here.
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
        }
    }
}

struct BlockingWriter<T>
where
    T: Write,
{
    inner_writer: T,
}

impl<T> BlockingWriter<T>
where
    T: Write,
{
    fn new(inner_writer: T) -> Self {
        BlockingWriter {
            inner_writer: inner_writer,
        }
    }
}

impl<T> Write for BlockingWriter<T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner_writer.write(buf) {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // spin here.
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        loop {
            match self.inner_writer.flush() {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // spin here.
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
        }
    }
}

impl SkySnapshotter {
    async fn new(db_path: &str, snapshot_dir: &str) -> Self {
        SkySnapshotter {
            client: reqwest::Client::new(),
            data_store: Arc::new(Mutex::new(DataStore::new(db_path).await)),
            snapshot_dir: snapshot_dir.to_string(),
        }
    }

    async fn fetch_image_manifest(&self, manifest_url: &String) -> json::JsonValue {
        let manifest_text = self
            .client
            .get(manifest_url)
            .header(
                "Accept",
                "application/vnd.docker.distribution.manifest.v2+json",
            )
            .send()
            .await
            .expect("Failed to fetch image manifest")
            .text()
            .await
            .expect("Failed to parse manifest");
        let parsed_json = json::parse(&manifest_text).unwrap();

        parsed_json
    }

    async fn parallel_head(
        &self,
        urls: Vec<String>,
    ) -> HashMap<String, reqwest::header::HeaderMap> {
        let mut tasks = tokio::task::JoinSet::new();
        let client_arc = Arc::new(self.client.clone());
        for url in urls {
            let client = client_arc.clone();
            tasks.spawn(async move {
                let resp = client.head(&url).send().await.unwrap();
                (url, resp.headers().clone())
            });
        }

        let mut responses = HashMap::new();
        while let Some(res) = tasks.join_next().await {
            let (url, header) = res.unwrap();
            responses.insert(url, header);
        }
        responses
    }

    async fn parallel_fetch_to_file(&self, url_to_path: HashMap<String, String>) -> () {
        let mut tasks = tokio::task::JoinSet::new();
        let client_arc = Arc::new(self.client.clone());
        for (url, path) in url_to_path {
            let client = client_arc.clone();

            {
                if self.data_store.lock().await.sha_fetched.contains(&url) {
                    info!("{} already fetched, skipping", url);
                    continue;
                }
            }

            tasks.spawn(async move {
                // remove `path` if the direcotry exists
                let dir_path = std::path::Path::new(&path);
                if dir_path.exists() {
                    warn!("{} exists, removing", dir_path.display());
                    std::fs::remove_dir_all(dir_path).unwrap();
                }

                // Use tokio tracing for more robust timing
                let start_time = Instant::now();
                let resp: reqwest::Response = client.get(&url).send().await.unwrap();
                let total_bytes = resp.content_length().unwrap();

                let raw_buff = ringbuf::HeapRb::<u8>::new(64 * 1024);
                let (mut write_raw, mut read_raw) = raw_buff.split();
                let decoded_buff = ringbuf::HeapRb::<u8>::new(64 * 1024);
                let (mut write_decoded, mut read_decoded) = decoded_buff.split();

                let decompression_thread = tokio::task::spawn_blocking(move || {
                    let reader = BlockingReader::new(&mut read_raw);
                    let mut writer = BlockingWriter::new(&mut write_decoded);
                    let mut gzip_reader = flate2::read::GzDecoder::new(reader);
                    std::io::copy(&mut gzip_reader, &mut writer).unwrap();
                });
                let untar_thread = tokio::task::spawn_blocking(move || {
                    let reader = BlockingReader::new(&mut read_decoded);
                    let mut tar = tar::Archive::new(reader);
                    tar.unpack(path).unwrap();
                });

                let mut byte_stream = tokio_util::io::StreamReader::new(
                    resp.bytes_stream()
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                );

                loop {
                    let mut buffer = [0; 8192];
                    let bytes_read = byte_stream.read(&mut buffer).await.unwrap();
                    let free_len = write_raw.free_len();
                    if bytes_read == 0 {
                        // EOF
                        break;
                    } else if bytes_read <= free_len {
                        write_raw.push_slice(&buffer[0..bytes_read]);
                    } else {
                        write_raw.push_slice(&buffer[0..free_len]);

                        let rest = &buffer[free_len..bytes_read];
                        loop {
                            // basically busy spin
                            tokio::task::yield_now().await;
                            if rest.len() <= write_raw.free_len() {
                                write_raw.push_slice(rest);
                                break;
                            }
                        }
                    }
                }

                drop(write_raw);
                decompression_thread.await.unwrap();
                untar_thread.await.unwrap();

                // Async version
                // let bytes_stream = resp.bytes_stream().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)).into_async_read();
                // let buf_reader = futures::io::BufReader::new(bytes_stream);

                // let mut bytes_stream = resp.bytes_stream();

                // let mut buffer = Vec::new();
                // {
                //     let writer = futures::io::BufWriter::new(&mut buffer);
                //     let mut gzip_decoder =
                //         async_compression::futures::write::GzipDecoder::new(writer);

                //     while let Some(bytes) = bytes_stream.next().await {
                //         let bytes = bytes.unwrap();
                //         gzip_decoder.write(&bytes).await.unwrap();
                //     }
                //     gzip_decoder.finish().await.unwrap();
                // }

                // // let gzip_reader = async_compression::futures::bufread::GzipDecoder::new(buf_reader);
                // let reader = futures::io::BufReader::new(buffer.as_slice());
                // async_tar::Archive::new(reader).unpack(path).await.unwrap();

                // Sync version
                // let bytes_arr = resp.bytes().await.unwrap();
                // let gzip_reader = flate2::read::GzDecoder::new(bytes_arr.as_ref());
                // tar::Archive::new(gzip_reader)
                //     .unpack(path)
                //     .unwrap();

                // Mixed version
                // let bytes_arr = resp.bytes().await.unwrap();
                // let bytes_stream = resp.bytes_stream();
                // .into_async_read();
                // let wrapper_stream = PrefetchReader::new(bytes_stream);
                // let wrapper_stream = WrapperStream::new(bytes_stream)
                //     .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
                //     .into_async_read();
                // let wrapper_stream = tokio_util::io::StreamReader::new(
                //     WrapperStream::new(bytes_stream)
                //         .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                // );

                // let chunk = resp.bytes_stream().next().await.unwrap().unwrap().len();
                // info!("{} chunk size", chunk);
                // 1 / 0;
                // let done_fetch = Instant::now();

                // let mut gzip_reader = async_compression::tokio::bufread::GzipDecoder::new(
                // futures::io::BufReader::new(bytes_arr.as_ref()),
                // tokio::io::BufReader::with_capacity(bytes_arr.len() / 10, bytes_arr.as_ref()),
                // bytes_stream,
                // buff_read,
                // tokio::io::BufReader::with_capacity(64 * 1024, wrapper_stream),
                // );

                // let mut buf = vec![0; total_bytes.try_into().unwrap()];
                // gzip_reader.read_to_end(&mut buf).await.unwrap();
                // info!(
                //     "{} bytes before, {} bytes after decompression",
                //     total_bytes,
                //     buf.len()
                // );

                // let prefetch_reader = PrefetchReader::new(gzip_reader);
                // let buffer_reader = tokio::io::BufReader::with_capacity(1_000_000_000, gzip_reader);

                // let done_unzip = Instant::now();

                // let mut decoded_reader = flate2::bufread::GzDecoder::new(bytes_arr.as_ref());
                // let mut buf = vec![0; total_bytes.try_into().unwrap()];
                // decoded_reader.read_to_end(&mut buf).unwrap();
                // tokio_tar::Archive::new(buf.as_slice())
                // tokio_tar::Archive::new(gzip_reader)
                // tokio_tar::Archive::new(prefetch_reader)
                // tokio_tar::Archive::new(buffer_reader)
                //     .unpack(path)
                //     .await
                //     .unwrap();
                // let done_untar = Instant::now();

                // let mut archive = tokio_tar::Archive::new(buf.as_slice());
                // let mut entries = archive.entries().unwrap();
                // let mut join_set = tokio::task::JoinSet::new();
                // while let Some(file) = entries.next().await {
                //     let path = path.clone();
                //     join_set.spawn(async move { file.unwrap().unpack_in(path).await.unwrap() });
                // }
                // while let Some(res) = join_set.join_next().await {
                //     res.unwrap();
                // }

                // let download_duration = done_fetch.duration_since(start_time);
                // let unzip_duration = done_unzip.duration_since(done_fetch);
                // let untar_duration = done_untar.duration_since(done_unzip);
                let total_duration = Instant::now().duration_since(start_time);
                info!(
                    "{}, {} bytes, fetched in {:?}",
                    url, total_bytes, total_duration
                );
                // info!(
                //     "{}, {} bytes, fetched in {:?}, untar: {:?}, unzip: {:?}, download: {:?}",
                //     url,
                //     total_bytes,
                //     total_duration,
                //     untar_duration,
                //     unzip_duration,
                //     download_duration
                // );

                return url;
            });
        }

        while let Some(res) = tasks.join_next().await {
            let url = res.unwrap();
            self.data_store.lock().await.sha_fetched.insert(url);
        }
    }
}

#[derive(Debug)]
struct OCILayer {
    media_type: String,
    digest: String,
    size: u64,
}

fn clone_info_hack(info: &Info) -> Info {
    // a hack because the Info doesn't have copy trait
    return serde_json::from_str(&serde_json::to_string(info).unwrap()).unwrap();
}

#[snapshots::tonic::async_trait]
impl snapshots::Snapshotter for SkySnapshotter {
    type Error = snapshots::tonic::Status;

    async fn stat(&self, key: String) -> Result<Info, Self::Error> {
        info!("Stat: {}", key);
        self.data_store
            .lock()
            .await
            .info_vec
            .iter()
            .find(|info| info.name == key)
            .map(|info| clone_info_hack(info))
            .ok_or(snapshots::tonic::Status::not_found(
                "Not found from skysnaphotter",
            ))
    }

    async fn update(
        &self,
        info: Info,
        fieldpaths: Option<Vec<String>>,
    ) -> Result<Info, Self::Error> {
        info!("Update: info={:?}, fieldpaths={:?}", info, fieldpaths);
        Ok(Info::default())
    }

    async fn usage(&self, key: String) -> Result<Usage, Self::Error> {
        info!("Usage: {}", key);
        Ok(Usage::default())
    }

    async fn mounts(&self, key: String) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("Mounts: {}", key);
        Ok(Vec::new())
    }

    async fn prepare(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!(
            "Prepare: key={}, parent={}, labels={:?}",
            key, parent, labels
        );

        {
            self.data_store.lock().await.info_vec.push(Info {
                kind: Kind::Committed,
                name: key,
                parent: parent,
                labels: labels.clone(),
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
            });
        }

        let image_ref = labels.get("containerd.io/snapshot/cri.image-ref").unwrap();

        // {
        //     let image_layers = labels
        //         .get("containerd.io/snapshot/cri.image-layers")
        //         .unwrap().split(",").collect::<Vec<&str>>();
        //     let layer_digest = labels
        //         .get("containerd.io/snapshot/cri.layer-digest")
        //         .unwrap();
        // }

        let image_ref_struct = parse_container_image_url(&image_ref);
        let manifest = self
            .fetch_image_manifest(&image_ref_struct.manifest_url)
            .await;
        let layers = manifest["layers"]
            .members()
            .map(|layer| {
                (
                    layer["digest"].to_string(),
                    OCILayer {
                        media_type: layer["mediaType"].to_string(),
                        digest: layer["digest"].to_string(),
                        size: layer["size"].as_u64().unwrap(),
                    },
                )
            })
            .collect::<HashMap<String, OCILayer>>();
        let blobs_url_to_layers = layers
            .iter()
            .map(|(digest, _)| {
                (
                    format!("{}{}", image_ref_struct.blob_url_prefix, digest),
                    digest.clone(),
                )
            })
            .collect::<HashMap<String, String>>();
        let headers = self
            .parallel_head(
                blobs_url_to_layers
                    .keys()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<String>>(),
            )
            .await;
        for (url, header) in headers {
            let spec = layers.get(blobs_url_to_layers.get(&url).unwrap()).unwrap();
            assert!(spec.media_type == "application/vnd.docker.image.rootfs.diff.tar.gzip");
            assert!(
                spec.size
                    == header
                        .get("content-length")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<u64>()
                        .unwrap()
            );
        }
        let blobs_url_to_path = blobs_url_to_layers
            .iter()
            .map(|(url, digest)| (url.clone(), format!("{}/{}", self.snapshot_dir, digest)))
            .collect::<HashMap<String, String>>();
        self.parallel_fetch_to_file(blobs_url_to_path).await;

        info!("");

        Err(snapshots::tonic::Status::already_exists("already exists"))
    }

    async fn view(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("View: key={}, parent={}, labels={:?}", key, parent, labels);
        Ok(Vec::new())
    }

    async fn commit(
        &self,
        name: String,
        key: String,
        labels: HashMap<String, String>,
    ) -> Result<(), Self::Error> {
        info!("Commit: name={}, key={}, labels={:?}", name, key, labels);
        Ok(())
    }

    async fn remove(&self, key: String) -> Result<(), Self::Error> {
        info!("Remove: {}", key);
        Ok(())
    }

    type InfoStream = WalkStream;
    async fn list(&self) -> Result<Self::InfoStream, Self::Error> {
        info!("List: ");
        let mut stream = WalkStream::new();
        {
            stream.infos.extend(
                self.data_store
                    .lock()
                    .await
                    .info_vec
                    .iter()
                    .map(|info| clone_info_hack(info)),
            );
        }

        Ok(stream)
    }
}

struct WalkStream {
    infos: Vec<Info>,
}
impl WalkStream {
    fn new() -> Self {
        WalkStream { infos: Vec::new() }
    }
}

impl Stream for WalkStream {
    type Item = Result<Info, snapshots::tonic::Status>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Poll::Ready(None)
        let next: Option<Info> = self.deref_mut().infos.pop();
        match next {
            Some(info) => {
                return Poll::Ready(Some(Ok(info)));
            }
            None => {
                return Poll::Ready(None);
            }
        }
    }
}

#[tokio::main()]
#[cfg(unix)]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .init();

    let socket_path = "/tmp/sky.sock";
    let db_path = "/tmp/sky-snaphotter.db";
    let snapshot_dir = "/tmp/sky-snapshots";

    // mkdir snapshot_dir if it doesn't exist
    if let Err(_) = std::fs::metadata(snapshot_dir) {
        std::fs::create_dir(snapshot_dir).expect("Failed to create snapshot dir");
    }

    // remove socket_path if it exists
    if let Ok(_) = std::fs::metadata(socket_path) {
        std::fs::remove_file(socket_path).expect("Failed to remove socket");
    }

    let sky_snapshotter = SkySnapshotter::new(db_path, snapshot_dir).await;

    let incoming = {
        let uds = UnixListener::bind(socket_path).expect("Failed to bind listener");
        UnixListenerStream::new(uds)
    };

    Server::builder()
        .layer(tower_http::trace::TraceLayer::new_for_grpc())
        .add_service(snapshots::server(Arc::new(sky_snapshotter)))
        .serve_with_incoming(incoming)
        .await
        .expect("Serve failed");
}
