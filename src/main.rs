use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
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

use tracing::{info, warn};

mod list_request_stream;
mod sync_io_utils;

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
struct ImageRegistryUrls {
    manifest_url: String,
    blob_url_prefix: String,
}

fn parse_container_image_url(image_ref: &str) -> ImageRegistryUrls {
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
    ImageRegistryUrls {
        manifest_url,
        blob_url_prefix: format!("http://{}/v2/{}/blobs/", host, image),
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

    async fn fetch_image_manifest(&self, manifest_url: &String) -> ImageManifest {
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

        ImageManifest::from_reader(manifest_text.as_bytes()).unwrap()
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

    async fn parallel_fetch_to_file(&self, url_to_path: HashMap<String, String>) {
        let mut tasks = tokio::task::JoinSet::new();
        let client_arc = Arc::new(self.client.clone());
        for (url, path) in url_to_path {
            let client = client_arc.clone();
            let url_clone = url.clone();

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

                let start = minstant::Instant::now();

                let resp: reqwest::Response = client.get(&url).send().await.unwrap();

                // TODO: use total_bytes to help infer buffer size to manage the relative buffer size and download throughput.
                let total_bytes = resp.content_length().unwrap();
                let raw_to_decode_buff = async_ringbuf::AsyncHeapRb::<u8>::new(64 * 1024 * 1024);
                let (mut write_raw, read_raw) = raw_to_decode_buff.split();
                let decode_to_untar_buff = async_ringbuf::AsyncHeapRb::<u8>::new(64 * 1024 * 1024);
                let (mut write_decoded, mut read_decoded) = decode_to_untar_buff.split();

                let mut work_set = tokio::task::JoinSet::new();
                work_set.spawn(async move {
                    let bufreader = tokio::io::BufReader::with_capacity(64 * 1024, read_raw);
                    let mut reader = async_compression::tokio::bufread::GzipDecoder::new(bufreader);
                    tokio::io::copy(&mut reader, &mut write_decoded)
                        .await
                        .unwrap();
                });
                let untar_thread = tokio::task::spawn_blocking(move || {
                    let read_decoded_sync = read_decoded.as_mut_base();
                    let mut archive =
                        tar::Archive::new(sync_io_utils::BlockingReader::new(read_decoded_sync));
                    archive.unpack(path).unwrap();
                });

                let mut output_from_socket = tokio_util::io::StreamReader::new(
                    resp.bytes_stream()
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
                );
                tokio::io::copy_buf(&mut output_from_socket, &mut write_raw)
                    .await
                    .unwrap();

                while let Some(res) = work_set.join_next().await {
                    res.unwrap();
                }
                untar_thread.await.unwrap();

                info!(
                    "Fetched {} in {}ms, {:.2}mb",
                    url_clone,
                    start.elapsed().as_millis(),
                    total_bytes as f64 / 1e6
                );

                url
            });
        }

        while let Some(res) = tasks.join_next().await {
            let url = res.unwrap();
            self.data_store.lock().await.sha_fetched.insert(url);
        }
    }
}

fn clone_info_hack(info: &Info) -> Info {
    // a hack because the Info doesn't have copy trait
    serde_json::from_str(&serde_json::to_string(info).unwrap()).unwrap()
}

#[snapshots::tonic::async_trait]
impl snapshots::Snapshotter for SkySnapshotter {
    type Error = snapshots::tonic::Status;

    async fn prepare(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("Prepare: key={}, parent={}", key, parent,);

        {
            self.data_store.lock().await.info_vec.push(Info {
                kind: Kind::Committed,
                name: key,
                parent,
                labels: labels.clone(),
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
            });
        }

        let image_ref = labels.get("containerd.io/snapshot/cri.image-ref").unwrap();

        let image_ref_struct = parse_container_image_url(image_ref);
        let manifest = self
            .fetch_image_manifest(&image_ref_struct.manifest_url)
            .await;
        let layers = manifest
            .layers()
            .iter()
            .map(|layer| (layer.digest().to_string(), layer))
            .collect::<HashMap<String, &Descriptor>>();
        let blobs_url_to_layers = layers
            .keys()
            .map(|digest| {
                (
                    format!("{}{}", image_ref_struct.blob_url_prefix, digest),
                    digest.clone(),
                )
            })
            .collect::<HashMap<String, String>>();
        let headers = self
            .parallel_head(blobs_url_to_layers.keys().cloned().collect::<Vec<String>>())
            .await;
        for (url, header) in headers {
            let spec = layers.get(blobs_url_to_layers.get(&url).unwrap()).unwrap();
            assert!(
                spec.media_type().to_string()
                    == "application/vnd.docker.image.rootfs.diff.tar.gzip"
            );
            assert!(
                spec.size()
                    == header
                        .get("content-length")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<i64>()
                        .unwrap()
            );
        }
        let blobs_url_to_path = blobs_url_to_layers
            .iter()
            .map(|(url, digest)| (url.clone(), format!("{}/{}", self.snapshot_dir, digest)))
            .collect::<HashMap<String, String>>();
        self.parallel_fetch_to_file(blobs_url_to_path).await;

        Err(snapshots::tonic::Status::already_exists("already exists"))
    }

    type InfoStream = list_request_stream::WalkStream;
    async fn list(&self) -> Result<Self::InfoStream, Self::Error> {
        info!("List: ");
        let mut stream = list_request_stream::WalkStream::new();
        {
            stream.infos.extend(
                self.data_store
                    .lock()
                    .await
                    .info_vec
                    .iter()
                    .map(clone_info_hack),
            );
        }

        Ok(stream)
    }

    async fn stat(&self, key: String) -> Result<Info, Self::Error> {
        info!("Stat: {}", key);
        self.data_store
            .lock()
            .await
            .info_vec
            .iter()
            .find(|info| info.name == key)
            .map(clone_info_hack)
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
    if std::fs::metadata(snapshot_dir).is_err() {
        std::fs::create_dir(snapshot_dir).expect("Failed to create snapshot dir");
    }

    // remove socket_path if it exists
    if std::fs::metadata(socket_path).is_ok() {
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
