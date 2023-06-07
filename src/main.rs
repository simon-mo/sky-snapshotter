use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    path::Path,
    sync::Arc,
    time::SystemTime,
    vec,
};

use containerd_snapshots as snapshots;
use containerd_snapshots::{api, Info, Kind, Usage};
use futures::TryStreamExt;
use oci_spec::image::{Descriptor, ImageManifest};
use snapshots::tonic::transport::Server;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;

use tracing::info;

mod list_request_stream;
mod sync_io_utils;

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::Read;
use std::io::SeekFrom;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SplitMetadata {
    path: String,
    start_offset: u32,
    chunk_size: u32,
    total_size: u64,
}

fn ensure_parent_dir_exists(path: &std::path::PathBuf) {
    let mut dir_path = path.clone();
    dir_path.pop();
    std::fs::create_dir_all(&dir_path).unwrap();
}

struct DataStore {
    info_vec: Vec<Info>,
    key_to_sha_map: HashMap<String, String>,
    key_to_mount: HashMap<String, api::types::Mount>,
    sha_fetched: HashSet<String>,
    // TODO: implement this for perisstent store
    _db_conn: tokio_rusqlite::Connection,
}

impl DataStore {
    async fn new(db_path: &str) -> Self {
        DataStore {
            info_vec: Vec::new(),
            key_to_sha_map: HashMap::new(),
            key_to_mount: HashMap::new(),
            sha_fetched: HashSet::new(),
            _db_conn: tokio_rusqlite::Connection::open(db_path).await.unwrap(),
        }
    }
}

struct SkySnapshotter {
    client: reqwest::Client,
    data_store: Arc<Mutex<DataStore>>,
    snapshot_dir: String,
    // multi_pbar: Arc<indicatif::MultiProgress>,
    // sty: indicatif::ProgressStyle,
    fallocate_lock: Arc<std::sync::Mutex<()>>,
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
            // multi_pbar: Arc::new(indicatif::MultiProgress::new()),
            // sty: (indicatif::ProgressStyle::with_template("{spinner:.green} ({msg}) [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes:>12}/{total_bytes:>12} ({bytes_per_sec:>15}, {eta:>5})")
            // .unwrap()
            // .progress_chars("#>-")),
            fallocate_lock: Arc::new(std::sync::Mutex::new(())),
        }
    }

    async fn fetch_image_manifest(&self, manifest_url: &String) -> ImageManifest {
        let manifest_text = self
            .client
            .get(manifest_url)
            .header("Accept", "application/vnd.oci.image.manifest.v1+json")
            .send()
            .await
            .expect("Failed to fetch image manifest")
            .text()
            .await
            .expect("Failed to parse manifest");

        // info!("Fetched manifest: {} from {}", manifest_text, manifest_url);

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
            let sha = url
                .split("/")
                .last()
                .unwrap()
                .to_string()
                .replace("sha256:", "");
            let fallocate_lock = self.fallocate_lock.clone();

            {
                if self.data_store.lock().await.sha_fetched.contains(&sha) {
                    info!("{} already fetched, skipping", sha);
                    continue;
                }
            }

            // let m = self.multi_pbar.clone();
            // let sty = self.sty.clone();

            tasks.spawn(async move {
                // remove `path` if the direcotry exists
                // safe to comment out beacuse we are starting with empty dir anyway.
                // let dir_path = std::path::Path::new(&path);
                // if dir_path.exists() {
                //     warn!("{} exists, removing", dir_path.display());
                //     std::fs::remove_dir_all(dir_path).unwrap();
                // }

                // create a directory for this sha regardless.
                let p = format!("/tmp/sky-snapshots/{}", sha.clone());
                let real_path = std::path::Path::new(p.as_str());
                std::fs::create_dir_all(real_path).unwrap();

                let start = minstant::Instant::now();

                let resp: reqwest::Response = client.get(&url).send().await.unwrap();
                let total_bytes = resp.content_length().unwrap();

                // let pbar = m.add(indicatif::ProgressBar::new(total_bytes));
                // pbar.set_style(sty);
                // pbar.set_message(url_clone.clone()[&url_clone.len() - 20..].to_owned());

                // TODO: use total_bytes to help infer buffer size to manage the relative buffer size and download throughput.
                let raw_to_decode_buff = async_ringbuf::AsyncHeapRb::<u8>::new(64 * 1024 * 1024);
                let (mut write_raw, read_raw) = raw_to_decode_buff.split();
                let decode_to_untar_buff = async_ringbuf::AsyncHeapRb::<u8>::new(64 * 1024 * 1024);
                let (mut write_decoded, mut read_decoded) = decode_to_untar_buff.split();

                let mut work_set = tokio::task::JoinSet::new();
                work_set.spawn(async move {
                    let bufreader = tokio::io::BufReader::with_capacity(64 * 1024, read_raw);
                    let mut reader = async_compression::tokio::bufread::ZstdDecoder::new(bufreader);
                    tokio::io::copy(&mut reader, &mut write_decoded)
                        .await
                        .unwrap();
                });
                let untar_thread = tokio::task::spawn_blocking(move || {
                    let read_decoded_sync = read_decoded.as_mut_base();
                    let mut tar =
                        tar::Archive::new(sync_io_utils::BlockingReader::new(read_decoded_sync));
                    // archive.unpack(path).unwrap();

                    // This has some lifetime issue... fix it later
                    // let decoder = Box::new(sync_io_utils::BlockingReader::new(read_decoded_sync));
                    // crate::tar_unpack::unpack_one_tar(decoder, path.clone(), fallocate_lock);

                    // let mut tar = tar::Archive::new(decoder);
                    let unpack_to = path.clone();
                    // mkdir
                    std::fs::create_dir_all(&unpack_to).unwrap();

                    let mut maybe_split_metadata: Option<SplitMetadata> = None;
                    for entry in tar.entries().unwrap() {
                        let mut entry = entry.expect(format!("Failed to read entry from tar {:?}", &url).as_str());
                        let path = entry.path().unwrap().display().to_string();

                        match entry.header().entry_type() {
                            tar::EntryType::Directory => {
                                // info!("Creating directory {}", &path);
                                let path = Path::new(&unpack_to).join(path.clone());
                                std::fs::create_dir_all(&path).unwrap();
                                continue;
                            }
                            tar::EntryType::Symlink => {
                                let target_path = entry.link_name().unwrap().unwrap().display().to_string();
                                let path = Path::new(&unpack_to).join(path.clone());

                                // symlink should point to an "abosolute" path as seen in the layer.
                                // this we are writing the target_path "as is".

                                // let target_path = Path::new(&unpack_to).join(target_path);
                                // info!("Creating symlink {:?} -> {:?}", &path, &target_path);
                                std::os::unix::fs::symlink(&target_path, &path).unwrap();
                                continue;
                            }
                            tar::EntryType::Link => {
                                let target_path = entry.link_name().unwrap().unwrap().display().to_string();
                                let path: std::path::PathBuf = Path::new(&unpack_to).join(path.clone());
                                let target_path = Path::new(&unpack_to).join(target_path);
                                // the target_path should already exist in the same archive.
                                assert!(target_path.is_file(), "{:?}", target_path.display());
                                // info!("Creating hard link {:?} -> {:?}", &path, &target_path);
                                std::fs::hard_link(&target_path, &path).unwrap();
                                continue;
                            }
                            _ => {}
                        }

                        if path.contains("split-metadata") {
                            // load the data
                            let mut buf = String::new();
                            entry.read_to_string(&mut buf).unwrap();
                            let split_metadata: SplitMetadata = serde_json::from_str(&buf).unwrap();

                            assert!(maybe_split_metadata.is_none());
                            maybe_split_metadata = Some(split_metadata.clone());

                            let path = Path::new(&unpack_to).join(split_metadata.path);
                            ensure_parent_dir_exists(&path);

                            {
                                let _lock = fallocate_lock.lock().unwrap();
                                if !path.is_file() {
                                    let f = OpenOptions::new()
                                        .read(true)
                                        .write(true)
                                        .create(true)
                                        .open(&path)
                                        .unwrap();
                                    vmm_sys_util::fallocate::fallocate(
                                        &f,
                                        vmm_sys_util::fallocate::FallocateMode::ZeroRange,
                                        false,
                                        0,
                                        split_metadata.total_size as u64,
                                    )
                                    .unwrap();
                                    info!("Fallocated file {:?}", &path);
                                } else {
                                    let current_size = std::fs::metadata(&path).unwrap().len();
                                    info!(
                                        "File {:?} (size {}) already exists, skip falllocate",
                                        &path, current_size
                                    );
                                }
                            }

                            continue;
                        }

                        // info!("Handling regular file {:?}", &path);

                        let path = Path::new(&unpack_to).join(path);
                        // create directory
                        ensure_parent_dir_exists(&path);
                        // create and write the file
                        match maybe_split_metadata {
                            Some(metadata) => {
                                assert!(path.is_file());
                                let mut file = OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .open(&path)
                                    .unwrap();
                                let offset = file
                                    .seek(SeekFrom::Start(metadata.start_offset as u64))
                                    .unwrap();
                                let num_bytes_written = std::io::copy(&mut entry, &mut file).unwrap();
                                let finished_offset = file.seek(SeekFrom::Current(0)).unwrap();
                                info!(
                                    "Writing {:?} (size {}) to {:?} (offset {}, physical_offset {}), {} written, finished_offset {}",
                                    &path,
                                    &entry.header().size().unwrap(),
                                    &path,
                                    &metadata.start_offset,
                                    &offset,
                                    &num_bytes_written,
                                    &finished_offset,
                                );
                                maybe_split_metadata = None;
                            }
                            None => {
                                assert!(entry.header().entry_type().is_file(), "entry {:?} is not a file, in {}", &path, &url);
                                // entry.unpack(&unpack_to).unwrap();
                                // entry.unpack_in(&path).unwrap();

                                let full_path = Path::new(&unpack_to.clone()).join(path);
                                let file_parent_dir = full_path.parent().unwrap();
                                std::fs::create_dir_all(&file_parent_dir).unwrap();


                                // TODO: set the actual mode
                                use std::os::unix::fs::OpenOptionsExt;
                                let mut file = std::fs::OpenOptions::new()
                                    .create(true)
                                    .write(true)
                                    .mode(777)
                                    .open(&full_path)
                                    .unwrap();
                                // info!(
                                //     "Writing {:?} (size {}) to {:?}",
                                //     &path,
                                //     &entry.header().size().unwrap(),
                                //     &path
                                // );
                                std::io::copy(&mut entry, &mut file).unwrap();
                            }
                        };
                    }




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
                    "Fetched {} in {:.3}s, {:.2}mb",
                    url_clone,
                    start.elapsed().as_millis() as f64 / 1e3,
                    total_bytes as f64 / 1e6
                );

                sha
            });
        }

        while let Some(res) = tasks.join_next().await {
            let sha = res.unwrap();
            self.data_store.lock().await.sha_fetched.insert(sha);
        }
    }
}

impl Debug for SkySnapshotter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkySnapshotter")
            .field("snapshot_dir", &self.snapshot_dir)
            .finish()
    }
}

impl SkySnapshotter {
    async fn prefetch_image(&self, image_ref: &str) {
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
            assert!(spec.media_type().to_string() == "application/vnd.oci.image.layer.v1.tar+zstd");
            let content_length = header
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<i64>()
                .unwrap();
            assert!(
                spec.size() == content_length,
                "size mismatch for {}: spec {}, header {}",
                url,
                spec.size(),
                content_length
            );
        }
        let mut blobs_url_to_path = blobs_url_to_layers
            .iter()
            .map(|(url, digest)| {
                (
                    url.clone(),
                    format!("{}/{}", self.snapshot_dir, digest.replace("sha256:", "")),
                )
            })
            .collect::<HashMap<String, String>>();

        for (digest, layer_descriptor) in layers {
            let url = format!("{}{}", image_ref_struct.blob_url_prefix, digest);

            if let Some(annotation) = layer_descriptor.annotations() {
                let shard_idx = annotation
                    .get("org.skycontainers.layer_shard_index")
                    .unwrap()
                    .parse::<u32>()
                    .unwrap();
                let shard_count = annotation
                    .get("org.skycontainers.layer_shard_total")
                    .unwrap()
                    .parse::<u32>()
                    .unwrap();

                let parent_sha = annotation
                    .get("org.skycontainers.layer_shard_parent_sha")
                    .map(|s| s.to_string());

                if shard_count > 1 && shard_idx != 0 {
                    let parent_sha = parent_sha.unwrap();
                    let parent_url = format!("{}{}", image_ref_struct.blob_url_prefix, parent_sha);
                    let parent_path = blobs_url_to_path.get(&parent_url).unwrap().clone();
                    blobs_url_to_path
                        .insert(url, parent_path)
                        .expect("we should be overwriting something here.");
                    info!("{} is a shard, parent is {}, updating its downloading directly to parent dir", digest, parent_sha);
                }
            }
        }

        self.parallel_fetch_to_file(blobs_url_to_path).await;
    }
}

fn clone_info_hack(info: &Info) -> Info {
    // a hack because the Info doesn't have copy trait
    serde_json::from_str(&serde_json::to_string(info).unwrap()).unwrap()
}

#[snapshots::tonic::async_trait]
impl snapshots::Snapshotter for SkySnapshotter {
    type Error = snapshots::tonic::Status;

    #[tracing::instrument(level = "info", skip(self, labels))]
    async fn prepare(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("Prepare: key={}, parent={}", key, parent);
        // info!(
        //     "Prepare: key={}, parent={}, label={:?}",
        //     key, parent, labels
        // );

        if !labels.contains_key("containerd.io/snapshot.ref") {
            // We are actually preparing a working container, not a snapshot.

            info!(
                "Preparing a working container, not a snapshot. key={}, parent={}",
                key, parent
            );

            let mut mount_vec = Vec::new();
            {
                let mut store = self.data_store.lock().await;
                let mut keys = vec![key.clone()];

                let mut parent = parent;
                while !parent.is_empty() {
                    let parent_info = store
                        .info_vec
                        .iter()
                        .find(|info| info.name == parent)
                        .unwrap();
                    keys.push(parent_info.name.clone());
                    parent = parent_info.parent.clone();
                }
                info!("Preparing an overlay fs for keys: {:?}", keys);

                let lower_dir_keys = &keys[1..];
                assert!(!lower_dir_keys.is_empty());
                let mut lower_dirs = lower_dir_keys
                    .iter()
                    .map(|key| {
                        let sha = store.key_to_sha_map.get(key).unwrap_or_else(|| panic!("can't find the corresponding sha for key={}, this shouldn't happen.",
                            key));
                        let dir = format!("{}/{}", self.snapshot_dir, sha);
                        assert!(std::path::Path::new(&dir).is_dir());
                        dir
                    }).filter(
                        |dir| std::path::Path::new(dir).is_dir() && std::fs::read_dir(dir).unwrap().count() > 0
                    )
                    .collect::<Vec<String>>();
                lower_dirs.reverse();

                let overylay_dir =
                    Path::new("/tmp/sky-snapshots/overlay").join(key.replace('/', "-"));
                if !overylay_dir.is_dir() {
                    std::fs::create_dir_all(&overylay_dir).unwrap();
                }
                let upper_dir = overylay_dir.join("fs");
                let work_dir = overylay_dir.join("work");
                std::fs::create_dir(&upper_dir).unwrap();
                std::fs::create_dir(&work_dir).unwrap();

                let mut options: Vec<String> = vec![];
                options.push("index=off".to_string());
                options.push("userxattr".to_string());
                options.push(format!("upperdir={}", upper_dir.to_str().unwrap()));
                options.push(format!("workdir={}", work_dir.to_str().unwrap()));
                options.push(format!("lowerdir={}", lower_dirs.join(":")));

                let mount = api::types::Mount {
                    r#type: "overlay".to_string(),
                    source: "overlay".to_string(),
                    options,
                    ..Default::default()
                };
                info!(
                    "Sending mount array: `mount -t {} {} -o{}`",
                    mount.r#type,
                    mount.source,
                    mount.options.join(","),
                );
                mount_vec.push(mount.clone());

                store.info_vec.push(Info {
                    name: key.clone(),
                    parent,
                    kind: Kind::Active,
                    ..Default::default()
                });
                store.key_to_mount.insert(key.clone(), mount);
            }
            return Ok(mount_vec);
        }

        let image_ref = labels.get("containerd.io/snapshot/cri.image-ref").unwrap();
        let layer_ref = labels
            .get("containerd.io/snapshot/cri.layer-digest")
            .unwrap()
            .replace("sha256:", "");

        let mut layer_exists = false;
        {
            let store = self.data_store.lock().await;
            if store.sha_fetched.contains(layer_ref.as_str()) {
                info!("{} already fetched, skipping", layer_ref);
                layer_exists = true;
            }
        }

        if !layer_exists {
            self.prefetch_image(image_ref).await;
        }

        {
            let mut store = self.data_store.lock().await;
            assert!(store.sha_fetched.contains(layer_ref.as_str()));
            store.key_to_sha_map.insert(key.clone(), layer_ref.clone());
            store.info_vec.push(Info {
                kind: Kind::Committed,
                name: key.clone(),
                parent,
                labels: labels.clone(),
                created_at: SystemTime::now(),
                updated_at: SystemTime::now(),
            });
        }

        Err(snapshots::tonic::Status::already_exists("already exists"))
    }

    type InfoStream = list_request_stream::WalkStream;
    #[tracing::instrument(level = "info")]
    async fn list(
        &self,
        snapshotter: String,
        filters: Vec<String>,
    ) -> Result<Self::InfoStream, Self::Error> {
        let mut stream = list_request_stream::WalkStream::new();
        {
            stream.infos.extend(
                self.data_store
                    .lock()
                    .await
                    .info_vec
                    .iter()
                    .filter(|info| {
                        for filters_ in filters.clone() {
                            for filter in filters_.as_str().split(",") {
                                let [key, value] = filter.split("==").collect::<Vec<&str>>()[..] else {
                                    panic!("Invalid filter {}", filter);
                                };
                                if key == "parent" {
                                    if info.parent != value {
                                        return false;
                                    }
                                } else if key.starts_with("labels") {
                                    let label_key = key.replace("labels.", "").replace('"', "");
                                    if info.labels.get(&label_key) != Some(&value.to_string()) {
                                        return false;
                                    }
                                } else {
                                    panic!("Unknown filter {}", key);
                                }
                                return true;
                            }
                        }
                        true
                    })
                    .map(clone_info_hack),
            );
        }
        info!(
            "List: filters {:?}, returning {} entries",
            &filters,
            stream.infos.len()
        );

        Ok(stream)
    }

    #[tracing::instrument(level = "info")]
    async fn stat(&self, key: String) -> Result<Info, Self::Error> {
        let start = minstant::Instant::now();
        let resp = self
            .data_store
            .lock()
            .await
            .info_vec
            .iter()
            .find(|info| info.name == key)
            .map(clone_info_hack)
            .ok_or(snapshots::tonic::Status::not_found(
                "Not found from skysnaphotter",
            ));
        info!("Stat: {}, duration: {:?}", key, start.elapsed());
        resp
    }

    async fn update(
        &self,
        info: Info,
        fieldpaths: Option<Vec<String>>,
    ) -> Result<Info, Self::Error> {
        todo!();
        // info!("Update: info={:?}, fieldpaths={:?}", info, fieldpaths);
        // Ok(Info::default())
    }

    async fn usage(&self, key: String) -> Result<Usage, Self::Error> {
        todo!();
        // info!("Usage: {}", key);
        // Ok(Usage::default())
    }

    #[tracing::instrument(level = "info")]
    async fn mounts(&self, key: String) -> Result<Vec<api::types::Mount>, Self::Error> {
        info!("Mounts: {}", key);
        {
            let store = self.data_store.lock().await;
            if let Some(mount) = store.key_to_mount.get(&key) {
                return Ok(vec![mount.clone()]);
            } else {
                return Err(snapshots::tonic::Status::not_found(
                    "Not found from skysnaphotter",
                ));
            }
        }
    }

    async fn view(
        &self,
        key: String,
        parent: String,
        labels: HashMap<String, String>,
    ) -> Result<Vec<api::types::Mount>, Self::Error> {
        todo!();
        // info!("View: key={}, parent={}, labels={:?}", key, parent, labels);
        // Ok(Vec::new())
    }

    async fn commit(
        &self,
        name: String,
        key: String,
        labels: HashMap<String, String>,
    ) -> Result<(), Self::Error> {
        todo!();
        // info!("Commit: name={}, key={}, labels={:?}", name, key, labels);
        // Ok(())
    }

    async fn remove(&self, key: String) -> Result<(), Self::Error> {
        todo!();
        // info!("Remove: {}", key);
        // Ok(())
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
