use std::{
    collections::HashMap,
    ops::DerefMut,
    pin::{self, Pin},
    sync::Arc,
    task::{Context, Poll},
    time::SystemTime,
};

use containerd_snapshots as snapshots;
use containerd_snapshots::{api, Info, Kind, Usage};
use log::{debug, info};
use snapshots::tonic::transport::Server;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_stream::Stream;

struct DataStore {
    info_vec: Vec<Info>,
}

struct SkySnapshotter {
    client: reqwest::Client,
    data_store: Arc<Mutex<DataStore>>,
}

impl SkySnapshotter {
    fn new() -> Self {
        SkySnapshotter {
            client: reqwest::Client::new(),
            data_store: Arc::new(Mutex::new(DataStore {
                info_vec: Vec::new(),
            })),
        }
    }

    async fn fetch_image_manifest(&self, image_ref: String) -> json::JsonValue {
        // TODO: handle more cases. This just handle forms like localhost:5000/image:latest case
        // docker tag is far more complex
        let parsed = regex::Regex::new(r"^(?P<host>[^/]+)/(?P<image>[^:]+):(?P<tag>.+)$")
            .unwrap()
            .captures(&image_ref)
            .unwrap();
        let host = parsed.name("host").unwrap().as_str();
        let image = parsed.name("image").unwrap().as_str();
        let tag = parsed.name("tag").unwrap().as_str();
        let manifest_url = format!("http://{}/v2/{}/manifests/{}", host, image, tag);

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
        let image_layers = labels
            .get("containerd.io/snapshot/cri.image-layers")
            .unwrap();
        let layer_digest = labels
            .get("containerd.io/snapshot/cri.layer-digest")
            .unwrap();

        info!("image_ref: {}", image_ref);
        info!("image_layers: {}", image_layers);
        info!("layer_digest: {}", layer_digest);

        debug!("fetching image manifest");
        let manifest = self.fetch_image_manifest(image_ref.to_string()).await;
        let layers = manifest["layers"]
            .members()
            .map(|layer| OCILayer {
                media_type: layer["mediaType"].to_string(),
                digest: layer["digest"].to_string(),
                size: layer["size"].as_u64().unwrap(),
            })
            .collect::<Vec<OCILayer>>();
        info!("{:?}", layers);

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
            stream
                .infos
                .extend(self.data_store.lock().await.info_vec.iter().map(|info| clone_info_hack(info)));
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
    use std::time::Duration;

    use tower_http::trace::{DefaultMakeSpan, DefaultOnRequest};
    use tracing::Span;

    tracing_subscriber::fmt::init();

    let socket_path = "/tmp/sky.sock";

    // remove socket_path if it exists
    if let Ok(_) = std::fs::metadata(socket_path) {
        std::fs::remove_file(socket_path).expect("Failed to remove socket");
    }

    let sky_snapshotter = SkySnapshotter::new();

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
