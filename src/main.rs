use std::{
    convert::Infallible,
    net::SocketAddrV4,
    ops::Deref,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
    time::SystemTime,
};

use anyhow::Context;
use axum::{
    Router,
    extract::State,
    response::{Sse, sse},
    routing::get,
};
use build_time::build_time_local;
use futures_util::pin_mut;
use gstreamer::prelude::*;
use gstreamer::{self as gst};
use serde::Serialize;
use serde_json::json;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, info_span, level_filters::LevelFilter, trace, warn};
use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};
use valuable::Valuable;

mod bands_transformation;

static GST: AtomicU64 = AtomicU64::new(0);
static TRANSFORMATIONS: AtomicU64 = AtomicU64::new(0);
static SSE: AtomicU64 = AtomicU64::new(0);
const SAMPLING_RATE: u32 = 96000;
const GST_PORT: u16 = 5556;
const DB_THRESHOLD: u8 = 100;
const EV_PER_SECOND: u64 = 100;

struct GstStream {
    pipeline: gst::Element,
    stream: gst::bus::BusStream,
}

impl GstStream {
    fn new() -> anyhow::Result<Self> {
        // It has once underneath, so initializing multiple times (when creating multiple instances of
        // GstStream) does nothing.
        gst::init().context("initialization failed")?;

        let pipeline_str = format!(
            "
            udpsrc port={GST_PORT} caps=\"audio/x-raw,rate={SAMPLING_RATE},channels=1,format=S16LE\" !
            queue !
            spectrum name=spec interval=500000 bands=4096 threshold=-{DB_THRESHOLD} ! fakesink
            "
        );

        let pipeline = gst::parse::launch(pipeline_str.as_str())
            .context(format!("pipeline creation failed {pipeline_str}"))?;

        pipeline.set_state(gst::State::Playing).context(format!(
            "cannot set the pipeline to the `Playing` state {pipeline_str}"
        ))?;

        let bus = pipeline
            .bus()
            .context(format!("cannot get pipeline bus {pipeline_str}"))?;
        let stream = bus.stream();

        info!(port = GST_PORT, "gstreamer started listening");

        Ok(Self { pipeline, stream })
    }
}

impl futures_util::Stream for GstStream {
    type Item = Vec<f32>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use gst::MessageView;

        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(msg)) => match msg.view() {
                MessageView::Element(element_msg) => {
                    if let Some(magnitudes) = element_msg
                        .structure()
                        .filter(|s| s.name() == "spectrum")
                        .and_then(|s| s.get::<gst::List>("magnitude").ok())
                    {
                        let magnitudes: Vec<f32> = magnitudes
                            .as_slice()
                            .iter()
                            .map(|v| v.get::<f32>().unwrap())
                            .collect();

                        GST.fetch_add(1, Ordering::Relaxed);
                        Poll::Ready(Some(magnitudes))
                    } else {
                        Poll::Pending
                    }
                }
                MessageView::Eos(..) => Poll::Ready(None),
                MessageView::Error(err) => {
                    error!(
                        "Error from {:?}: {} ({:?})",
                        err.src().map(|s| s.path_string()),
                        err.error(),
                        err.debug()
                    );
                    Poll::Ready(None)
                }
                _ => Poll::Pending,
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for GstStream {
    fn drop(&mut self) {
        self.pipeline
            .set_state(gst::State::Null)
            .expect("Unable to set the pipeline to the `Null` state");

        info!(port = GST_PORT, "gstreamer finished listening");
    }
}

struct AppState {
    rx: tokio::sync::broadcast::Receiver<Arc<Vec<f32>>>,
}

async fn sse_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl futures_util::Stream<Item = Result<sse::Event, Infallible>>> {
    let stream =
        tokio_stream::wrappers::BroadcastStream::new(state.rx.resubscribe()).filter_map(|data| {
            #[derive(Serialize)]
            struct SseMessage {
                data: Vec<f32>,
                timestamp: u128,
            }

            let data = data.ok()?.deref().clone();
            let message = SseMessage {
                data,
                timestamp: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            };

            SSE.fetch_add(1, Ordering::Relaxed);

            Some(Ok::<_, Infallible>(
                sse::Event::default().data(json!(message).to_string()),
            ))
        });

    Sse::new(stream).keep_alive(sse::KeepAlive::default())
}

async fn serve_web(
    shared_state: Arc<AppState>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let app = Router::<Arc<AppState>>::new()
        .route("/sse", get(sse_handler))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(shared_state);

    let addr = "0.0.0.0:3000";
    let socket_addr: SocketAddrV4 = addr
        .parse()
        .context(format!("cannot parse sse address {addr}"))?;
    let listener = tokio::net::TcpListener::bind(socket_addr)
        .await
        .context(format!("cannot bind socket {addr}"))?;

    Ok(tokio::spawn(async move {
        info!(addr = addr, "sse started listening");

        tokio::select! {
            _ = axum::serve(listener, app) => (),
            _ = cancel_token.cancelled() => (),
        }
        info!(addr = addr, "sse finished listening");
    }))
}

fn transform_data(
    mut gst_in: tokio::sync::watch::Receiver<Vec<f32>>,
    str_out: tokio::sync::broadcast::Sender<Arc<Vec<f32>>>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_millis(1000 / EV_PER_SECOND));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if gst_in.has_changed().unwrap() {
                        let bands = gst_in.borrow_and_update().clone();

                        let config = bands_transformation::Config {
                            db_threshold: -(DB_THRESHOLD as f32),
                            sampling_rate: SAMPLING_RATE,
                        };
                        let bands: Vec<f32> = bands_transformation::transform_bands(bands, config);

                        let _ = str_out.send(Arc::new(bands));
                        TRANSFORMATIONS.fetch_add(1, Ordering::Relaxed);
                    }
                },
                _ = cancel_token.cancelled() => break,
            }
        }
    })
}

fn init_logging() -> tracing_appender::non_blocking::WorkerGuard {
    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    let format = tracing_subscriber::fmt::format().compact();

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(filter)
        .event_format(format)
        .with_span_events(FmtSpan::ENTER)
        .init();

    guard
}

#[derive(valuable::Valuable)]
struct BuildInfo {
    git_sha: String,
    pkg_version: String,
    build_time: String,
}
impl BuildInfo {
    fn new() -> Self {
        BuildInfo {
            git_sha: std::env::var("GIT_SHA").unwrap_or("unknown".to_string()),
            pkg_version: env!("CARGO_PKG_VERSION").to_string(),
            build_time: build_time_local!("%Y-%m-%dT%H:%M:%S%:z").to_string(),
        }
    }
}

#[derive(valuable::Valuable)]
struct InstanceInfo {
    os_release: String,
    os_type: String,
}
impl InstanceInfo {
    fn new() -> Self {
        InstanceInfo {
            os_release: sys_info::os_release().unwrap_or("unknown".to_string()),
            os_type: sys_info::os_type().unwrap_or("unknown".to_string()),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _logging_guard = init_logging();

    info!(build_info = BuildInfo::new().as_value());
    info!(instance_info = InstanceInfo::new().as_value());

    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (gst_in, gst_out) = tokio::sync::watch::channel(vec![0.0]);
    let (web_in, web_out) = tokio::sync::broadcast::channel(1);

    let gst_stream = GstStream::new().context("cannot create gstreamer pipeline")?;
    let transformer = transform_data(gst_out, web_in, cancel_token.clone());

    let shared_state = Arc::new(AppState { rx: web_out });
    let web_server_handle = serve_web(shared_state, cancel_token.clone())
        .await
        .context("cannot create sse server")?;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

    pin_mut!(gst_stream);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                cancel_token.cancel();
                break
            },
            _ = interval.tick() => {
                debug!(
                    gst = GST.swap(0, Ordering::Relaxed),
                    transformations = TRANSFORMATIONS.swap(0, Ordering::Relaxed),
                    sse = SSE.swap(0, Ordering::Relaxed),
                );
            },
            received = gst_stream.next() => {
                match received {
                    Some(magnitudes) => {
                        gst_in.send_replace(magnitudes);
                    },
                    None => {
                        debug!("Gstreamer stream finished.");
                        cancel_token.cancel();
                        break
                    },
                }
            }
        }
    }

    transformer.await?;
    web_server_handle.await?;

    Ok(())
}
