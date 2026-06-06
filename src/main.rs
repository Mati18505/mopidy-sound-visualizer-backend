use std::{
    convert::Infallible,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::SystemTime,
};

use axum::{
    Router,
    extract::State,
    response::{Sse, sse},
    routing::get,
};
use build_time::build_time_local;
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

fn stream(
    sender: tokio::sync::mpsc::Sender<Vec<f32>>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        gst::init().expect("initialization failed");

        let pipeline_str = format!("
            udpsrc port={GST_PORT} caps=\"audio/x-raw,rate={SAMPLING_RATE},channels=1,format=S16LE\" !
            queue !
            spectrum name=spec interval=500000 bands=4096 threshold=-{DB_THRESHOLD} ! fakesink
            ");

        let pipeline = gst::parse::launch(pipeline_str.as_str()).expect("pipeline creation failed");

        pipeline
            .set_state(gst::State::Playing)
            .expect("unable to set the pipeline to the `Playing` state");

        let bus = pipeline.bus().unwrap();

        info!(port = GST_PORT, "gstreamer started listening");

        loop {
            if cancel_token.is_cancelled() {
                break;
            }

            let msg = bus.timed_pop_filtered(
                gst::ClockTime::from_mseconds(10),
                &[
                    gst::MessageType::Error,
                    gst::MessageType::Eos,
                    gst::MessageType::Element,
                ],
            );

            let Some(msg) = msg else {
                continue;
            };

            use gst::MessageView;

            match msg.view() {
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
                        let _ = sender.blocking_send(magnitudes);

                        GST.fetch_add(1, Ordering::Relaxed);
                    }
                }
                MessageView::Eos(..) => break,
                MessageView::Error(err) => {
                    error!(
                        "Error from {:?}: {} ({:?})",
                        err.src().map(|s| s.path_string()),
                        err.error(),
                        err.debug()
                    );
                    break;
                }
                _ => (),
            }
        }

        pipeline
            .set_state(gst::State::Null)
            .expect("Unable to set the pipeline to the `Null` state");

        info!(port = GST_PORT, "gstreamer finished listening");
    })
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

fn serve_web(
    shared_state: Arc<AppState>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let app = Router::<Arc<AppState>>::new()
        .route("/sse", get(sse_handler))
        .layer(tower_http::cors::CorsLayer::permissive())
        .with_state(shared_state);

    tokio::spawn(async move {
        let addr = "0.0.0.0:3000";
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

        info!(addr = addr, "sse started listening");

        tokio::select! {
            _ = axum::serve(listener, app) => (),
            _ = cancel_token.cancelled() => (),
        }
        info!(addr = addr, "sse finished listening");
    })
}

fn transform_data(
    mut gst_in: tokio::sync::mpsc::Receiver<Vec<f32>>,
    str_out: tokio::sync::broadcast::Sender<Arc<Vec<f32>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(bands) = gst_in.recv().await {
            TRANSFORMATIONS.fetch_add(1, Ordering::Relaxed);

            let config = bands_transformation::Config {
                db_threshold: -(DB_THRESHOLD as f32),
                sampling_rate: SAMPLING_RATE,
            };
            let bands: Vec<f32> = bands_transformation::transform_bands(bands, config);

            let _ = str_out.send(Arc::new(bands));
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

async fn run() {
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let (gst_in, gst_out) = tokio::sync::mpsc::channel(1);
    let (web_in, web_out) = tokio::sync::broadcast::channel(1);

    let stream_handle = stream(gst_in, cancel_token.clone());
    let transformer = transform_data(gst_out, web_in);

    let shared_state = Arc::new(AppState { rx: web_out });
    let web_server_handle = serve_web(shared_state, cancel_token.clone());

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

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
        }
    }

    stream_handle.await.unwrap();
    transformer.await.unwrap();
    web_server_handle.await.unwrap();
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

#[tokio::main]
async fn main() {
    let _logging_guard = init_logging();
    let build_info = BuildInfo::new();
    let span_build_info = info_span!("build_info", build_info = build_info.as_value());
    let _enter_build_info = span_build_info.enter();

    run().await;
}
