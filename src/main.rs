use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    extract::State,
    response::{Sse, sse},
    routing::get,
};
use gstreamer::prelude::*;
use gstreamer::{self as gst};
use serde_json::json;
use tokio_stream::StreamExt;

mod log_partition;
mod ffi_spectrum;

static GST: AtomicU64 = AtomicU64::new(0);
static TRANSFORMATIONS: AtomicU64 = AtomicU64::new(0);
static SSE: AtomicU64 = AtomicU64::new(0);

fn stream(
    sender: tokio::sync::mpsc::Sender<Vec<f32>>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        gst::init().expect("initialization failed");

        let pipeline_str = "
            udpsrc port=5555 caps=\"audio/x-raw,rate=44100,channels=2,format=S16LE\" !
            queue ! audioconvert ! audioresample !
            spectrum name=spec interval=10000000 bands=4096 ! fakesink
        ";

        let pipeline = gst::parse::launch(pipeline_str).expect("pipeline creation failed");

        pipeline
            .set_state(gst::State::Playing)
            .expect("unable to set the pipeline to the `Playing` state");

        let bus = pipeline.bus().unwrap();

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
                    println!(
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
    })
}

struct AppState {
    rx: tokio::sync::broadcast::Receiver<Arc<String>>,
}

async fn sse_handler(
    State(state): State<Arc<AppState>>,
) -> Sse<impl futures_util::Stream<Item = Result<sse::Event, Infallible>>> {
let stream = tokio_stream::wrappers::BroadcastStream::new(state.rx.resubscribe())
    .filter_map(|data| {
        let text = data.ok()?;

        SSE.fetch_add(1, Ordering::Relaxed);

        Some(Ok::<_, Infallible>(
            sse::Event::default().data(text.to_string()),
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
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

        tokio::select! {
            _ = axum::serve(listener, app) => (),
            _ = cancel_token.cancelled() => (),
        }
    })
}

fn transform_data(mut gst_in: tokio::sync::mpsc::Receiver<Vec<f32>>, str_out: tokio::sync::broadcast::Sender<Arc<String>>) -> tokio::task::JoinHandle<()>  {
    tokio::spawn(async move {
        while let Some(data) = gst_in.recv().await {
            TRANSFORMATIONS.fetch_add(1, Ordering::Relaxed);
            let data: Vec<f32> = data.iter().map(|amp| *amp + 60.0).collect();
            let data: Vec<f32> = transform_ffi_to_log_scale(data);
            let text = json!(data).to_string();
            let _ = str_out.send(Arc::new(text));
        };
    })
}

#[derive(Default, Clone, Copy)]
struct LogBandAggregator {
    sum_ffi_amplitudes: f32,
    ffi_bands_count: u32,
}

impl LogBandAggregator {
    fn add_ffi_band(&mut self, amplitude: f32) {
        self.sum_ffi_amplitudes += amplitude;
        self.ffi_bands_count += 1;
    }

    fn get_avg(&self) -> f32 {
        self.sum_ffi_amplitudes / self.ffi_bands_count as f32
    }
}

fn transform_ffi_to_log_scale(ffi_bands: Vec<f32>) -> Vec<f32> {
    let sampling_rate: u32 = 44100;
    let bands_count: u32 = ffi_bands.len() as u32;
    let log_partition_bands: u32 = 100;

    let bw = ffi_spectrum::band_width(sampling_rate, bands_count);
    let log_freq_ranges = log_partition::create_freq_tuples(log_partition_bands);
    let mut log_band_aggregators: Vec<LogBandAggregator> = vec![Default::default(); log_freq_ranges.len()];

    for (ffi_band_n, ffi_band_amplitude) in ffi_bands.iter().enumerate() {
        let ffi_band_freq =
            ffi_spectrum::get_freq_for_band_n(bw, ffi_band_n as u32);
        let log_band_index = log_partition::get_tuple_index_containing_freq(
            &log_freq_ranges,
            ffi_band_freq,
        );
        //dbg!(&log_band_index, &ffi_band_n);

        if let Some(log_band_index) = log_band_index {
            log_band_aggregators[log_band_index].add_ffi_band(*ffi_band_amplitude);
        }
    }

    log_band_aggregators.iter().map(|log_band_aggregator| log_band_aggregator.get_avg()).collect()
}

#[tokio::main]
async fn main() {
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
                println!(
                    "gst: {}/s, transformations: {}/s, sse: {}/s",
                    GST.swap(0, Ordering::Relaxed),
                    TRANSFORMATIONS.swap(0, Ordering::Relaxed),
                    SSE.swap(0, Ordering::Relaxed),
                );
            },
        }
    }

    stream_handle.await.unwrap();
    transformer.await.unwrap();
    web_server_handle.await.unwrap();
}
