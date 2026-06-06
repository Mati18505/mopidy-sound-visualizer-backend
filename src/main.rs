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
use gstreamer::prelude::*;
use gstreamer::{self as gst};
use lerp::Lerp;
use serde::Serialize;
use serde_json::json;
use tokio_stream::StreamExt;

mod ffi_spectrum;
mod log_partition;

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

        let pipeline_str = 
            format!("
            udpsrc port={GST_PORT} caps=\"audio/x-raw,rate={SAMPLING_RATE},channels=1,format=S16LE\" !
            queue !
            spectrum name=spec interval=500000 bands=4096 threshold=-{DB_THRESHOLD} ! fakesink
            ")
        ;

        let pipeline = gst::parse::launch(pipeline_str.as_str()).expect("pipeline creation failed");

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
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

        tokio::select! {
            _ = axum::serve(listener, app) => (),
            _ = cancel_token.cancelled() => (),
        }
    })
}

fn transform_data(
    mut gst_in: tokio::sync::mpsc::Receiver<Vec<f32>>,
    str_out: tokio::sync::broadcast::Sender<Arc<Vec<f32>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(data) = gst_in.recv().await {
            TRANSFORMATIONS.fetch_add(1, Ordering::Relaxed);
            let data: Vec<f32> = data.iter().map(|amp| *amp + DB_THRESHOLD as f32).collect();
            let data: Vec<Option<f32>> = transform_ffi_to_log_scale(data);
            let data = interpolate_empty(data);
            let _ = str_out.send(Arc::new(data));
        }
    })
}

fn none_to_zero(data: Vec<Option<f32>>) -> Vec<f32> {
    data.iter().map(|band| band.unwrap_or_default()).collect()
}

// For each None band search for its nearest Some bands into left and right.
// Interpolate with them.
// i = x, band.val = y
fn interpolate_empty(data: Vec<Option<f32>>) -> Vec<f32> {
    let mut nearest_left: Option<(usize, f32)> = None;

    data.iter()
        .enumerate()
        .map(|(i, band)| match band {
            Some(band) => {
                nearest_left = Some((i, *band));
                *band
            }
            None => {
                let nearest_right: Option<(usize, f32)> = data[i + 1..data.len()]
                    .iter()
                    .position(|e| e.is_some())
                    .map(|idx| (i + 1 + idx, data.get(i + 1 + idx).unwrap().unwrap()));

                match (nearest_left, nearest_right) {
                    (None, None) => 0.0,
                    (None, Some(rv)) => rv.1,
                    (Some(lv), None) => lv.1,
                    (Some(lv), Some(rv)) => {
                        let li = lv.0 as f32;
                        let ri = rv.0 as f32;
                        let i = i as f32;

                        let t = (i - li) / (ri - li);

                        debug_assert!((0.0..=1.0).contains(&t));
                        lv.1.lerp(rv.1, t)
                    }
                }
            }
        })
        .collect()
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

    fn get_avg(&self) -> Option<f32> {
        if self.ffi_bands_count > 0 {
            Some(self.sum_ffi_amplitudes / self.ffi_bands_count as f32)
        } else {
            None
        }
    }
}

// Use two pointers technique to optimize to O(ffi_bands).
// This assumes that ffi_band_freq(n+1) > ffi_band_freq(n)
fn transform_ffi_to_log_scale(ffi_bands: Vec<f32>) -> Vec<Option<f32>> {
    let ffi_bands_count: usize = ffi_bands.len();
    let log_bands_count: usize = 100;

    let bw = ffi_spectrum::band_width(SAMPLING_RATE, ffi_bands_count as u32);
    let log_freq_ranges = log_partition::create_freq_tuples(log_bands_count as u32);

    let mut log_band_aggregators: Vec<LogBandAggregator> =
        vec![Default::default(); log_freq_ranges.len()];
    let mut ffi_idx: usize = 0;
    let mut log_idx: usize = 0;

    while ffi_idx < ffi_bands_count && log_idx < log_bands_count {
        let ffi_band_freq = ffi_spectrum::get_freq_for_band_n(bw, ffi_idx as u32);
        let curr_log_freq_range = log_freq_ranges[log_idx];

        if log_partition::freq_in_range(curr_log_freq_range, ffi_band_freq) {
            log_band_aggregators[log_idx].add_ffi_band(ffi_bands[ffi_idx]);
            ffi_idx += 1;
        } else if ffi_band_freq < curr_log_freq_range.0 {
            ffi_idx += 1;
        } else {
            log_idx += 1;
        }
    }

    log_band_aggregators
        .iter()
        .map(|log_band_aggregator| log_band_aggregator.get_avg())
        .collect()
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
