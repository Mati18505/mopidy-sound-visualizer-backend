use std::{convert::Infallible, sync::Arc};

use axum::{
    Router, extract::State, response::sse::{Event, KeepAlive, Sse}, routing::get
};
use gstreamer::prelude::*;
use gstreamer::{self as gst};
use tokio_stream::{StreamExt, wrappers::WatchStream};

fn stream(
    sender: tokio::sync::watch::Sender<Vec<f32>>,
    cancel_token: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        gst::init().expect("initialization failed");

        let pipeline_str = "
            udpsrc port=5001 caps=\"audio/x-raw,rate=44100,channels=2,format=S16LE\" !
            queue ! audioconvert ! audioresample !
            spectrum name=spec bands=64 ! fakesink
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
                gst::ClockTime::from_mseconds(100),
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
                        let _ = sender.send(magnitudes);
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
    rx: tokio::sync::watch::Receiver<Vec<f32>>,
}

async fn sse_handler(State(state): State<Arc<AppState>>) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    // A `Stream` that repeats an event every second
    let stream = tokio_stream::wrappers::WatchStream::new(state.rx.clone())
        .map(|data| {
            let text = format!("{:?}", data);
            Ok(Event::default().data(text))
        });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn serve_web(shared_state: Arc<AppState>, cancel_token: tokio_util::sync::CancellationToken) -> tokio::task::JoinHandle<()> {
    let app = Router::<Arc<AppState>>::new().route("/sse", get(sse_handler)).with_state(shared_state);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

        tokio::select! {
            _ = axum::serve(listener, app) => (),
            _ = cancel_token.cancelled() => (),
        }
    })
}

#[tokio::main]
async fn main() {
    let (tx, mut rx) = tokio::sync::watch::channel(Vec::<f32>::new());
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let stream_handle = stream(tx, cancel_token.clone());

    let shared_state = Arc::new(AppState{rx: rx.clone()});
    let web_server_handle = serve_web(shared_state, cancel_token.clone());

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                cancel_token.cancel();
                break
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                println!("Odebrane pasma FFT: {:?}", rx.borrow_and_update());
            },
        }
    }

    stream_handle.await.unwrap();
    web_server_handle.await.unwrap();
}
