use gstreamer::prelude::*;
use gstreamer::{self as gst};

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

#[tokio::main]
async fn main() {
    let (tx, mut rx) = tokio::sync::watch::channel(Vec::<f32>::new());
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let stream = stream(tx, cancel_token.clone());

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                cancel_token.cancel();
                break
            },
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                println!("Odebrane pasma FFT: {:?}", rx.borrow_and_update());
            }
        }
    }

    stream.await.unwrap();
}
