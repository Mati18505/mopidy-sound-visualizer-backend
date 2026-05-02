use gstreamer::ffi::GstValueList;
use gstreamer::{self as gst, MessageType};
use gstreamer::prelude::*;

fn main() {
    // 1. Inicjalizacja GStreamera
    gst::init().expect("Nie udało się zainicjować GStreamera");

    // 2. Budowa potoku (pipeline) z wtyczką spectrum
    let pipeline_str = "
        udpsrc port=5001 caps=\"audio/x-raw,rate=44100,channels=2,format=S16LE\" !
        queue ! audioconvert ! audioresample !
        spectrum name=spec bands=64 ! fakesink
    ";

    let pipeline = gst::parse::launch(pipeline_str)
        .expect("Błąd podczas tworzenia potoku")
        .dynamic_cast::<gst::Pipeline>()
        .expect("To nie jest prawidłowy potok");

    // Uruchamiamy odtwarzanie potoku
    pipeline
        .set_state(gst::State::Playing)
        .expect("Nie udało się uruchomić potoku");

    println!("GStreamer działa i nasłuchuje...");

    // Wait until error or EOS
    let bus = pipeline.bus().unwrap();
    for msg in bus.iter_timed_filtered(
        gst::ClockTime::NONE,
        &[MessageType::Error, MessageType::Eos, MessageType::Element],
    ) {
        use gst::MessageView;

        match msg.view() {
            MessageView::Element(element_msg) => {
                // Sprawdzamy, czy to wiadomość od naszej wtyczki 'spectrum'
                if let Some(s) = element_msg.structure() {
                    if s.name() == "spectrum" {
                        // Wyciągamy tablicę wartości w decybelach (magnitude)
                        if let Ok(magnitudes) = s.get::<gst::List>("magnitude") {
                            // 'values' to wektor 64 liczb typu float
                            // TODO: Przekaż te dane do kanału (np. tokio::sync::broadcast)
                            // aby Axum wysłał je przez SSE do przeglądarek.
                            println!("Odebrano pasma FFT: {:?}", magnitudes);
                        }
                    }
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

    println!("GStreamer kończy działanie");

    // Zatrzymujemy potok przed zamknięciem programu
    pipeline
        .set_state(gst::State::Null)
        .expect("Nie udało się zatrzymać potoku");
}
