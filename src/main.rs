use std::{env, fs::File, path::Path, thread::sleep, time::Duration};

use symphonia::core::{
    codecs::audio::AudioDecoderOptions,
    formats::{probe::Hint, FormatOptions, TrackType},
    io::MediaSourceStream,
    meta::MetadataOptions,
};

// // use crate to implement this one
// fn audio_to_pcm(audio_path: &Path) -> () {
//     unimplemented!()
// }

// fn FT(pcm_data: i32) -> () {
//     unimplemented!()
// }

fn main() {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();

    // Create a media source.
    // "MediaSource" trait is automatically implemented for "File" among other type
    let file = Box::new((File::open(Path::new(&args[1]))).unwrap());

    // Create the media source stream using the boxed media source from above
    let mss = MediaSourceStream::new(file, Default::default());

    // create a hint to help the format registry guess what format reader is appropriate.
    // in this example we'll leave it empty
    let hint = Hint::new();

    // use the default options when reading and decoding
    let fmt_opts: FormatOptions = Default::default();
    let meta_opts: MetadataOptions = Default::default();
    let dec_opts: AudioDecoderOptions = Default::default();

    // probe the media source stream for a format
    let mut format = symphonia::default::get_probe()
        .probe(&hint, mss, fmt_opts, meta_opts)
        .unwrap();

    // get the default audio track
    let track = format.default_track(TrackType::Audio).unwrap();

    // create a decoder for the track
    let mut decoder = symphonia::default::get_codecs()
        .make_audio_decoder(
            track.codec_params.as_ref().unwrap().audio().unwrap(),
            &dec_opts,
        )
        .unwrap();

    // store the track identifier, we'll use it to filter packages
    let track_id = track.id;

    let mut samples: Vec<f32> = Default::default();
    let mut total_sample_count = 0;

    // read and decode all packets from the format reader
    while let Some(packet) = format.next_packet().unwrap() {
        // if the packet does not belong to the selected track, skip it
        if packet.track_id != track_id {
            continue;
        }

        // decode the packet into audio samples,ignoring any decode errors
        match decoder.decode(&packet) {
            Ok(audio_buf) => {
                // The decoded audio samples may now be accessed via the generic audio buffer
                // returned by the decoder. You may match on the buffer to access a sample-format
                // specific buffer, or use generic routines to copy out the audio samples in the
                // desired sample format.
                //
                // In the example below, we will copy the all the samples into a vector in
                // the f32 sample format in channel interleaved order.

                // ensure the vector is large enough to hold all the samples
                samples.resize(audio_buf.samples_interleaved(), 0f32);

                // copy the audio sample from the generic buffer to the vector in interleaved
                // order. The sample format to convert to is inferred from the type of the vec
                audio_buf.copy_to_slice_interleaved(&mut samples);

                // sleep for a while and show the samples' datas
                sleep(Duration::from_secs_f32(1.0));
                println!("The samples(Vec<f32>) = {:?}", samples);

                // Sum up the total number of samples
                total_sample_count += samples.len();
                println!("decoded {total_sample_count} samples");
            }
            Err(symphonia::core::errors::Error::DecodeError(_)) => {
                println!("Here we have a Decode error!!!");
            }
            Err(_) => {
                break;
            }
        }
    }

    // // transfer the audio file to pcm_data
    // let audio_path = Path::new("_");
    // let pcm_data = audio_to_pcm(audio_path);

    // do the "Fourier transform"
}
