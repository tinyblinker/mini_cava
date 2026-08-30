use std::{env, f32::consts::PI, fs::File, path::Path};

use symphonia::core::{
    codecs::audio::{AudioDecoder, AudioDecoderOptions}, dsp::{complex::Complex32, fft}, formats::{FormatOptions, FormatReader, Track, TrackType, probe::Hint}, io::MediaSourceStream, meta::MetadataOptions,
};

struct AudioInitData {
    track: Track,
    decoder: Box<dyn AudioDecoder>,
    format: Box<dyn FormatReader>,
}

#[derive(Clone, Copy, Debug)]
struct Complex {
    re: f32,
    im: f32,
}

impl Complex {
    fn new(re: f32, im: f32) -> Self {
        Self { re: re, im: im }
    }
    fn add(self, other: Complex) -> Self {
        Complex::new(self.re + other.re, self.im + other.im)
    }
    fn sub(self, other: Complex) -> Self {
        Complex::new(self.re - other.re, self.im - other.im)
    }
    fn mul(self, other: Complex) -> Self {
        Complex::new(
            self.re * other.re - self.im * other.im,
            self.re * other.im + self.im * other.re,
        )
    }
    fn norm(self) -> f32 {
        (self.re * self.re + self.im * self.im).sqrt()
    }
}

// 迭代基-2 Cooley-Tukey FFT(原地计算,未归一化)
fn fft(input: &mut [Complex]) {
    let n = input.len();
    assert!(n.is_power_of_two(), "N必须是2的幂");

    // 1.位反转置换(bit-reversal permutation)
    let mut j = 0usize;
    for i in 1..n {
        // 对[Complex]中每一个Complex元素做判断然后位置换
        let mut bit = n >> 1;
        while j & bit != 0 {
            j ^= bit;
            bit >>= 1;
        }
        j ^= bit;
        if i < j {
            input.swap(i, j);
        }
    }

    // 2.蝶形运算(butterfly),子长度从2逐级翻倍
    let mut len = 2usize;
    while len <= n {
        let angle = -2.0 * PI / (len as f32);
        let w_len = Complex::new(angle.cos(), angle.sin());
        for i in (0..n).step_by(len) {
            let mut w = Complex::new(1.0, 0.0);
            for k in 0..len / 2 {
                let u = input[i + k];
                let v = input[i + k + len / 2].mul(w);
                input[i + k] = u.add(v);
                input[i + k + len / 2] = u.sub(v);
                w = w.mul(w_len);
            }
        }
        len <<= 1;
    }
}

fn compute_spectrum(frame: &[f32], window: &[f32]) -> Vec<f32> {
    let n = frame.len();
    assert_eq!(frame.len(), window.len());
    assert!(n.is_power_of_two(), "N 必须是 2 的幂");

    // 1. 加窗，构造复数数组（实部为样本，虚部为 0）
    let mut buffer: Vec<Complex> = frame
        .iter()
        .zip(window.iter())
        .map(|(&x, &w)| Complex::new(x * w, 0.0))
        .collect();

    // 2. 执行手写 FFT
    fft(&mut buffer);

    // 3. 求幅度谱，只取 0..=n/2（实信号频谱共轭对称）
    buffer[..=n / 2].iter().map(|c| c.norm()).collect()
}

fn detect_audio_format(args: &Vec<String>) -> Box<dyn FormatReader> {
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

    // probe the media source stream for a format
    let format = symphonia::default::get_probe()
        .probe(&hint, mss, fmt_opts, meta_opts)
        .unwrap();

    // return format
    format
}

fn get_the_track(format: &mut Box<dyn FormatReader>) -> Track {
    // get the default audio track
    let track = format.default_track(TrackType::Audio).unwrap();
    track.clone()
}

fn create_the_decoder_for_track(track: &Track) -> Box<dyn AudioDecoder> {
    let dec_opts: AudioDecoderOptions = Default::default();
    // create a decoder for the track
    let decoder = symphonia::default::get_codecs()
        .make_audio_decoder(
            track.codec_params.as_ref().unwrap().audio().unwrap(),
            &dec_opts,
        )
        .unwrap();
    decoder
}

fn decode_the_track(
    track: &Track,
    decoder: &mut Box<dyn AudioDecoder>,
    format: &mut Box<dyn FormatReader>,
) -> () {
    // store the track identifier, we'll use it to filter packages
    let track_id = track.id;

    // some variables about samples
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
                // sleep(Duration::from_secs_f32(1.0));
                // println!("The samples(Vec<f32>) = {:?}", samples);

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
}

fn init_the_decoder_and_track(args: &Vec<String>) -> AudioInitData {
    // Input the reference of args,
    // return an Boxed Object which impl "FormatReader Trait"
    // (Moved the ownership)
    let mut format = detect_audio_format(&args);

    // get Track based on the format (Moved the ownership)
    let track: Track = get_the_track(&mut format);

    // get the decoder based on the track (Moved the ownership)
    let decoder = create_the_decoder_for_track(&track);

    AudioInitData {
        track: track,
        decoder: decoder,
        format: format,
    }
}

fn main() {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();

    // Init the decoder and decoder the corresponding audio file
    let mut audio_init_data = init_the_decoder_and_track(&args);
    decode_the_track(
        &audio_init_data.track,
        &mut audio_init_data.decoder,
        &mut audio_init_data.format,
    );
}
