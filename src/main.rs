use std::{thread, time::Duration};

use anyhow::anyhow;
use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    Device, Error, ErrorKind, FromSample, InputCallbackInfo, SampleFormat, SizedSample,
    StreamConfig,
};
use ringbuf::{
    traits::{Consumer, Observer, Producer, Split},
    HeapRb,
};
use rustfft::{num_complex::Complex, FftPlanner};

// 封装一个只在cargo run(debug)下执行的debug_print()函数
#[cfg(debug_assertions)]
macro_rules! debug_println {
    ($($args:tt)*) => {
        println!($($args)*);
    }
}
#[cfg(not(debug_assertions))]
macro_rules! debug_println {
    ($($args:tt)*) => {};
}

// 定义FFT_SIZE常数
const FFT_SIZE: usize = 1024;

fn clean_the_screen() -> () {
    print!("\x1B[2J\x1B[H");
}

fn err_fn(err: Error) {
    match err.kind() {
        ErrorKind::DeviceChanged | ErrorKind::RealtimeDenied => {
            eprintln!("{err}")
        }
        _ => eprintln!("Stream error: {err}"),
    }
}

fn display_one_bin(bin: &f32) -> () {
    println!("*****************");
}

fn display_fft_buffer(buffer: Vec<Complex<f32>>, config: StreamConfig) -> () {
    // fft_data(transferred to Vec<f32>)
    let fft_data: Vec<f32> = buffer
        .iter()
        .map(|fft_complex| fft_complex.norm())
        .collect();
    debug_println!("fft_data.len() = {:?}", fft_data.len());

    // 定义Vec<f32>(长度FFT_SIZE)中每一个数字是一个bin(视为cava显示中的一根柱子)
    // 每两个bin之间相隔的频率等于sample_rate / FFT_SIZE
    let freq_atom: f32 = config.sample_rate as f32 / FFT_SIZE as f32;
    debug_println!("freq_atom = {:?}", freq_atom);

    // 清屏并显示(超级草率地)
    clean_the_screen();
    let _ = fft_data.iter().map(|bin| display_one_bin(bin));
}

fn run<T>(input_device: &Device, config: StreamConfig) -> Result<(), anyhow::Error>
where
    T: SizedSample
        + std::fmt::Debug
        + Send
        + 'static
        + rustfft::num_traits::FromPrimitive
        + rustfft::num_traits::Signed
        + std::marker::Sync,
    f32: FromSample<T>,
{
    // Create RingBuffer for "input_data_fn" (producer), for the "RustFFT"(consumer)
    let ring_capacity = FFT_SIZE * 40;
    let ring = HeapRb::<T>::new(ring_capacity);
    let (mut producer, mut consumer) = ring.split();
    for _ in 0..(ring_capacity / 2) {
        producer.try_push(T::EQUILIBRIUM).unwrap();
    }

    // "move" 能让闭包捕获的外部变量拿走所有权,而不是继续借用他们
    let input_data_fn = move |data: &[T], _: &InputCallbackInfo| {
        let pushed_slice_cnt = producer.push_slice(data);
        debug_println!(
            "pushed_slice_cnt = {:?}, data.len() = {:?}.",
            pushed_slice_cnt,
            data.len()
        );
        if pushed_slice_cnt < data.len() {
            eprintln!("Producing fft input data too fast!!");
        }
    };

    // Build streams.
    debug_println!(
        "Attempting to build input stream with {} samples and `{config:?}`.",
        T::FORMAT
    );
    let input_stream = input_device.build_input_stream(config, input_data_fn, err_fn, None)?;
    debug_println!("Successfully built streams.");

    // play the stream(should be alive in this thread always!)
    input_stream.play()?;

    // RustFFT(PCM_data -> 频域数据<频谱>)
    let mut planner = FftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(FFT_SIZE);
    let channels = config.channels;
    let required_samples = FFT_SIZE * (channels as usize);

    loop {
        // Data is consumed too fast!!! should be wait here
        while consumer.occupied_len() < required_samples {
            thread::sleep(Duration::from_millis(1));
        }

        // vars about poped data
        let mut poped_data = vec![T::EQUILIBRIUM; required_samples];
        let poped_data_len = consumer.pop_slice(&mut poped_data);
        debug_println!("poped_data_len = {:?}", poped_data_len);

        // convert the data to satisfy the buffer
        let mut buffer: Vec<Complex<f32>> = poped_data
            .chunks_exact_mut(2)
            .map(|lr| {
                Complex::<f32>::new(
                    (lr[0].to_sample::<f32>() + lr[1].to_sample::<f32>()) / (channels as f32),
                    0.0,
                )
            })
            .collect();
        debug_println!("buffer[10](before) = {:?}", buffer[10]);
        fft.process(&mut buffer);
        debug_println!("buffer[10](after) = {:?}", buffer[10]);

        // display the fft buffer
        display_fft_buffer(buffer, config);
    }
}

fn main() -> Result<(), anyhow::Error> {
    // init Host and Device
    let host = cpal::host_from_id(cpal::HostId::PipeWire)?;
    let input_device = host
        .devices()?
        .find(|d| {
            d.description()
                .map(|des| des.name() == "default_sink")
                .unwrap_or(false)
        })
        .ok_or(anyhow!("Cannot find the \"default_sink\"!"))?;
    debug_println!("input device: {}", input_device);

    // init the config
    let input_config = input_device.default_input_config()?;
    debug_println!(
        "sample rate: {} Hz, channels: {}, format: {:?}",
        input_config.sample_rate(),
        input_config.channels(),
        input_config.sample_format()
    );

    // 接收_stream,防止stream被自动drop()
    let _stream = match input_config.sample_format() {
        // SampleFormat::I8 => run::<i8>(&input_device, input_config.into()),
        // SampleFormat::I16 => run::<i16>(&input_device, input_config.into()),
        // SampleFormat::I32 => run::<i32>(&input_device, input_config.into()),
        // SampleFormat::I64 => run::<i64>(&input_device, input_config.into()),
        // SampleFormat::U8 => run::<u8>(&input_device, input_config.into()),
        // SampleFormat::U16 => run::<u16>(&input_device, input_config.into()),
        // SampleFormat::U32 => run::<u32>(&input_device, input_config.into()),
        // SampleFormat::U64 => run::<u64>(&input_device, input_config.into()),
        SampleFormat::F32 => run::<f32>(&input_device, input_config.into()),
        // SampleFormat::F64 => run::<f64>(&input_device, input_config.into()),
        sample_format => panic!("Unsupported sample format '{sample_format}'"),
    }?;

    // leave the thread sleep
    loop {
        std::thread::park();
    }
}
