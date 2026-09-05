use std::ptr::with_exposed_provenance;

use anyhow::anyhow;
use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    Device, Error, ErrorKind, FromSample, InputCallbackInfo, SampleFormat, SizedSample, Stream,
    StreamConfig,
};
use ringbuf::{
    traits::{Consumer, Producer, Split},
    HeapRb,
};
use rustfft::{
    num_complex::{Complex, Complex32},
    FftPlanner,
};

const FFT_SIZE: usize = 1024;

fn err_fn(err: Error) {
    match err.kind() {
        ErrorKind::DeviceChanged | ErrorKind::RealtimeDenied => {
            eprintln!("{err}")
        }
        _ => eprintln!("Stream error: {err}"),
    }
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
    let ring_capacity = FFT_SIZE * 8;
    let ring = HeapRb::<T>::new(ring_capacity);
    let (mut producer, mut consumer) = ring.split();
    for _ in 0..(ring_capacity / 2) {
        producer.try_push(T::EQUILIBRIUM).unwrap();
    }

    // "move" 能让闭包捕获的外部变量拿走所有权,而不是继续借用他们
    let input_data_fn = move |data: &[T], _: &InputCallbackInfo| {
        if producer.push_slice(data) < data.len() {
            eprintln!("Producing fft input data too slowly!!");
        }
    };

    // Build streams.
    println!(
        "Attempting to build input stream with {} samples and `{config:?}`.",
        T::FORMAT
    );
    let input_stream = input_device.build_input_stream(config, input_data_fn, err_fn, None)?;
    println!("Successfully built streams.");

    // play the stream(should be alive in this thread always!)
    input_stream.play()?;

    // RustFFT
    let mut planner = FftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(FFT_SIZE);
    loop {
        let poped_data: &mut [T] = &mut [T::EQUILIBRIUM; FFT_SIZE];
        let poped_data_len = consumer.pop_slice(poped_data);

        // 此处有问题,poped_data_len 始终 = 0
        if poped_data_len < poped_data.len() {
            eprintln!("Consuming fft input data too slowly!!");
        }
        println!("poped_data_len = {:?}", poped_data_len);
        let mut buffer: Vec<Complex<f32>> = poped_data
            .iter()
            .map(|&x| Complex32::new(x.to_sample::<f32>(), 0.0))
            .collect();
        fft.process(&mut buffer);
        println!("buffer.len() = {:?}", buffer.len());
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
    println!("input device: {}", input_device);

    // init the config
    let input_config = input_device.default_input_config()?;
    println!(
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
