use std::{
    fs::File,
    io::stdout,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::anyhow;
use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    Device, Error, ErrorKind, FromSample, InputCallbackInfo, SampleFormat, SizedSample, Stream,
    StreamConfig,
};
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use log::LevelFilter;
use ringbuf::{
    traits::{Consumer, Observer, Producer, Split},
    HeapRb,
};
use rustfft::{num_complex::Complex, FftPlanner};
use simplelog::{Config, WriteLogger};

// 封装一个只在cargo run(debug)下执行的debug_print()函数
#[cfg(debug_assertions)]
macro_rules! debug_println {
    ($($args:tt)*) => {
        log::info!($($args)*);
    }
}
#[cfg(not(debug_assertions))]
macro_rules! debug_println {
    ($($args:tt)*) => {};
}

type MyProducer<T> = ringbuf::wrap::caching::Caching<
    std::sync::Arc<ringbuf::SharedRb<ringbuf::storage::Heap<T>>>,
    true,
    false,
>;

// 定义常数
const FFT_SIZE: usize = 1024;
const RING_CAPACITY_CPAL_FFT: usize = FFT_SIZE * 40;
const RING_CAPACITY_FFT_UI: usize = FFT_SIZE * 40;

fn err_fn(err: Error) {
    match err.kind() {
        ErrorKind::DeviceChanged | ErrorKind::RealtimeDenied => {
            log::error!("{err}")
        }
        _ => log::error!("Stream error: {err}"),
    }
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
}

fn run<T>(
    input_device: &Device,
    config: StreamConfig,
    mut producer: MyProducer<f32>,
) -> Result<Stream, anyhow::Error>
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
    // 塞入初始化静音数据
    for _ in 0..(RING_CAPACITY_CPAL_FFT / 2) {
        // producer.try_push(T::EQUILIBRIUM).unwrap();
        producer.try_push(0.0f32).unwrap();
    }
    // "move" 能让闭包捕获的外部变量拿走所有权,而不是继续借用他们
    let input_data_fn = move |data: &[T], _: &InputCallbackInfo| {
        let samples: Vec<f32> = data.iter().map(|d| d.to_sample::<f32>()).collect();
        let pushed_slice_cnt = producer.push_slice(&samples);
        debug_println!(
            "pushed_slice_cnt = {:?}, data.len() = {:?}.",
            pushed_slice_cnt,
            data.len()
        );
        if pushed_slice_cnt < data.len() {
            log::error!("Producing fft input data too fast!!");
        }
    };

    // Build streams.
    debug_println!(
        "Attempting to build input stream with {} samples and `{config:?}`.",
        T::FORMAT
    );
    let input_stream = input_device.build_input_stream(config, input_data_fn, err_fn, None)?;
    debug_println!("Successfully built streams.");

    Ok(input_stream)
}

fn init_device() -> Device {
    // CPAL: init Host and Device
    let host = cpal::host_from_id(cpal::HostId::PipeWire).unwrap();
    let input_device = host
        .devices()
        .unwrap()
        .find(|d| {
            d.description()
                .map(|des| des.name() == "default_sink")
                .unwrap_or(false)
        })
        .ok_or(anyhow!("Cannot find the \"default_sink\"!"));
    input_device.unwrap()
}

fn main() -> Result<(), anyhow::Error> {
    // Create RingBuffer for "input_data_fn" (producer), for the "RustFFT"(consumer)
    let ring_pcm_fft = HeapRb::<f32>::new(RING_CAPACITY_CPAL_FFT);
    let ring_fft_ui = HeapRb::<Complex<f32>>::new(RING_CAPACITY_FFT_UI);
    let (producer_a, mut consumer_a) = ring_pcm_fft.split();
    let (mut producer_b, mut consumer_b) = ring_fft_ui.split();
    
    // 申明线程退出信号(shutdown)
    let shutdown = Arc::new(AtomicBool::new(false));

    // 初始化日志输出
    let _ = WriteLogger::init(
        LevelFilter::Info,
        Config::default(),
        File::create("my_cava.log").unwrap(),
    );
    log::info!("Hello logger!");
    log::error!("This is when errors happended in logger!");

    // CPAL get the device and config
    let input_device = init_device();
    let input_config = input_device.default_input_config()?;
    debug_println!(
        "sample rate: {} Hz, channels: {}, format: {:?}",
        input_config.sample_rate(),
        input_config.channels(),
        input_config.sample_format()
    );

    // 开始执行(保活stream即可,自有多线程调度)
    let stream = match input_config.sample_format() {
        // SampleFormat::I8 => run::<i8>(&input_device, input_config.into()),
        // SampleFormat::I16 => run::<i16>(&input_device, input_config.into()),
        // SampleFormat::I32 => run::<i32>(&input_device, input_config.into()),
        // SampleFormat::I64 => run::<i64>(&input_device, input_config.into()),
        // SampleFormat::U8 => run::<u8>(&input_device, input_config.into()),
        // SampleFormat::U16 => run::<u16>(&input_device, input_config.into()),
        // SampleFormat::U32 => run::<u32>(&input_device, input_config.into()),
        // SampleFormat::U64 => run::<u64>(&input_device, input_config.into()),
        SampleFormat::F32 => run::<f32>(&input_device, input_config.into(), producer_a),
        // SampleFormat::F64 => run::<f64>(&input_device, input_config.into()),
        sample_format => panic!("Unsupported sample format '{sample_format}'"),
    }?;
    stream.play()?;

    // atomicBool
    let shutdown_worker_a = Arc::clone(&shutdown);

    // spawn the thread for fft
    let handle_fft = thread::spawn(move || -> Result<(), anyhow::Error> {
        // RustFFT(PCM_data -> 频域数据<频谱>)
        let mut planner = FftPlanner::<f32>::new();
        let fft = planner.plan_fft_forward(FFT_SIZE);
        let channels = input_config.channels();
        let required_samples = FFT_SIZE * (channels as usize);

        loop {
            // Data is consumed too fast!!! should be wait here
            while consumer_a.occupied_len() < required_samples {
                thread::sleep(Duration::from_millis(1));
            }

            // vars about poped data
            // let mut poped_data = vec![T::EQUILIBRIUM; required_samples];
            let mut poped_data = vec![0.0f32; required_samples];
            let poped_data_len = consumer_a.pop_slice(&mut poped_data);
            debug_println!("poped_data_len = {:?}", poped_data_len);

            // convert the data to satisfy the buffer(已经是 f32,无需转换)
            let mut buffer: Vec<Complex<f32>> = poped_data
                .chunks_exact_mut(2)
                .map(|lr| Complex::<f32>::new((lr[0] + lr[1]) / (channels as f32), 0.0))
                .collect();
            debug_println!("buffer[10](before) = {:?}", buffer[10]);
            fft.process(&mut buffer);
            debug_println!("buffer[10](after) = {:?}", buffer[10]);

            // If have signals to end thread, then exit it
            if shutdown_worker_a.load(Ordering::Relaxed) == true {
                break;
            }
        }
        Ok(())
    });

    // 利用crossterm进入AlternateScreen
    let mut stdout = stdout();
    enable_raw_mode()?; // 开启raw_mode才能逐键捕获,不然需要按回车
    let _ = execute!(stdout, EnterAlternateScreen);
    println!("Hello AlernateScreen");

    // spawn the thread for "exit" monitoring
    let shutdown_worker_b = Arc::clone(&shutdown);
    let handle_keyscan = thread::spawn(move || -> Result<(), anyhow::Error> {
        loop {
            // exit the AlternateScreen when press 'q'
            if event::poll(Duration::from_millis(16)).unwrap() {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') {
                        break;
                    }
                }
            }
        }
        // Crossterm: exit the alternate screen
        // And change atomicbool for other threads
        shutdown_worker_b.store(true, Ordering::Relaxed);
        let _ = execute!(stdout, LeaveAlternateScreen)?;
        disable_raw_mode()?;
        Ok(())
    });

    // 清理与回收
    // 等待"q"按下,并等待所有线程退出,同时也保活了主线程
    while shutdown.load(Ordering::Relaxed) != true {
        thread::sleep(Duration::from_millis(100));
    }
    // 等待所有线程推出并drop这个stream(stream的存在自动拉起一个线程,只要被drop就立刻失效)
    drop(stream);
    let _ = handle_fft.join().unwrap();
    let _ = handle_keyscan.join().unwrap();

    Ok(())
}
