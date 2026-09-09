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
    StreamConfig, SupportedStreamConfig,
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

type MyConsumer<T> =
    ringbuf::wrap::caching::Caching<Arc<ringbuf::SharedRb<ringbuf::storage::Heap<T>>>, false, true>;

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

fn display_fft_buffer(buffer: &Vec<Complex<f32>>, config: StreamConfig) -> () {
    // fft_data(transferred to Vec<f32>)
    let fft_data: Vec<f32> = buffer
        .iter()
        .map(|fft_complex| fft_complex.norm())
        .collect();

    // 定义Vec<f32>(长度FFT_SIZE)中每一个数字是一个bin(视为cava显示中的一根柱子)
    // 每两个bin之间相隔的频率等于sample_rate / FFT_SIZE
    let freq_atom: f32 = config.sample_rate as f32 / FFT_SIZE as f32;
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
    // !!!这里有性能问题,这里没必要用迭代器,这个迭代器非常耗时(错误,纯算术运算,不太耗时)
    let input_data_fn = move |data: &[T], _: &InputCallbackInfo| {
        let pushed_slice_cnt = producer.push_iter(data.iter().map(|d| d.to_sample::<f32>()));
        if pushed_slice_cnt < data.len() {
            log::error!("Producing fft input data too fast!!");
        }
    };

    // Build streams.
    let input_stream = input_device.build_input_stream(config, input_data_fn, err_fn, None)?;
    Ok(input_stream)
}

/// CPAL: init Host and Device
fn init_device() -> Device {
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

struct MyRingData {
    p_a: MyProducer<f32>,
    c_a: MyConsumer<f32>,
    p_b: MyProducer<Complex<f32>>,
    c_b: MyConsumer<Complex<f32>>,
}

/// Create RingBuffer for "input_data_fn" (producer), for the "RustFFT"(consumer)
fn create_ring() -> MyRingData {
    let ring_pcm_fft = HeapRb::<f32>::new(RING_CAPACITY_CPAL_FFT);
    let ring_fft_ui = HeapRb::<Complex<f32>>::new(RING_CAPACITY_FFT_UI);
    let (producer_a, consumer_a) = ring_pcm_fft.split();
    let (producer_b, consumer_b) = ring_fft_ui.split();
    MyRingData {
        p_a: producer_a,
        c_a: consumer_a,
        p_b: producer_b,
        c_b: consumer_b,
    }
}

/// CPAL: init input_config
fn init_input_config(input_device: &Device) -> Result<SupportedStreamConfig, anyhow::Error> {
    Ok(input_device.default_input_config()?)
}

/// CPAL get the input_device and input_config
fn init_cpal() -> Result<(Device, SupportedStreamConfig), anyhow::Error> {
    let input_device = init_device();
    let input_config = init_input_config(&input_device)?;
    Ok((input_device, input_config))
}

/// 初始化日志输出
fn init_logger() -> Result<(), anyhow::Error> {
    WriteLogger::init(
        LevelFilter::Info,
        Config::default(),
        File::create("my_cava.log").unwrap(),
    )?;
    log::info!("Hello logger!");
    log::error!("This is when errors happended in logger!");
    Ok(())
}

fn make_stream_cpal(
    input_device: &Device,
    input_config: &SupportedStreamConfig,
    p_a: MyProducer<f32>,
) -> Result<Stream, anyhow::Error> {
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
        SampleFormat::F32 => run::<f32>(&input_device, (input_config).clone().into(), p_a),
        // SampleFormat::F64 => run::<f64>(&input_device, input_config.into()),
        sample_format => panic!("Unsupported sample format '{sample_format}'"),
    }?;
    Ok(stream)
}

/// 需要保活stream!!!!,不能放任跑出作用域后隐式drop(stream)
fn play_stream_cpal(stream: &Stream) -> Result<(), anyhow::Error> {
    stream.play()?;
    Ok(())
}

/// end the whole program in worker
fn end_program_through_worker(shutdown_worker: Arc<AtomicBool>) -> Result<(), anyhow::Error> {
    exit_alternate_screen()?;
    shutdown_worker.store(true, Ordering::Relaxed);
    Ok(())
}

/// make fft buffer
fn make_fft_buffer(
    poped_data: &mut [f32],
    channels: usize,
) -> Result<Vec<Complex<f32>>, anyhow::Error> {
    let buffer = match channels {
        1usize => {
            let res = poped_data
                .iter()
                .map(|data| Complex::<f32>::new(*data, 0.0f32))
                .collect();
            Ok(res)
        }
        2usize => {
            let res = poped_data
                .chunks_exact_mut(2)
                .map(|lr| Complex::<f32>::new((lr[0] + lr[1]) / 2f32, 0.0f32))
                .collect();
            Ok(res)
        }
        _ => Err(anyhow!(
            "unsupported channel count: {channels}, only 1 or 2 are supported"
        )),
    };
    buffer
}

/// RustFFT(PCM_data -> 频域数据<频谱>)
fn fft_worker(
    shutdown_worker: Arc<AtomicBool>,
    mut c_a: MyConsumer<f32>,
    mut p_b: MyProducer<Complex<f32>>,
    channels: usize,
) -> Result<(), anyhow::Error> {
    let mut planner = FftPlanner::<f32>::new();
    let fft = planner.plan_fft_forward(FFT_SIZE);
    let required_samples = FFT_SIZE * channels;
    let mut poped_data = vec![0.0f32; required_samples];
    // init ringbuffer_b
    for _ in 0..(RING_CAPACITY_FFT_UI / 2) {
        p_b.try_push(Complex::<f32>::new(0.0f32, 0.0f32)).unwrap();
    }
    loop {
        // Data is consumed too fast!!! should be wait here
        while c_a.occupied_len() < required_samples {
            // If have signals to end thread, then exit it
            if shutdown_worker.load(Ordering::Relaxed) == true {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        // If have signals to end thread, then exit it
        if shutdown_worker.load(Ordering::Relaxed) == true {
            break;
        }

        // use poped data to fill the buffer
        let _ = c_a.pop_slice(&mut poped_data);
        let mut buffer = make_fft_buffer(&mut poped_data, channels)?;
        fft.process(&mut buffer);

        // push the "buffer" to the "ring_fft_ui"
        let pushed_slice_cnt = p_b.push_slice(&buffer);

        // log
        if pushed_slice_cnt < buffer.len() {
            log::error!("Producing fft output data too fast!!");
        }
    }
    Ok(())
}

fn init_atomicbool() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

fn ui_worker(
    shutdown_worker: Arc<AtomicBool>,
    mut c_b: MyConsumer<Complex<f32>>,
    input_config: &SupportedStreamConfig,
) -> Result<(), anyhow::Error> {
    let required_samples = FFT_SIZE;
    let mut poped_data = vec![Complex::<f32>::new(0.0f32, 0.0f32); required_samples];
    loop {
        // wait for enough data to be received
        while c_b.occupied_len() < required_samples {
            // if have signals to end thread, then exit it
            if shutdown_worker.load(Ordering::Relaxed) == true {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        // if have signals to end thread, then exit it
        if shutdown_worker.load(Ordering::Relaxed) == true {
            break;
        }

        // pop data
        let _ = c_b.pop_slice(&mut poped_data);

        // draw the ui using the "crossterm"
        display_fft_buffer(&poped_data, input_config.clone().into());
    }
    Ok(())
}

/// exit alternateScreen, disable raw mode
fn exit_alternate_screen() -> Result<(), anyhow::Error> {
    let mut stdout = stdout();
    disable_raw_mode()?;
    let _ = execute!(stdout, LeaveAlternateScreen);
    Ok(())
}

fn keyscan_worker(shutdown_worker: Arc<AtomicBool>) -> Result<(), anyhow::Error> {
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

    // end the whole program
    end_program_through_worker(shutdown_worker)?;
    Ok(())
}

/// enter alternate,enable raw mode(能不用按Enter直接捕获键)
fn enter_alternate_screen() -> Result<(), anyhow::Error> {
    let mut stdout = stdout();
    enable_raw_mode()?;
    let _ = execute!(stdout, EnterAlternateScreen);
    println!("Hello AlernateScreen");
    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    // init
    init_logger()?;
    enter_alternate_screen()?;
    let ring_data = create_ring();
    let shutdown = init_atomicbool();

    // CPAL create
    let (input_device, input_config) = init_cpal()?;
    let stream = make_stream_cpal(&input_device, &input_config, ring_data.p_a)?;
    play_stream_cpal(&stream)?;

    // spawn the thread for fft
    let shutdown_worker_a = Arc::clone(&shutdown);
    let handle_fft = thread::spawn(move || {
        fft_worker(
            shutdown_worker_a,
            ring_data.c_a,
            ring_data.p_b,
            (input_config.channels()) as usize,
        )
    });

    // spawn the thread for ui display (crossterm create)
    let shutdown_worker_b = Arc::clone(&shutdown);
    let handle_ui =
        thread::spawn(move || ui_worker(shutdown_worker_b, ring_data.c_b, &input_config));

    // spawn the thread for "exit" monitoring
    let shutdown_worker_c = Arc::clone(&shutdown);
    let handle_keyscan = thread::spawn(move || keyscan_worker(shutdown_worker_c));

    // wait for shutdown's change
    while shutdown.load(Ordering::Relaxed) != true {
        thread::sleep(Duration::from_millis(100));
    }
    // clean: drop stream and exit all thread
    drop(stream);
    let _ = handle_fft.join().unwrap();
    let _ = handle_keyscan.join().unwrap();
    let _ = handle_ui.join().unwrap();

    Ok(())
}
