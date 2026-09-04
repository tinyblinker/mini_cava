use anyhow::anyhow;
use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    Device, Error, ErrorKind, InputCallbackInfo, SampleFormat, SizedSample, StreamConfig,
};

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
    T: SizedSample + std::fmt::Debug + Send + 'static,
{
    let input_data_fn = move |data: &[T], _: &InputCallbackInfo| {
        println!("data = {:?}", data);
    };

    // Build streams.
    println!(
        "Attempting to build input stream with {} samples and `{config:?}`.",
        T::FORMAT
    );
    let input_stream = input_device.build_input_stream(config, input_data_fn, err_fn, None)?;
    println!("Successfully built streams.");

    // play the stream
    input_stream.play()?;

    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    // init Host and Device
    let host = cpal::host_from_id(cpal::HostId::PipeWire)?;
    let input_device = host
        .input_devices()?
        .find(|d| {
            d.id()
                .map(|id| id.to_string() == "default_sink")
                .unwrap_or(false)
        })
        .ok_or(anyhow!("找不到input sink"))?;
    println!("input device: {}", input_device);

    // init the config
    let input_config = input_device.default_input_config()?;
    println!(
        "sample rate: {} Hz, channels: {}, format: {:?}",
        input_config.sample_rate(),
        input_config.channels(),
        input_config.sample_format()
    );

    match input_config.sample_format() {
        SampleFormat::I8 => run::<i8>(&input_device, input_config.into()),
        SampleFormat::I16 => run::<i16>(&input_device, input_config.into()),
        SampleFormat::I32 => run::<i32>(&input_device, input_config.into()),
        SampleFormat::I64 => run::<i64>(&input_device, input_config.into()),
        SampleFormat::U8 => run::<u8>(&input_device, input_config.into()),
        SampleFormat::U16 => run::<u16>(&input_device, input_config.into()),
        SampleFormat::U32 => run::<u32>(&input_device, input_config.into()),
        SampleFormat::U64 => run::<u64>(&input_device, input_config.into()),
        SampleFormat::F32 => run::<f32>(&input_device, input_config.into()),
        SampleFormat::F64 => run::<f64>(&input_device, input_config.into()),
        sample_format => panic!("Unsupported sample format '{sample_format}'"),
    }?;

    // leave the thread sleep
    loop {
        std::thread::park();
    }

    Ok(())
}
