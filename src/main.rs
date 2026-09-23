use std::{
    fs::File,
    io::{stdout, Stdout, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use anyhow::anyhow;
use apodize::hanning_iter;
use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait},
    Device, Error, ErrorKind, FromSample, InputCallbackInfo, SampleFormat, SizedSample, Stream,
    StreamConfig, SupportedStreamConfig,
};
use crossterm::{
    cursor,
    event::{self, Event, KeyCode},
    execute, queue,
    style::{Color, Print, SetBackgroundColor, SetForegroundColor},
    terminal::{
        self, disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
    },
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
// —— 频谱显示参数 ——
const BAR_WIDTH: usize = 3; // 每根柱子的列宽(字符)
const BAR_SPACING: usize = 1; // 柱子之间的空隙列数
const DB_FLOOR: f32 = -60.0; // 动态范围下限(dB):柱高映射到 [-60, 0] dB
const GRAVITY: f32 = 1.5; // 幂次曲线:>1 让小声柱子相对更矮,高低差更明显

// —— cava 式降噪/平滑参数(对齐 cava 默认配置)——
const NOISE_REDUCTION: f32 = 0.77; // 降噪强度(0..1],越大杂波越少、柱子越"糊"(cava 默认 77%)
const FRAMERATE: f32 = 60.0; // 标称帧率,用于平滑/autosens 的时域归一(cava 默认 60)
const AUTOSENS: f32 = 1.0; // autosens 增益抬升速度(cava 默认 1)
const MONSTERCAT: f32 = 0.0; // monstercat 邻柱平滑系数,0=关闭(cava 默认 0)
const WAVES: usize = 0; // waves 邻柱平滑,>0 开启(cava 默认 0)

// 柱子按对数频率分布,覆盖 [LOWER_CUTOFF_FREQ, HIGHER_CUTOFF_FREQ].
// 上限取 ~10kHz 即可:10k~24k(奈奎斯特)段基本无内容,别把柱子浪费在超声波上.
const LOWER_CUTOFF_FREQ: f32 = 40.0;
const HIGHER_CUTOFF_FREQ: f32 = 13000.0;

// 柱顶"坠格":落在柱子上方的全字符块,下落速度比柱子慢
const CAP_SIZE: usize = 0; // 坠格高度(>=8或0,否则会因为没有"中填充"的unicode而闪烁)(单位:1/8 格,8 = 1 格)
const CAP_GRAVITY: f32 = 1.0; // 坠格每帧下落步进(单位:1/8 格)

// 垂直渐变:HSV 彩虹,颜色随高度连续扫过色相(底部 -> 顶部)
const GRADIENT_START_HUE: f32 = 0.0; // 底部色相(度),0 = 红
const GRADIENT_END_HUE: f32 = 240.0; // 顶部色相(度),360 = 红(整圈彩虹)
const GRADIENT_SAT: f32 = 1.0; // 饱和度(0..1)
const GRADIENT_VAL: f32 = 1.0; // 明度(0..1)

fn err_fn(err: Error) {
    match err.kind() {
        ErrorKind::DeviceChanged | ErrorKind::RealtimeDenied => {
            log::error!("{err}")
        }
        _ => log::error!("Stream error: {err}"),
    }
}

/// 终端一格的内容:字符 + 前景色(用于 diff 渲染,只重绘变化过的格子).
#[derive(Clone, Copy, PartialEq)]
struct Cell {
    ch: char,
    fg: Color,
    bg: Color,
}

/// HSV(色相 h 单位:度,s/v 单位 0..1)-> RGB(0..255).
fn hsv_to_rgb(h: f32, s: f32, v: f32) -> (u8, u8, u8) {
    let h = h.rem_euclid(360.0);
    let c = v * s;
    let x = c * (1.0 - ((h / 60.0) % 2.0 - 1.0).abs());
    let m = v - c;
    let (r, g, b) = match h as u32 / 60 {
        0 => (c, x, 0.0),
        1 => (x, c, 0.0),
        2 => (0.0, c, x),
        3 => (0.0, x, c),
        4 => (x, 0.0, c),
        _ => (c, 0.0, x),
    };
    (
        ((r + m) * 255.0).round() as u8,
        ((g + m) * 255.0).round() as u8,
        ((b + m) * 255.0).round() as u8,
    )
}

/// 根据高度比例 t(0=底部,1=顶部)取垂直渐变颜色:HSV 彩虹,随高度连续扫过色相.
fn gradient_color(t: f32) -> Color {
    let t = t.clamp(0.0, 1.0);
    let hue = GRADIENT_START_HUE + (GRADIENT_END_HUE - GRADIENT_START_HUE) * t;
    let (r, g, b) = hsv_to_rgb(hue, GRADIENT_SAT, GRADIENT_VAL);
    Color::Rgb { r, g, b }
}

struct SpectrumRenderer {
    stdout: Stdout,
    width: u16,
    height: u16,
    frame_buffer: Vec<Cell>,         // 上一帧画面,用于 diff 渲染
    bar_ranges: Vec<(usize, usize)>, // 每根柱对应的 FFT bin 区间(预计算,互不重叠)
    cap_heights: Vec<f32>,           // 每根柱顶坠格(底部)的高度(单位:1/8 格)
    cap_active: Vec<bool>,           // 每根柱的坠格是否已激活(出现过峰值,静音时不画)
    bar_values: Vec<f32>,            // 本帧每根柱的强度(0..1,已含平滑/autosens)
    sensitivity: f32,                // autosens 自动增益(缓变)
    // —— cava 式降噪平滑的状态(cava_fall/cava_mem/cava_peak/prev_cava_out)——
    bar_fall: Vec<f32>, // falloff 计数器(下落越久越大)
    bar_mem: Vec<f32>,  // integral 低通的记忆项
    bar_peak: Vec<f32>, // 当前"峰值"(下落曲线的起点)
    prev_bar: Vec<f32>, // 上一帧柱值(判断涨/跌)
}

impl SpectrumRenderer {
    pub fn new(stream_config: StreamConfig) -> Result<Self, anyhow::Error> {
        let (width, height) = terminal::size()?;
        let cells = vec![
            Cell {
                ch: ' ',
                fg: Color::Reset,
                bg: Color::Reset
            };
            width as usize * height as usize
        ];
        let freq_atom = stream_config.sample_rate as f32 / FFT_SIZE as f32;
        // 柱子数封顶为频率范围内的可用 bin 数(否则低频端必然有柱子共享 bin)
        let total_bins = (HIGHER_CUTOFF_FREQ / freq_atom) as usize;
        let bars = (width as usize / (BAR_WIDTH + BAR_SPACING)).min(total_bins);
        let bar_ranges = build_bar_ranges(bars, total_bins, freq_atom);
        Ok(Self {
            stdout: stdout(),
            width,
            height,
            frame_buffer: cells,
            bar_ranges,
            cap_heights: vec![0.0; bars],
            cap_active: vec![false; bars],
            bar_values: vec![0.0; bars],
            sensitivity: 1.0,
            bar_fall: vec![0.0; bars],
            bar_mem: vec![0.0; bars],
            bar_peak: vec![0.0; bars],
            prev_bar: vec![0.0; bars],
        })
    }

    /// 只重绘与上一帧不同的格子:移动光标 -> 设前景/背景色 -> 打印字符 -> 更新缓存.
    fn draw_cell(
        &mut self,
        col: usize,
        row: u16,
        ch: char,
        fg: Color,
        bg: Color,
    ) -> Result<(), anyhow::Error> {
        let idx = row as usize * self.width as usize + col;
        let cell = &mut self.frame_buffer[idx];
        if cell.ch != ch || cell.fg != fg || cell.bg != bg {
            queue!(
                self.stdout,
                cursor::MoveTo(col as u16, row),
                SetForegroundColor(fg),
                SetBackgroundColor(bg),
                Print(ch)
            )?;
            *cell = Cell { ch, fg, bg };
        }
        Ok(())
    }

    /// 更新第 x 根柱的高度与其坠格,返回 (柱高, 坠格底部位置).
    /// 柱高单位是"格",坠格位置单位是"1/8 格".
    /// 柱值已含 cava 平滑与 autosens(见 apply_cava_smoothing),这里直接映射到格.
    fn update_heights(&mut self, x: usize, max_height: f32) -> (f32, f32) {
        let bar_height = self.bar_values[x].clamp(0.0, 1.0) * max_height;

        // 坠格:柱子涨就跟上去(并激活),柱子跌就按 CAP_GRAVITY(1/8 格)步进下落,
        // 可一路落到屏幕最底行(cap = 0).
        let bar_top = bar_height * 8.0;
        let cap = self.cap_heights[x];
        if bar_top > cap {
            self.cap_active[x] = true;
            self.cap_heights[x] = bar_top;
        } else {
            self.cap_heights[x] = (cap - CAP_GRAVITY).max(bar_top);
        }
        (bar_height, self.cap_heights[x])
    }

    /// cava 式降噪平滑 + autosens(移植自 cava 的 cava_execute):
    /// 对每根柱做 falloff(平滑下落)+ integral(降噪低通),钳制到 [0,1],
    /// 并根据是否 overshoot 用乘法式更新 autosens 增益.
    fn apply_cava_smoothing(&mut self, silence: bool) {
        let bars = self.bar_values.len();
        let framerate_mod = 66.0 / FRAMERATE;
        let gravity_mod = framerate_mod.powf(2.5) * 2.0 / NOISE_REDUCTION;
        let integral_mod = framerate_mod.powf(0.1);

        let mut overshoot = false;
        for x in 0..bars {
            // 先乘 autosens 增益(增益把柱值推到 [0,1] 附近,超了就是 overshoot)
            let mut v = self.bar_values[x] * self.sensitivity;

            // falloff:柱子下跌时按二次曲线平滑下落,而非瞬间跌
            if v < self.prev_bar[x] && NOISE_REDUCTION > 0.1 {
                v = self.bar_peak[x] * (1.0 - self.bar_fall[x] * self.bar_fall[x] * gravity_mod);
                if v < 0.0 {
                    v = 0.0;
                }
                self.bar_fall[x] += 0.028;
            } else {
                self.bar_peak[x] = v;
                self.bar_fall[x] = 0.0;
            }
            self.prev_bar[x] = v;

            // integral:把上一帧记忆项按 NOISE_REDUCTION 混入,低通降噪
            v = self.bar_mem[x] * NOISE_REDUCTION / integral_mod + v;
            self.bar_mem[x] = v;

            if v > 1.0 {
                overshoot = true;
                v = 1.0;
            }
            self.bar_values[x] = v;
        }

        // autosens:乘法式缓变增益(cava 做法)
        if overshoot {
            self.sensitivity *= 1.0 - 0.02 * framerate_mod;
        } else if !silence {
            self.sensitivity *= 1.0 + 0.001 * framerate_mod * AUTOSENS;
        }
    }

    /// monstercat / waves 邻柱平滑(移植自 cava 的 monstercat_filter):
    /// 用较亮柱子的值向两侧"填",让频谱轮廓更连贯、少毛刺.
    fn apply_monstercat_filter(&mut self) {
        let bars = self.bar_values.len();
        if bars == 0 {
            return;
        }
        if WAVES > 0 {
            // waves:cava 在"格"单位上做二次衰减;这里作用在归一化 [0,1] 上,
            // 用终端高度折算衰减步长.
            let eighth = (self.height as f32 * 8.0).max(1.0);
            for z in 0..bars {
                self.bar_values[z] /= 1.25;
                let base = self.bar_values[z];
                for my in (0..z).rev() {
                    let de = (z - my) as f32;
                    let v = base - de * de / eighth;
                    if v > self.bar_values[my] {
                        self.bar_values[my] = v;
                    }
                }
                for my in z + 1..bars {
                    let de = (my - z) as f32;
                    let v = base - de * de / eighth;
                    if v > self.bar_values[my] {
                        self.bar_values[my] = v;
                    }
                }
            }
        } else if MONSTERCAT > 0.0 {
            for z in 0..bars {
                let base = self.bar_values[z];
                for my in (0..z).rev() {
                    let de = (z - my) as f32;
                    let v = base / (MONSTERCAT * 1.5).powf(de);
                    if v > self.bar_values[my] {
                        self.bar_values[my] = v;
                    }
                }
                for my in z + 1..bars {
                    let de = (my - z) as f32;
                    let v = base / (MONSTERCAT * 1.5).powf(de);
                    if v > self.bar_values[my] {
                        self.bar_values[my] = v;
                    }
                }
            }
        }
    }

    /// 绘制第 x 根柱这一列(bar 列 + 分隔列).
    fn draw_bar(&mut self, x: usize, bar_height: f32, cap: f32) -> Result<(), anyhow::Error> {
        let base_col = x * (BAR_WIDTH + BAR_SPACING);
        // 坠格位置量化到整数 1/8 格
        let cap_bottom = cap as usize; // 坠格底部(1/8 格)
        let cap_top = cap_bottom + CAP_SIZE; // 坠格顶部(1/8 格),CAP_SIZE 单位是 1/8 格
        for row in 0..self.height {
            let dist = (self.height - 1 - row) as usize; // 距底部的行数
            let cell_bottom = dist * 8; // 本行底部(1/8 格)
            let cell_top = cell_bottom + 8; // 本行顶部(1/8 格)

            // 坠格与本行 [cell_bottom, cell_top) 的交集;cap_active 是静音守卫
            let lo = cap_bottom.max(cell_bottom);
            let hi = cap_top.min(cell_top);
            let cap_ch = if self.cap_active[x] && lo < hi {
                if hi == cell_top {
                    // 坠格底边(或满格):从本行顶部向下填充(上块,精确到 1/8)
                    upper_block(cell_top - lo)
                } else {
                    // 坠格顶边:从本行底部向上填充(下块,精确到 1/8)
                    block_char(hi - cell_bottom)
                }
            } else {
                ' '
            };

            // 柱子在该行的 1/8 填充
            let filled = (bar_height * 8.0) as usize;
            let filled = filled.saturating_sub(dist * 8).min(8);

            // 渐变位置:该格下半格 / 上半格 / 填充顶端 分别在整根柱上的比例
            let inv_h = 1.0 / bar_height.max(f32::EPSILON);
            let t_bottom = (dist as f32 + 0.25) * inv_h;
            let t_top = (dist as f32 + 0.75) * inv_h;
            let t_fill_top = (dist as f32 + filled as f32 / 8.0) * inv_h;

            // 确定 (字符, 前景色, 背景色)
            let (ch, fg, bg) = if cap_ch != ' ' {
                // 坠格:峰值色(渐变顶端),背景复位
                (cap_ch, gradient_color(1.0), Color::Reset)
            } else if filled == 0 {
                (' ', Color::Reset, Color::Reset)
            } else if filled == 8 {
                // 满格:上半格前景色 + 下半格背景色,一格显示两种颜色(半格渐变)
                ('▀', gradient_color(t_top), gradient_color(t_bottom))
            } else {
                // 顶格部分填充:1/8 块(单前景色,保留细腻柱顶),背景复位
                (block_char(filled), gradient_color(t_fill_top), Color::Reset)
            };

            for offset in 0..BAR_WIDTH {
                self.draw_cell(base_col + offset, row, ch, fg, bg)?;
            }
            for offset in 0..BAR_SPACING {
                self.draw_cell(
                    base_col + BAR_WIDTH + offset,
                    row,
                    ' ',
                    Color::Reset,
                    Color::Reset,
                )?;
            }
        }
        Ok(())
    }
}

/// 第 s 根柱(共 bars 根)对应的 FFT bin 下标.
/// 柱子按对数频率分布,覆盖 [LOWER_CUTOFF_FREQ, HIGHER_CUTOFF_FREQ].
/// freq_atom = 每个 bin 代表的频率(Hz).
fn bar_to_bin(s: usize, bars: usize, freq_atom: f32) -> usize {
    let t = s as f32 / bars as f32;
    let freq = LOWER_CUTOFF_FREQ * (HIGHER_CUTOFF_FREQ / LOWER_CUTOFF_FREQ).powf(t);
    (freq / freq_atom) as usize
}

/// 预计算每根柱子的 FFT bin 区间 [start, end),保证严格递增,互不重叠.
/// 低频端多根柱子原本会截断到同一个 bin,这里把它们依次顺延到后续 bin,
/// 使每根柱子都读到独立的 bin(否则这些柱子会有一模一样的变化趋势).
fn build_bar_ranges(bars: usize, total_bins: usize, freq_atom: f32) -> Vec<(usize, usize)> {
    let mut ranges = Vec::with_capacity(bars);
    let mut last_start = 0usize;
    for x in 0..bars {
        let start = bar_to_bin(x, bars, freq_atom).max(last_start);
        let end = bar_to_bin(x + 1, bars, freq_atom)
            .max(start + 1)
            .min(total_bins);
        ranges.push((start, end));
        last_start = start + 1;
    }
    ranges
}

/// 8 级填充字符:0 = 空格,1..=8 对应'下 1/8 块'到'全满块'.
fn block_char(level: usize) -> char {
    const LOWER_BLOCKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    if level == 0 {
        ' '
    } else {
        LOWER_BLOCKS[level - 1]
    }
}

/// 从单元格'顶部向下填充'的块字符(用于坠格的底边).
/// 1..=8 档精确对应 U+2594 / U+1FB82~1FB86 / U+2580 / U+2588,做到 1/8 格精度.
fn upper_block(fill: usize) -> char {
    const UPPER_BLOCKS: [char; 8] = [
        '\u{2594}',  // 1/8 UPPER ONE EIGHTH BLOCK
        '\u{1fb82}', // 2/8 UPPER ONE QUARTER BLOCK
        '\u{1fb83}', // 3/8 UPPER THREE EIGHTHS BLOCK
        '\u{2580}',  // 4/8 UPPER HALF BLOCK
        '\u{1fb84}', // 5/8 UPPER FIVE EIGHTHS BLOCK
        '\u{1fb85}', // 6/8 UPPER THREE QUARTERS BLOCK
        '\u{1fb86}', // 7/8 UPPER SEVEN EIGHTHS BLOCK
        '\u{2588}',  // 8/8 FULL BLOCK
    ];
    if fill == 0 {
        ' '
    } else {
        UPPER_BLOCKS[fill - 1]
    }
}

/// 某根柱子的原始强度(0..1):频段内线性幅度求和 -> dB 映射 -> gravity 幂次.
/// 求和(而非取最大)是 cava 的做法,对单 bin 噪声更鲁棒.
fn compute_bar_value(data: &[f32], range: (usize, usize)) -> f32 {
    let sum: f32 = data[range.0..range.1].iter().sum();
    let db = 20.0 * sum.max(1e-12).log10();
    ((db - DB_FLOOR) / (-DB_FLOOR))
        .clamp(0.0, 1.0)
        .powf(GRAVITY)
}

/// 用 crossterm 绘制频谱条.
/// 流程:原始强度 -> cava 降噪平滑 + autosens -> 邻柱平滑 -> 柱高 + 坠格 -> 渐变着色.
fn display_fft_buffer(
    normed_half_mag_data: &[f32],
    renderer: &mut SpectrumRenderer,
) -> Result<(), anyhow::Error> {
    let bars = renderer.bar_ranges.len();
    if bars == 0 {
        return Ok(());
    }
    let max_height = (renderer.height - 1) as f32;

    // 第一遍:算每根柱子的原始强度(频段求和 -> dB -> gravity)
    let mut frame_max = 0.0f32;
    for x in 0..bars {
        let v = compute_bar_value(normed_half_mag_data, renderer.bar_ranges[x]);
        renderer.bar_values[x] = v;
        frame_max = frame_max.max(v);
    }
    // 静音判定:整帧几乎无能量时,autosens 不抬增益(避免把底噪顶满)
    let silence = frame_max < 1e-6;

    // cava 式降噪平滑 + autosens(内含 overshoot 检测与增益调整)
    renderer.apply_cava_smoothing(silence);

    // 可选:monstercat / waves 邻柱平滑
    renderer.apply_monstercat_filter();

    // 第二遍:柱高 + 坠格 + 绘制每一列
    for x in 0..bars {
        let (bar_height, cap) = renderer.update_heights(x, max_height);
        renderer.draw_bar(x, bar_height, cap)?;
    }

    // 把本帧 diff 一次性刷出
    renderer.stdout.flush()?;
    Ok(())
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
        SampleFormat::F32 => run::<f32>(&input_device, (*input_config).into(), p_a),
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

/// process the hanning window
fn process_hanning_window(
    buffer: &mut Vec<Complex<f32>>,
    hanning_window: &[f32],
    samples: &mut [f32],
    channels: &usize,
) -> Result<(), anyhow::Error> {
    *buffer = match *channels {
        1usize => {
            let windowed = samples
                .iter()
                .zip(hanning_window.iter())
                .map(|(buffer_data, hanning_window)| {
                    Complex::<f32>::new(buffer_data * hanning_window, 0.0f32)
                })
                .collect();
            Ok(windowed)
        }
        2usize => {
            let windowed = samples
                .chunks_exact_mut(*channels)
                .zip(hanning_window.iter())
                .map(|(sample_data_lr, hanning_window)| {
                    Complex::<f32>::new(
                        (sample_data_lr[0] + sample_data_lr[1]) / ((*channels) as f32)
                            * hanning_window,
                        0.0f32,
                    )
                })
                .collect();
            Ok(windowed)
        }
        _ => Err(anyhow!(
            "unsupported channel count: {channels}, only 1 or 2 are supported!!!!"
        )),
    }?;
    Ok(())
}

fn init_hanning_window(hanning_window: &mut Vec<f32>) -> () {
    *hanning_window = hanning_iter(FFT_SIZE).map(|x| x as f32).collect();
}

/// RustFFT(PCM_data -> 频域数据<频谱>)
fn fft_worker(
    shutdown_worker: Arc<AtomicBool>,
    mut c_a: MyConsumer<f32>,
    mut p_b: MyProducer<Complex<f32>>,
    channels: usize,
    hanning_window: &[f32],
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

        // 去除直流分量(一阶高通等价):减去本帧均值,消除 DC 偏移/极低频隆隆声
        let mean: f32 = poped_data.iter().sum::<f32>() / poped_data.len() as f32;
        for s in poped_data.iter_mut() {
            *s -= mean;
        }

        // process buffer
        let mut buffer: Vec<Complex<f32>> = vec![];
        process_hanning_window(&mut buffer, &hanning_window, &mut poped_data, &channels)?;
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

/// FFT 结果 -> 一半频谱的"线性幅度"(norm / window_sum),供频段求和用.
/// 不在此处转 dB:cava 是先对频段内幅度求和,再整体转 dB.
fn fft_normalization(poped_data: &mut [Complex<f32>], window_sum: &f32) -> Vec<f32> {
    poped_data[0..(poped_data.len() / 2)]
        .iter()
        .map(|complex_data| complex_data.norm() / window_sum)
        .collect()
}

fn ui_worker(
    shutdown_worker: Arc<AtomicBool>,
    mut c_b: MyConsumer<Complex<f32>>,
    input_config: &SupportedStreamConfig,
    window_sum: &f32,
) -> Result<(), anyhow::Error> {
    let required_samples = FFT_SIZE;
    let mut poped_data = vec![Complex::<f32>::new(0.0f32, 0.0f32); required_samples];
    // 启动一个频谱渲染器实例
    let mut spectrum_renderer: SpectrumRenderer =
        SpectrumRenderer::new((*input_config).clone().into())?;

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

        // fft normalization(线性幅度)
        let normed_half_mag_data = fft_normalization(&mut poped_data, &window_sum);

        debug_println!("normed_half_mag_data = {:?}", normed_half_mag_data);
        // draw the ui using the "crossterm"
        display_fft_buffer(&normed_half_mag_data, &mut spectrum_renderer)?;
    }
    Ok(())
}

/// exit alternateScreen, disable raw mode
fn exit_alternate_screen() -> Result<(), anyhow::Error> {
    let mut stdout = stdout();
    let _ = execute!(stdout, cursor::Show);
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
    let _ = execute!(stdout, EnterAlternateScreen, cursor::Hide);
    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    // init
    init_logger()?;
    enter_alternate_screen()?;
    let ring_data = create_ring();
    let shutdown = init_atomicbool();
    let mut hanning_window: Vec<f32> = vec![];
    init_hanning_window(&mut hanning_window);
    let window_sum: f32 = hanning_window.iter().sum();

    // CPAL create (!!should keep stream alive)
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
            &hanning_window,
        )
    });

    // spawn the thread for ui display (crossterm create)
    let shutdown_worker_b = Arc::clone(&shutdown);
    let handle_ui = thread::spawn(move || {
        ui_worker(shutdown_worker_b, ring_data.c_b, &input_config, &window_sum)
    });

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
