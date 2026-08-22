use std::path::Path;

// use crate to implement this one
fn audio_to_pcm(audio_path: &Path)->(){
    unimplemented!()
}

fn FT(pcm_data: i32)->(){
    unimplemented!()
}

fn main() {
    // transfer the audio file to pcm_data 
    let audio_path = Path::new("_");
    let pcm_data = audio_to_pcm(audio_path);

    // do the "Fourier transform"
    
}
