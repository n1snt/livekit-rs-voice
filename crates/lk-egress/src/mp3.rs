//! Minimal libmp3lame FFI (mono/stereo MP3 encoding), self-contained so we get
//! a correct encoder flush (the `lame` crate omits it, truncating the tail).
//!
//! Links `libmp3lame` (Debian: `libmp3lame-dev`; macOS: `brew install lame`).

use std::os::raw::{c_int, c_void};

pub const MP3_BUF: usize = 16 * 1024;
const SAMPLE_RATE: c_int = 48000;

#[link(name = "mp3lame")]
extern "C" {
    fn lame_init() -> *mut c_void;
    fn lame_set_in_samplerate(gfp: *mut c_void, sample_rate: c_int) -> c_int;
    fn lame_set_num_channels(gfp: *mut c_void, channels: c_int) -> c_int;
    fn lame_set_brate(gfp: *mut c_void, bitrate: c_int) -> c_int;
    fn lame_init_params(gfp: *mut c_void) -> c_int;
    fn lame_encode_buffer_interleaved(
        gfp: *mut c_void,
        pcm: *const i16,
        samples: c_int,
        mp3buf: *mut u8,
        mp3buf_size: c_int,
    ) -> c_int;
    fn lame_encode_buffer(
        gfp: *mut c_void,
        pcm_l: *const i16,
        pcm_r: *const i16,
        samples: c_int,
        mp3buf: *mut u8,
        mp3buf_size: c_int,
    ) -> c_int;
    fn lame_encode_flush(gfp: *mut c_void, mp3buf: *mut u8, size: c_int) -> c_int;
    fn lame_close(gfp: *mut c_void) -> c_int;
}

pub struct Mp3Encoder {
    handle: *mut c_void,
    channels: usize,
    buffer: Vec<u8>,
}

unsafe impl Send for Mp3Encoder {}

impl Drop for Mp3Encoder {
    fn drop(&mut self) {
        unsafe {
            lame_close(self.handle);
        }
    }
}

impl Mp3Encoder {
    /// Builds an MP3 encoder. `channels` is 1 (mono) or 2 (stereo).
    pub fn new(channels: usize, bitrate_kbps: i32) -> Result<Self, String> {
        unsafe {
            let handle = lame_init();
            if handle.is_null() {
                return Err("lame_init failed".to_string());
            }
            let check = |rc: c_int| -> Result<(), String> {
                if rc < 0 {
                    Err(format!("libmp3lame error {rc}"))
                } else {
                    Ok(())
                }
            };
            check(lame_set_in_samplerate(handle, SAMPLE_RATE))?;
            check(lame_set_num_channels(handle, channels as c_int))?;
            check(lame_set_brate(handle, bitrate_kbps))?;
            check(lame_init_params(handle))?;
            Ok(Mp3Encoder {
                handle,
                channels,
                buffer: vec![0u8; MP3_BUF],
            })
        }
    }

    /// Encodes PCM16 samples. For mono, `pcm` holds the samples; for stereo,
    /// it must be interleaved. Returns the encoded bytes (may be empty while
    /// the encoder buffers).
    pub fn encode(&mut self, pcm: &[i16]) -> Result<Vec<u8>, String> {
        if pcm.is_empty() {
            return Ok(Vec::new());
        }
        unsafe {
            let rc = if self.channels == 1 {
                lame_encode_buffer(
                    self.handle,
                    pcm.as_ptr(),
                    pcm.as_ptr(),
                    pcm.len() as c_int,
                    self.buffer.as_mut_ptr(),
                    MP3_BUF as c_int,
                )
            } else {
                lame_encode_buffer_interleaved(
                    self.handle,
                    pcm.as_ptr(),
                    (pcm.len() / 2) as c_int,
                    self.buffer.as_mut_ptr(),
                    MP3_BUF as c_int,
                )
            };
            if rc < 0 {
                return Err(format!("libmp3lame encode error {rc}"));
            }
            Ok(self.buffer[..rc as usize].to_vec())
        }
    }

    /// Flushes the encoder's tail (delay + padding). Call once at the end.
    pub fn flush(&mut self) -> Result<Vec<u8>, String> {
        unsafe {
            let rc = lame_encode_flush(self.handle, self.buffer.as_mut_ptr(), MP3_BUF as c_int);
            if rc < 0 {
                return Err(format!("libmp3lame flush error {rc}"));
            }
            Ok(self.buffer[..rc as usize].to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_pcm_to_mp3() {
        let mut enc = Mp3Encoder::new(1, 64).unwrap();
        let pcm: Vec<i16> = (0..4800).map(|i| ((i % 480) as f32 * 0.3) as i16).collect();
        let out = enc.encode(&pcm).unwrap();
        let tail = enc.flush().unwrap();
        let total = out.len() + tail.len();
        assert!(total > 0, "encoder produced no output");
        // The output should contain a valid MPEG frame header (0xFFEx).
        let bytes = [out, tail].concat();
        assert!(
            bytes
                .windows(2)
                .any(|w| w[0] == 0xff && (w[1] & 0xe0) == 0xe0),
            "no MPEG frame sync found"
        );
    }
}
