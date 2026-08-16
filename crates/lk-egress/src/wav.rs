//! Minimal PCM WAV writer (16-bit, mono or stereo).

use std::io::{BufWriter, Write};

pub struct WavWriter {
    writer: BufWriter<std::fs::File>,
    channels: u16,
    sample_rate: u32,
    data_bytes: u64,
}

const PCM_FMT: u16 = 1;

impl WavWriter {
    pub fn create(path: &str, channels: u16, sample_rate: u32) -> Result<Self, String> {
        let file = std::fs::File::create(path).map_err(|e| format!("open {path}: {e}"))?;
        let mut writer = BufWriter::new(file);
        // Write a placeholder RIFF header; filled on finish().
        writer
            .write_all(&[0u8; 44])
            .map_err(|e| format!("wav header: {e}"))?;
        Ok(WavWriter {
            writer,
            channels,
            sample_rate,
            data_bytes: 0,
        })
    }

    pub fn write_pcm(&mut self, pcm: &[i16]) -> Result<(), String> {
        let mut bytes = Vec::with_capacity(pcm.len() * 2);
        for s in pcm {
            bytes.extend_from_slice(&s.to_le_bytes());
        }
        self.writer
            .write_all(&bytes)
            .map_err(|e| format!("wav write: {e}"))?;
        self.data_bytes += bytes.len() as u64;
        Ok(())
    }

    /// Writes the final RIFF header and closes the file.
    pub fn finish(&mut self) -> Result<(), String> {
        let block_align = self.channels * 2;
        let byte_rate = self.sample_rate * block_align as u32;
        let total = 36 + self.data_bytes;
        let mut header = Vec::with_capacity(44);
        header.extend_from_slice(b"RIFF");
        header.extend_from_slice(&(total as u32).to_le_bytes());
        header.extend_from_slice(b"WAVE");
        header.extend_from_slice(b"fmt ");
        header.extend_from_slice(&16u32.to_le_bytes());
        header.extend_from_slice(&PCM_FMT.to_le_bytes());
        header.extend_from_slice(&self.channels.to_le_bytes());
        header.extend_from_slice(&self.sample_rate.to_le_bytes());
        header.extend_from_slice(&byte_rate.to_le_bytes());
        header.extend_from_slice(&block_align.to_le_bytes());
        header.extend_from_slice(&16u16.to_le_bytes());
        header.extend_from_slice(b"data");
        header.extend_from_slice(&(self.data_bytes as u32).to_le_bytes());
        self.writer.flush().map_err(|e| format!("wav flush: {e}"))?;
        use std::io::{Seek, SeekFrom};
        self.writer
            .seek(SeekFrom::Start(0))
            .map_err(|e| format!("wav seek: {e}"))?;
        self.writer
            .write_all(&header)
            .map_err(|e| format!("wav header: {e}"))?;
        self.writer.flush().map_err(|e| format!("wav flush: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_valid_wav() {
        let dir = std::env::temp_dir();
        let path = dir.join("lk_wav_test.wav");
        let p = path.to_str().unwrap();
        let mut w = WavWriter::create(p, 1, 48000).unwrap();
        let pcm: Vec<i16> = (0..4800).map(|i| i as i16).collect();
        w.write_pcm(&pcm).unwrap();
        w.finish().unwrap();

        let bytes = std::fs::read(p).unwrap();
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(&bytes[8..12], b"WAVE");
        assert_eq!(&bytes[12..16], b"fmt ");
        let _ = std::fs::remove_file(p);
        // data size: 4800 samples * 2 bytes = 9600
        let data_len = u32::from_le_bytes(bytes[40..44].try_into().unwrap());
        assert_eq!(data_len, 9600);
    }
}
