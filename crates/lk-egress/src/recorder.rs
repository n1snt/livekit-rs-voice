//! Recording pipeline: consume the room audio stream, decode Opus, mix all
//! tracks, and write WAV or MP3.

use std::collections::{HashMap, VecDeque};

use lk_proto::livekit as lk;
use tokio::sync::mpsc;

use crate::audio::{decode_opus, SAMPLE_RATE};
use crate::client::AudioPacket;
use crate::mp3::Mp3Encoder;
use crate::wav::WavWriter;

const FRAME_SAMPLES: usize = 960; // 20 ms at 48 kHz

/// The requested output format for a recording.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Wav,
    Mp3,
}

/// Per-track PCM buffers that are drained into mixed frames of fixed length.
struct Mixer {
    tracks: HashMap<String, VecDeque<i16>>,
}

impl Mixer {
    fn new() -> Self {
        Mixer {
            tracks: HashMap::new(),
        }
    }

    fn push(&mut self, cid: &str, pcm: Vec<i16>) {
        self.tracks.entry(cid.to_string()).or_default().extend(pcm);
    }

    /// Number of samples available in every track (the mixable minimum).
    fn available(&self) -> usize {
        if self.tracks.is_empty() {
            return 0;
        }
        self.tracks.values().map(|q| q.len()).min().unwrap_or(0)
    }

    /// Reads `len` samples from each track and mixes them (missing samples are
    /// silence). The result is averaged to avoid clipping.
    fn read_frame(&mut self, len: usize) -> Vec<i16> {
        let mut frame = vec![0i16; len];
        let count = self.tracks.len().max(1) as i32;
        for q in self.tracks.values_mut() {
            for slot in frame.iter_mut() {
                let s = q.pop_front().unwrap_or(0);
                *slot = (*slot as i32 + s as i32 / count).clamp(i16::MIN as i32, i16::MAX as i32)
                    as i16;
            }
        }
        frame
    }
}

/// Records the room's audio to `path` until the audio stream ends (the room
/// client drops its sender).
pub async fn run_recording(
    mut audio: mpsc::Receiver<AudioPacket>,
    path: &str,
    format: OutputFormat,
    mp3_bitrate: i32,
) -> Result<u64, String> {
    let mut mixer = Mixer::new();
    let mut wav = if format == OutputFormat::Wav {
        Some(WavWriter::create(path, 1, SAMPLE_RATE)?)
    } else {
        None
    };
    let mut mp3 = if format == OutputFormat::Mp3 {
        Some(Mp3Encoder::new(1, mp3_bitrate)?)
    } else {
        None
    };
    let mut frames: u64 = 0;

    while let Some(packet) = audio.recv().await {
        let pcm = match decode_opus(&packet.payload) {
            Ok(pcm) => pcm,
            Err(_) => continue, // skip a malformed frame rather than abort
        };
        mixer.push(&packet.track_cid, pcm);
        // Drain fixed-size frames; mix whatever is available in all tracks.
        while mixer.available() >= FRAME_SAMPLES {
            let frame = mixer.read_frame(FRAME_SAMPLES);
            if let Some(w) = &mut wav {
                w.write_pcm(&frame)?;
            }
            if let Some(e) = &mut mp3 {
                let out = e.encode(&frame).map_err(|e| format!("mp3 encode: {e}"))?;
                if !out.is_empty() {
                    append_bytes(path, &out)?;
                }
            }
            frames += 1;
        }
    }

    if let Some(w) = &mut wav {
        w.finish()?;
    }
    if let Some(mut e) = mp3 {
        let tail = e.flush().map_err(|e| format!("mp3 flush: {e}"))?;
        if !tail.is_empty() {
            append_bytes(path, &tail)?;
        }
    }
    Ok(frames)
}

/// Appends MP3 bytes to the output file (the MP3 writer is streaming, unlike
/// the WAV writer which buffers and patches its header at the end).
fn append_bytes(path: &str, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("open {path}: {e}"))?;
    f.write_all(bytes).map_err(|e| format!("write {path}: {e}"))
}

/// Builds an `EgressInfo` reflecting a finished recording.
pub fn finished_info(
    egress_id: &str,
    room_name: &str,
    path: &str,
    request: lk::egress_info::Request,
) -> lk::EgressInfo {
    let now = crate::now_secs();
    lk::EgressInfo {
        egress_id: egress_id.to_string(),
        room_name: room_name.to_string(),
        status: lk::EgressStatus::EgressComplete as i32,
        started_at: now,
        updated_at: now,
        request: Some(request),
        result: Some(lk::egress_info::Result::File(lk::FileInfo {
            filename: path.to_string(),
            started_at: now,
            ended_at: now,
            duration: 0,
            location: String::new(),
            size: 0,
        })),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mixer_reads_fixed_frames() {
        let mut m = Mixer::new();
        m.push("t1", vec![100i16, 200]);
        m.push("t2", vec![50i16, 100, 300]);
        assert_eq!(m.available(), 2);
        let f = m.read_frame(2);
        assert_eq!(f, vec![75, 150]);
    }

    #[tokio::test]
    async fn missing_track_is_silence() {
        let mut m = Mixer::new();
        m.push("t1", vec![100i16]);
        let f = m.read_frame(2);
        assert_eq!(f, vec![100, 0]); // second sample fills with silence
    }
}
