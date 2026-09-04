//! Recording pipeline: consume the room audio stream, decode Opus, mix all
//! tracks, and write WAV or MP3.

use std::collections::{HashMap, VecDeque};

use lk_proto::livekit as lk;
use tokio::sync::mpsc;

use crate::audio::{OpusDecoder, SAMPLE_RATE};
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
    last_packet: HashMap<String, std::time::Instant>,
}

/// A track with no packets for this long is considered gone and is pruned
/// (DTX silence, a participant that stopped, etc.).
const SILENT_TRACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

impl Mixer {
    fn new() -> Self {
        Mixer {
            tracks: HashMap::new(),
            last_packet: HashMap::new(),
        }
    }

    fn push(&mut self, cid: &str, pcm: Vec<i16>) {
        self.tracks.entry(cid.to_string()).or_default().extend(pcm);
        self.last_packet
            .insert(cid.to_string(), std::time::Instant::now());
    }

    /// Drops tracks that have not produced a packet for `SILENT_TRACK_TIMEOUT`
    /// (a stalled/DTX track must not keep other tracks' queues growing
    /// unboundedly). Returns the pruned track ids so decoders can be dropped.
    fn prune_stale(&mut self) -> Vec<String> {
        let now = std::time::Instant::now();
        let stale: Vec<String> = self
            .last_packet
            .iter()
            .filter(|(_, t)| now.duration_since(**t) > SILENT_TRACK_TIMEOUT)
            .map(|(k, _)| k.clone())
            .collect();
        for cid in &stale {
            self.tracks.remove(cid);
            self.last_packet.remove(cid);
        }
        stale
    }

    /// Samples available to mix: the largest queue, so any track with a full
    /// frame triggers a drain. Tracks with fewer samples are padded with
    /// silence, which a silent/DTX track can no longer prevent.
    fn available(&self) -> usize {
        self.tracks.values().map(|q| q.len()).max().unwrap_or(0)
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
    mut cancel: tokio::sync::watch::Receiver<bool>,
) -> Result<u64, String> {
    let mut mixer = Mixer::new();
    let mut decoders: HashMap<String, OpusDecoder> = HashMap::new();
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

    loop {
        let packet = tokio::select! {
            p = audio.recv() => p,
            _ = cancel.changed() => None,
        };
        let Some(packet) = packet else { break };
        let decoder = decoders
            .entry(packet.track_cid.clone())
            .or_insert_with(|| OpusDecoder::new().expect("opus decoder"));
        match decoder.decode(&packet.payload) {
            Ok(pcm) => {
                mixer.push(&packet.track_cid, pcm);
            }
            Err(e) => {
                tracing::debug!(len = packet.payload.len(), "opus decode failed: {e}");
                continue;
            }
        }
        // Prune tracks that stopped sending (DTX/pause) so a silent track can
        // neither stall the mix nor leak decoder/queue memory.
        for cid in mixer.prune_stale() {
            decoders.remove(&cid);
        }
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

/// Builds an `EgressInfo` reflecting a finished recording. Populates both the
/// non-deprecated `file_results` and the legacy `result` oneof (clients read
/// `file_results`, matching the reference). `filename` is the storage key and
/// `location` the uploaded URL (or local path when not uploaded).
#[allow(clippy::too_many_arguments)]
pub fn finished_info(
    egress_id: &str,
    room_id: &str,
    room_name: &str,
    filename: &str,
    location: &str,
    request: lk::egress_info::Request,
    frames: u64,
    size: u64,
) -> lk::EgressInfo {
    let now = crate::now_nanos();
    let file = lk::FileInfo {
        filename: filename.to_string(),
        started_at: now,
        ended_at: now,
        duration: (frames * 20) as i64,
        location: location.to_string(),
        size: size as i64,
    };
    lk::EgressInfo {
        egress_id: egress_id.to_string(),
        room_id: room_id.to_string(),
        room_name: room_name.to_string(),
        status: lk::EgressStatus::EgressComplete as i32,
        started_at: now,
        ended_at: now,
        updated_at: now,
        request: Some(request),
        result: Some(lk::egress_info::Result::File(file.clone())),
        file_results: vec![file],
        source_type: lk::EgressSourceType::Sdk as i32,
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
        // available() is the largest queue: any track with a full frame drains.
        assert_eq!(m.available(), 3);
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

    #[tokio::test]
    async fn silent_track_does_not_block_mix_and_is_pruned() {
        let mut m = Mixer::new();
        m.push("t1", vec![100i16, 200]);
        m.push("t2", vec![50i16]);
        // t2 has fewer samples: draining is driven by t1's larger queue.
        assert_eq!(m.available(), 2);
        let f = m.read_frame(2);
        // t1 contributes 100,200; t2 contributes 50 then silence.
        assert_eq!(f, vec![75, 100]);

        // A track that stops sending is pruned after the silence timeout.
        m.last_packet.insert(
            "t2".to_string(),
            std::time::Instant::now() - SILENT_TRACK_TIMEOUT - std::time::Duration::from_secs(1),
        );
        let pruned = m.prune_stale();
        assert_eq!(pruned, vec!["t2".to_string()]);
        assert_eq!(m.tracks.len(), 1);
        assert!(!m.tracks.contains_key("t2"));
    }
}
