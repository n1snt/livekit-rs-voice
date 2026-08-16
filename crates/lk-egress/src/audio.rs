//! Audio pipeline: Opus decode (via libopus) and PCM mixing for
//! room-composite recording.

use audiopus::coder::Decoder;
use audiopus::packet::Packet;
use audiopus::{Channels, MutSignals, SampleRate};

/// A decoded PCM16 (mono, 48 kHz) audio frame.
pub const SAMPLE_RATE: u32 = 48000;

/// Decodes one Opus RTP payload into 16-bit PCM mono.
pub fn decode_opus(payload: &[u8]) -> Result<Vec<i16>, String> {
    let mut decoder = Decoder::new(SampleRate::Hz48000, Channels::Mono)
        .map_err(|e| format!("opus decoder: {e}"))?;
    let packet = Packet::try_from(payload).map_err(|e| format!("opus packet: {e}"))?;
    let mut out = vec![0i16; 5760];
    let signals =
        MutSignals::try_from(out.as_mut_slice()).map_err(|e| format!("opus output: {e}"))?;
    let n = decoder
        .decode(Some(packet), signals, false)
        .map_err(|e| format!("opus decode: {e}"))?;
    out.truncate(n);
    Ok(out)
}

/// Sums (with clipping) multiple same-length mono PCM frames into one mixed
/// frame. Returns an empty vec when there are no frames.
pub fn mix_frames(frames: &[&[i16]]) -> Vec<i16> {
    let Some(len) = frames.iter().map(|f| f.len()).max() else {
        return Vec::new();
    };
    if len == 0 {
        return Vec::new();
    }
    let count = frames.len().max(1);
    let mut out = vec![0i16; len];
    for (i, slot) in out.iter_mut().enumerate() {
        let mut sum: i32 = 0;
        for f in frames {
            sum += f.get(i).copied().unwrap_or(0) as i32;
        }
        // Average to avoid clipping on overload, then soft clamp.
        *slot = (sum / count as i32).clamp(i16::MIN as i32, i16::MAX as i32) as i16;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix_averages_and_clips() {
        let a = [1000i16, 30000, -1000];
        let b = [2000i16, -30000, 1000];
        let m = mix_frames(&[&a, &b]);
        assert_eq!(m[0], 1500);
        assert_eq!(m[1], 0);
        assert_eq!(m[2], 0);
    }

    #[test]
    fn mix_single_frame_passthrough() {
        let a = [5i16, -5];
        let m = mix_frames(&[&a]);
        assert_eq!(m, a);
    }
}
