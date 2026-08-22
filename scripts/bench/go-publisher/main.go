// Command publisher streams N seconds of Opus audio into a LiveKit room,
// mirroring the Rust `send_audio` example. It uses the reference Go SDK
// (pion-based), so it interoperates with the Go livekit-server stack that the
// Rust webrtc-rs publisher cannot reach over DTLS-SRTP.
//
// Usage:
//   publisher --ws ws://127.0.0.1:7880 --key devkey --secret secret \
//             --room room-a --seconds 10

package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/livekit/media-sdk"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
)

func main() {
	wsURL := flag.String("ws", "ws://127.0.0.1:7880", "websocket url")
	apiKey := flag.String("key", "devkey", "api key")
	apiSecret := flag.String("secret", "secret", "api secret")
	roomName := flag.String("room", "bench", "room name")
	seconds := flag.Int("seconds", 10, "seconds of audio to stream")
	flag.Parse()

	logger.InitFromConfig(&logger.Config{Level: "error"}, "lkbench")
	lksdk.SetLogger(logger.GetLogger())

	token, err := auth.NewAccessToken(*apiKey, *apiSecret).
		SetIdentity("sender").
		SetVideoGrant(&auth.VideoGrant{
			RoomJoin: true,
			Room:     *roomName,
		}).
		ToJWT()
	if err != nil {
		panic(err)
	}

	room := lksdk.NewRoom(&lksdk.RoomCallback{})
	if err := room.JoinWithToken(*wsURL, token); err != nil {
		panic(err)
	}
	defer room.Disconnect()

	publishTrack, err := lkmedia.NewPCMLocalTrack(lkmedia.DefaultOpusSampleRate, 1, logger.GetLogger())
	if err != nil {
		panic(err)
	}
	defer publishTrack.Close()

	if _, err = room.LocalParticipant.PublishTrack(publishTrack, &lksdk.TrackPublicationOptions{
		Name: "bench-mic",
	}); err != nil {
		panic(err)
	}

	// 20 ms of mono 48 kHz silence per frame.
	silence := media.PCM16Sample(make([]int16, 960))
	frames := *seconds * 50
	for i := 0; i < frames; i++ {
		if err := publishTrack.WriteSample(silence); err != nil {
			panic(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("sent %d frames to room %s\n", frames, *roomName)
	// Stay joined a moment so the recorder drains, then leave.
	time.Sleep(2 * time.Second)
}
