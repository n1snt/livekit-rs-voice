use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir =
        std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protos"))?;

    let files = [
        "livekit_models.proto",
        "livekit_metrics.proto",
        "livekit_agent.proto",
        "livekit_agent_dispatch.proto",
        "livekit_egress.proto",
        "livekit_room.proto",
        "livekit_rtc.proto",
        "livekit_sip.proto",
        "livekit_ingress.proto",
        "livekit_webhook.proto",
        "internal.proto",
        "rpc/sip.proto",
        "rpc/io.proto",
        "rpc/egress.proto",
    ];
    let files: Vec<String> = files
        .iter()
        .map(|f| proto_dir.join(f).display().to_string())
        .collect();

    println!("cargo:rerun-if-changed={}", proto_dir.display());

    let fds = protox::compile(files, [proto_dir])
        .map_err(|e| format!("protox::compile failed: {e:?}"))?;

    let mut config = prost_build::Config::new();
    config.btree_map(["."]);
    config.compile_well_known_types();
    config.disable_comments(["."]);
    config
        .compile_fds(fds.clone())
        .map_err(|e| format!("prost compile_fds failed: {e:?}"))?;

    let mut b = pbjson_build::Builder::new();
    b.btree_map(["."]);
    b.exclude([
        ".google.protobuf.Timestamp",
        ".google.protobuf.Duration",
        ".google.protobuf.Any",
        ".google.protobuf.Empty",
        ".google.protobuf.Value",
        ".google.protobuf.Struct",
        ".google.protobuf.ListValue",
        ".google.protobuf.NullValue",
        ".google.protobuf.FieldMask",
        ".google.protobuf.BoolValue",
        ".google.protobuf.BytesValue",
        ".google.protobuf.DoubleValue",
        ".google.protobuf.FloatValue",
        ".google.protobuf.Int32Value",
        ".google.protobuf.Int64Value",
        ".google.protobuf.StringValue",
        ".google.protobuf.UInt32Value",
        ".google.protobuf.UInt64Value",
    ]);
    let mut fds_bytes = Vec::new();
    prost::Message::encode(&fds, &mut fds_bytes)
        .map_err(|e| format!("fds encode failed: {e:?}"))?;
    b.register_descriptors(&fds_bytes)
        .map_err(|e| format!("register_descriptors failed: {e:?}"))?;
    b.build(&[".livekit", ".google", ".internal", ".rpc"])
        .map_err(|e| format!("pbjson build failed: {e:?}"))?;

    Ok(())
}
