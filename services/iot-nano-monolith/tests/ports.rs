use std::{
    fs,
    path::{Path, PathBuf},
};

fn rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("library source directory must be readable") {
        let entry = entry.expect("library source entry must be readable");
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}

#[test]
fn typed_library_ports_exclude_retired_http_boundaries() {
    let services = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("monolith package must live below services");
    let mut sources = Vec::new();
    for package in [
        "iot-nano-api",
        "iot-nano-core",
        "iot-nano-stream",
        "iot-nano-mqttd",
    ] {
        rust_sources(&services.join(package).join("src"), &mut sources);
    }

    for source in sources {
        let text = fs::read_to_string(&source).expect("library source must be readable");
        for retired in [
            "reqwest::Client",
            "/internal/",
            "x-iot-nano-",
            "HttpStreamConsumer",
            "HttpTransportRpcClient",
        ] {
            assert!(
                !text.contains(retired),
                "retired HTTP boundary {retired:?} remains in {}",
                source.display()
            );
        }
        assert!(
            !text.contains("iot_nano_monolith"),
            "library imports the composition root: {}",
            source.display()
        );
    }
}
