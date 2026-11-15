//! Declaration validation uses the production validator and real reference targets.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{collections::BTreeMap, fs};

use gesang_plugins::{
    BindingScope, InstallationId, InstalledVersion, InterfaceId, ManifestError, PackageVersion,
    PluginManifest, PluginSettings, ServiceEndpoint, ServiceRequirement,
};
use serde_json::json;
use uuid::Uuid;

fn binding() -> BindingScope {
    BindingScope::Deployment {
        deployment_id: Uuid::new_v4(),
    }
}

fn settings() -> PluginSettings {
    PluginSettings {
        manifest: PluginManifest {
            format: 1,
            package: PackageVersion {
                package: "test.provider".into(),
                version: "1".into(),
                digest: "content".into(),
            },
            provides: BTreeMap::from([(
                "records".into(),
                InterfaceId {
                    name: "test.records".into(),
                    version: 1,
                },
            )]),
            requires: BTreeMap::new(),
            configuration: json!({"type":"object","properties":{"port":{"type":"integer","minimum":1}},"required":["port"],"additionalProperties":false}),
            secrets: Default::default(),
        },
        values: json!({"port":1234}),
        dependencies: BTreeMap::new(),
    }
}

#[test]
fn configuration() {
    let mut value = settings();
    value.validate(binding(), &Default::default()).unwrap();
    value.values = json!({"port":"private-value"});
    let error = value.validate(binding(), &Default::default()).unwrap_err();
    assert!(matches!(error, ManifestError::Configuration { ref path } if path == "/port"));
    assert!(!error.to_string().contains("private-value"));
    value.values = json!({"port":1,"extra":true});
    assert!(value.validate(binding(), &Default::default()).is_err());
    value.values = json!({"port":0});
    assert!(value.validate(binding(), &Default::default()).is_err());
    value.values = json!({"port":1});
    value.manifest.configuration = json!({"$defs":{"port":{"type":"integer","minimum":1}},"type":"object","properties":{"port":{"$ref":"#/$defs/port"}},"required":["port"]});
    value.validate(binding(), &Default::default()).unwrap();
}

#[test]
fn declarations() {
    let mut value = settings();
    value.manifest.format = 2;
    assert_eq!(
        value.validate(binding(), &Default::default()),
        Err(ManifestError::Format)
    );
    value.manifest.format = 1;
    value.manifest.provides.get_mut("records").unwrap().version = 0;
    assert_eq!(
        value.validate(binding(), &Default::default()),
        Err(ManifestError::Identity)
    );
    value.manifest.provides.get_mut("records").unwrap().version = 1;
    value.manifest.secrets.insert("token".into());
    assert_eq!(
        value.validate(binding(), &Default::default()),
        Err(ManifestError::Secrets)
    );
    value.validate(binding(), &value.manifest.secrets).unwrap();
}

#[test]
fn schema_references() {
    let directory = std::env::temp_dir().join(format!("gesang-schema-{}", Uuid::new_v4()));
    fs::create_dir(&directory).unwrap();
    let target = directory.join("schema.json");
    fs::write(&target, b"true").unwrap();
    let mut value = settings();
    value.manifest.configuration = json!({"$ref":format!("file://{}",target.display())});
    assert_eq!(
        value.validate(binding(), &Default::default()),
        Err(ManifestError::Schema)
    );
    value.manifest.configuration = json!({"$schema":"http://json-schema.org/draft-07/schema#"});
    assert_eq!(
        value.validate(binding(), &Default::default()),
        Err(ManifestError::Schema)
    );
    value.manifest.configuration = json!({"type":"not-a-type"});
    assert_eq!(
        value.validate(binding(), &Default::default()),
        Err(ManifestError::Schema)
    );
}

#[test]
fn dependency_boundaries() {
    let deployment_id = Uuid::new_v4();
    let scope = BindingScope::Integration {
        deployment_id,
        integration_id: Uuid::new_v4(),
    };
    let mut value = settings();
    value.manifest.requires.insert(
        "source".into(),
        ServiceRequirement {
            interface: InterfaceId {
                name: "test.source".into(),
                version: 1,
            },
            optional: false,
        },
    );
    assert_eq!(
        value.validate(scope, &Default::default()),
        Err(ManifestError::Dependency("source".into()))
    );
    value.manifest.requires.get_mut("source").unwrap().optional = true;
    value.validate(scope, &Default::default()).unwrap();
    let endpoint = ServiceEndpoint {
        provider: InstalledVersion {
            installation: InstallationId {
                binding: BindingScope::Deployment { deployment_id },
                id: Uuid::new_v4(),
            },
            revision: 1,
        },
        service: "source".into(),
    };
    value.dependencies.insert("source".into(), endpoint.clone());
    value.validate(scope, &Default::default()).unwrap();
    value
        .dependencies
        .get_mut("source")
        .unwrap()
        .provider
        .installation
        .binding = binding();
    assert_eq!(
        value.validate(scope, &Default::default()),
        Err(ManifestError::Dependency("source".into()))
    );
    value.dependencies.insert("source".into(), endpoint.clone());
    value.dependencies.insert("undeclared".into(), endpoint);
    assert_eq!(
        value.validate(scope, &Default::default()),
        Err(ManifestError::Dependency("undeclared".into()))
    );
}

#[test]
fn remote_schema() {
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let (stop, stopped) = mpsc::channel();
    let server = std::thread::spawn(move || {
        loop {
            let (mut socket, _) = listener.accept().unwrap();
            if stopped.try_recv().is_ok() {
                break;
            }
            let mut request = BufReader::new(&mut socket);
            loop {
                let mut line = String::new();
                if request.read_line(&mut line).unwrap() == 0 || line == "\r\n" {
                    break;
                }
            }
            count.fetch_add(1, Ordering::SeqCst);
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4\r\nConnection: close\r\n\r\ntrue").unwrap();
        }
    });
    let mut value = settings();
    value.manifest.configuration = json!({"$ref":format!("http://{address}/schema")});
    let result = value.validate(binding(), &Default::default());
    stop.send(()).unwrap();
    TcpStream::connect(address).unwrap();
    server.join().unwrap();
    assert_eq!(
        requests.load(Ordering::SeqCst),
        0,
        "external schema retrieval reached the network"
    );
    assert_eq!(result, Err(ManifestError::Schema));
}
