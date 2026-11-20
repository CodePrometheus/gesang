use std::{
    collections::BTreeMap,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    time::Duration,
};

use gesang_plugins::ServiceEndpoint;
use jiff::Timestamp;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{Error, Result};

/// A pinned local provider identity and an explicit host request deadline.
#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContainerConnection {
    /// Protected local control socket; never mounted in a user sandbox.
    pub socket: PathBuf,
    /// Installation identity observed from the provider before configuration publication.
    pub engine_id: String,
    /// Host-selected deadline; a timeout never proves that a mutation did not happen.
    pub request_timeout_ms: NonZeroU64,
}

/// An administrator-approved ephemeral execution profile, not user-supplied container arguments.
#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContainerProfile {
    /// Exact local image content identity, including the digest algorithm prefix.
    pub image: String,
    /// Executable and arguments interpreted without an intervening host shell.
    pub command: Vec<String>,
    /// Explicit non-root process identity.
    pub uid: NonZeroU32,
    /// Explicit non-root primary group identity.
    pub gid: NonZeroU32,
    /// Host-approved memory budget in bytes.
    pub memory_bytes: NonZeroU64,
    /// Host-approved private scratch-space budget in bytes.
    pub temporary_bytes: NonZeroU64,
    /// Host-approved maximum process count.
    pub pids: NonZeroU32,
    /// Host-approved CPU budget, where 1000 represents one CPU.
    pub cpu_millis: NonZeroU32,
}

impl ContainerProfile {
    /// Checks immutable image identity, process arguments, and explicit host resource policy.
    pub fn validate(&self) -> Result<()> {
        let digest = self
            .image
            .strip_prefix("sha256:")
            .ok_or_else(|| Error::Invalid("an immutable local image digest is required".into()))?;
        if digest.len() != 64
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || self.command.is_empty()
            || self.command[0].is_empty()
            || self.command.iter().any(|part| part.contains('\0'))
            || self.temporary_bytes > self.memory_bytes
            || self.memory_bytes.get() > i64::MAX as u64
        {
            return Err(Error::Invalid("invalid sandbox profile".into()));
        }
        Ok(())
    }
}

/// A provider's current process observation, not proof of namespace destruction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerState {
    /// Allocated without a completed start.
    Created,
    /// The main process is observed running.
    Running,
    /// The main process exited; final removal still supplies the fencing boundary.
    Exited,
    /// The provider cannot establish one of the supported states.
    Unknown,
}

/// The original physical identity and validated process facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalContainer {
    /// Immutable physical identity, never a mutable name alias.
    pub id: ContainerId,
    /// Current observed process state.
    pub state: ContainerState,
    /// Time of the recorded process start, absent when the instance has never started.
    pub started_at: Option<Timestamp>,
}

/// An immutable physical instance identifier; mutable name aliases cannot construct one.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(try_from = "String", into = "String")]
pub struct ContainerId(String);

impl ContainerId {
    /// Returns the canonical provider identifier without allocating another string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ContainerId {
    type Error = Error;

    fn try_from(mut value: String) -> Result<Self> {
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::ProviderData);
        }
        value.make_ascii_lowercase();
        Ok(Self(value))
    }
}

impl From<ContainerId> for String {
    fn from(id: ContainerId) -> Self {
        id.0
    }
}

/// Whether positive provider evidence establishes destruction of the original instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Termination {
    /// A successful removal response or matching destruction event was observed.
    Confirmed,
    /// Evidence is unavailable; this does not permit a replacement writer.
    Unknown,
}

/// An authenticated local runtime connection that refuses identity drift and implicit retries.
#[derive(Clone)]
pub struct ContainerRuntime {
    client: Client,
    connection: ContainerConnection,
    endpoint: ServiceEndpoint,
    base: Url,
}

impl std::fmt::Debug for ContainerRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContainerRuntime")
            .field("engine_id", &self.connection.engine_id)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct EngineInfo {
    #[serde(rename = "ID")]
    id: String,
    #[serde(rename = "SystemTime")]
    time: Timestamp,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ImageInfo {
    id: String,
    config: ImageConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ImageConfig {
    volumes: Option<BTreeMap<String, Value>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Created {
    id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Inspection {
    id: String,
    image: String,
    config: InspectionConfig,
    state: InspectionState,
    host_config: HostConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectionConfig {
    user: String,
    labels: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InspectionState {
    running: bool,
    status: String,
    started_at: Timestamp,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct HostConfig {
    privileged: bool,
    readonly_rootfs: bool,
    network_mode: String,
    cap_drop: Vec<String>,
    security_opt: Vec<String>,
    memory: i64,
    pids_limit: i64,
    #[serde(rename = "NanoCpus")]
    nano_cpus: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Event {
    #[serde(rename = "Type")]
    kind: String,
    action: String,
    actor: EventActor,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct EventActor {
    #[serde(rename = "ID")]
    id: String,
    attributes: BTreeMap<String, String>,
}

impl ContainerRuntime {
    /// Connects to the protected local API and verifies its configured identity.
    ///
    /// The client uses the supplied Unix socket, bypasses environment proxies,
    /// and disables redirects and automatic retries. Clones share the connection
    /// pool. Dropping the client does not stop resources that it created.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] for an incomplete configuration and
    /// [`Error::ProviderIdentity`] if the endpoint identifies another installation.
    /// Transport and protocol failures are preserved as errors.
    #[cfg(unix)]
    pub async fn connect(
        connection: ContainerConnection,
        endpoint: ServiceEndpoint,
    ) -> Result<Self> {
        if connection.engine_id.is_empty() || !connection.socket.is_absolute() {
            return Err(Error::Invalid(
                "an explicit local provider identity is required".into(),
            ));
        }
        endpoint
            .validate()
            .map_err(|_| Error::Invalid("invalid provider endpoint".into()))?;
        let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(Error::Tls)?
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
        let client = Client::builder()
            .use_preconfigured_tls(tls)
            .unix_socket(connection.socket.as_path())
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(Duration::from_millis(connection.request_timeout_ms.get()))
            .build()
            .map_err(Error::Transport)?;
        let runtime = Self {
            client,
            connection,
            endpoint,
            base: Url::parse("http://localhost/v1.56/").map_err(|_| Error::ProviderData)?,
        };
        runtime.identity().await?;
        Ok(runtime)
    }

    /// Verifies the profile against an already installed immutable image.
    ///
    /// Implicit image volumes must be covered by the private scratch mounts.
    /// Validation reads provider metadata without pulling images or allocating
    /// resources.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] for invalid limits, mutable image references, or
    /// unsupported implicit volumes. An unavailable provider or image is an error.
    pub async fn validate_profile(&self, profile: &ContainerProfile) -> Result<()> {
        profile.validate()?;
        self.identity().await?;
        let response = self
            .client
            .get(self.url(&["images", &profile.image, "json"])?)
            .send()
            .await
            .map_err(Error::Transport)?;
        if response.status() != StatusCode::OK {
            return Err(Error::ProviderStatus {
                status: response.status(),
            });
        }
        let image: ImageInfo = response.json().await.map_err(Error::Transport)?;
        if image.id != profile.image
            || image.config.volumes.as_ref().is_some_and(|volumes| {
                volumes
                    .keys()
                    .any(|path| path != "/tmp" && path != "/workspace")
            })
        {
            return Err(Error::Invalid(
                "image identity or implicit volume declarations do not match the profile".into(),
            ));
        }
        Ok(())
    }

    /// Allocates the original instance under its stable resource identity.
    ///
    /// A new instance is stopped. If the same instance already exists, its labels,
    /// image, identity, and isolation settings must match before it can be adopted.
    /// The caller must persist admission and issuance before polling this future.
    ///
    /// # Errors
    ///
    /// Transport errors and unexpected responses leave the allocation outcome
    /// uncertain. [`Error::ResourceIdentity`] rejects a conflicting physical
    /// instance without altering it.
    ///
    /// # Cancel safety
    ///
    /// Cancelling the future does not prove that allocation stopped. Reconcile
    /// with [`Self::find`] using the original identity. A missing observation does
    /// not authorize another allocation attempt.
    pub async fn allocate(
        &self,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<PhysicalContainer> {
        profile.validate()?;
        self.identity().await?;
        let labels = self.labels(resource, profile)?;
        let scratch = format!(
            "rw,nosuid,nodev,uid={},gid={},mode=0700,size={}",
            profile.uid, profile.gid, profile.temporary_bytes
        );
        let body = json!({"Image":profile.image,"Entrypoint":profile.command,"Cmd":[],"User":format!("{}:{}",profile.uid,profile.gid),
            "WorkingDir":"/workspace","Labels":labels,"Env":[],"HostConfig":{"NetworkMode":"none","Privileged":false,
            "ReadonlyRootfs":true,"CapDrop":["ALL"],"SecurityOpt":["no-new-privileges:true"],"Init":true,
            "Memory":profile.memory_bytes.get(),"MemorySwap":profile.memory_bytes.get(),"PidsLimit":profile.pids.get(),
            "NanoCpus":u64::from(profile.cpu_millis.get())*1_000_000,"RestartPolicy":{"Name":"no"},
            "Tmpfs":{"/tmp":scratch,"/workspace":scratch}}});
        let response = self
            .client
            .post(self.url(&["containers", "create"])?)
            .query(&[("name", name(resource))])
            .json(&body)
            .send()
            .await
            .map_err(Error::Transport)?;
        match response.status() {
            StatusCode::CREATED => {
                let created: Created = response.json().await.map_err(Error::Transport)?;
                let id = ContainerId::try_from(created.id)?;
                self.inspect(id.as_str(), resource, profile)
                    .await?
                    .ok_or(Error::Uncertain)
            }
            StatusCode::CONFLICT => self.find(resource, profile).await?.ok_or(Error::Uncertain),
            status => Err(Error::ProviderStatus { status }),
        }
    }

    /// Looks up an issued allocation by its stable name and immutable labels.
    ///
    /// `None` means that this lookup found no instance. It does not prove that an
    /// earlier allocation was never accepted or that an in-flight request stopped.
    /// This method never creates or starts an instance.
    pub async fn find(
        &self,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<Option<PhysicalContainer>> {
        self.inspect(&name(resource), resource, profile).await
    }

    /// Observes the original physical instance and verifies its resource binding.
    ///
    /// Only [`ContainerId`] is accepted, so a mutable name cannot redirect an
    /// observation to a replacement instance. `None` is not termination evidence;
    /// use [`Self::terminated`] to reconcile an uncertain removal.
    pub async fn observe(
        &self,
        id: &ContainerId,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<Option<PhysicalContainer>> {
        self.inspect(id.as_str(), resource, profile).await
    }

    /// Starts the original physical instance after durable service admission.
    ///
    /// The caller must record issuance before polling this future. The returned
    /// observation describes the process, not application readiness or goal success.
    ///
    /// # Errors
    ///
    /// Identity mismatches prevent the request from being sent. Transport failure
    /// or an unavailable final observation leaves the start outcome uncertain.
    ///
    /// # Cancel safety
    ///
    /// A cancelled request may have reached the provider. Reconcile the original
    /// instance's recorded start; do not replay the start solely because its reply
    /// was lost.
    pub async fn start(
        &self,
        id: &ContainerId,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<PhysicalContainer> {
        self.inspect(id.as_str(), resource, profile)
            .await?
            .ok_or(Error::Uncertain)?;
        let response = self
            .client
            .post(self.url(&["containers", id.as_str(), "start"])?)
            .send()
            .await
            .map_err(Error::Transport)?;
        if !matches!(
            response.status(),
            StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED
        ) {
            return Err(Error::ProviderStatus {
                status: response.status(),
            });
        }
        self.inspect(id.as_str(), resource, profile)
            .await?
            .ok_or(Error::Uncertain)
    }

    /// Terminates the recorded compute instance while retaining attached volumes.
    ///
    /// The resource binding is verified before removal. Confirmation requires a
    /// successful removal response or a matching destruction event. Missing state
    /// alone returns [`Termination::Unknown`].
    ///
    /// # Errors
    ///
    /// [`Error::ResourceIdentity`] prevents removal of another instance. Transport
    /// errors leave the original removal outcome uncertain.
    ///
    /// # Cancel safety
    ///
    /// Cancellation does not stop an issued removal. Keep the original physical
    /// identity and reconcile it with [`Self::terminated`]; a new instance must
    /// never be substituted as the removal target.
    pub async fn terminate(
        &self,
        id: &ContainerId,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<Termination> {
        if self
            .inspect(id.as_str(), resource, profile)
            .await?
            .is_none()
        {
            return self.terminated(id, resource, profile).await;
        }
        let response = self
            .client
            .delete(self.url(&["containers", id.as_str()])?)
            .query(&[("force", "true"), ("v", "false")])
            .send()
            .await
            .map_err(Error::Transport)?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(Termination::Confirmed);
        }
        self.terminated(id, resource, profile).await
    }

    /// Reads retained destruction evidence for the original physical instance.
    ///
    /// The read ends at the provider's current timestamp and checks all matching
    /// retained events. Event retention is provider-defined. If the matching event
    /// is unavailable, the result is [`Termination::Unknown`], including when an
    /// ordinary instance lookup returns no record.
    ///
    /// This method performs no resource mutation and can safely be used to
    /// reconcile a removal after caller cancellation or a lost response.
    pub async fn terminated(
        &self,
        id: &ContainerId,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<Termination> {
        let info = self.identity().await?;
        let nanos = info.time.as_nanosecond();
        let seconds = nanos.div_euclid(1_000_000_000);
        let fraction = nanos.rem_euclid(1_000_000_000);
        let until = format!("{seconds}.{fraction:09}");
        let filters =
            json!({"container":[id],"event":["destroy"],"type":["container"]}).to_string();
        let response = self
            .client
            .get(self.url(&["events"])?)
            .query(&[
                ("since", "0"),
                ("until", until.as_str()),
                ("filters", filters.as_str()),
            ])
            .send()
            .await
            .map_err(Error::Transport)?;
        if response.status() != StatusCode::OK {
            return Err(Error::ProviderStatus {
                status: response.status(),
            });
        }
        let data = response.bytes().await.map_err(Error::Transport)?;
        let labels = self.labels(resource, profile)?;
        for event in serde_json::Deserializer::from_slice(&data).into_iter::<Event>() {
            let event = event?;
            if event.kind == "container"
                && event.action == "destroy"
                && event.actor.id == id.as_str()
                && labels
                    .iter()
                    .all(|(key, value)| event.actor.attributes.get(key) == Some(value))
            {
                return Ok(Termination::Confirmed);
            }
        }
        Ok(Termination::Unknown)
    }

    async fn identity(&self) -> Result<EngineInfo> {
        let response = self
            .client
            .get(self.url(&["info"])?)
            .send()
            .await
            .map_err(Error::Transport)?;
        if response.status() != StatusCode::OK {
            return Err(Error::ProviderStatus {
                status: response.status(),
            });
        }
        let info: EngineInfo = response.json().await.map_err(Error::Transport)?;
        if info.id != self.connection.engine_id {
            return Err(Error::ProviderIdentity);
        }
        Ok(info)
    }

    async fn inspect(
        &self,
        id: &str,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<Option<PhysicalContainer>> {
        self.identity().await?;
        let response = self
            .client
            .get(self.url(&["containers", id, "json"])?)
            .send()
            .await
            .map_err(Error::Transport)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() != StatusCode::OK {
            return Err(Error::ProviderStatus {
                status: response.status(),
            });
        }
        let info: Inspection = response.json().await.map_err(Error::Transport)?;
        let physical_id = ContainerId::try_from(info.id)?;
        if info.image != profile.image
            || info.config.user != format!("{}:{}", profile.uid, profile.gid)
            || !self
                .labels(resource, profile)?
                .iter()
                .all(|(key, value)| info.config.labels.get(key) == Some(value))
            || info.host_config.privileged
            || !info.host_config.readonly_rootfs
            || info.host_config.network_mode != "none"
            || !info.host_config.cap_drop.iter().any(|value| value == "ALL")
            || !info
                .host_config
                .security_opt
                .iter()
                .any(|value| value == "no-new-privileges:true")
            || info.host_config.memory != profile.memory_bytes.get() as i64
            || info.host_config.pids_limit != i64::from(profile.pids.get())
            || info.host_config.nano_cpus != i64::from(profile.cpu_millis.get()) * 1_000_000
        {
            return Err(Error::ResourceIdentity);
        }
        let state = match (info.state.status.as_str(), info.state.running) {
            ("running", true) => ContainerState::Running,
            ("created", false) => ContainerState::Created,
            ("exited", false) => ContainerState::Exited,
            _ => ContainerState::Unknown,
        };
        let unstarted = "0001-01-01T00:00:00Z"
            .parse::<Timestamp>()
            .map_err(|_| Error::ProviderData)?;
        Ok(Some(PhysicalContainer {
            id: physical_id,
            state,
            started_at: (info.state.started_at != unstarted).then_some(info.state.started_at),
        }))
    }

    fn labels(
        &self,
        resource: Uuid,
        profile: &ContainerProfile,
    ) -> Result<BTreeMap<String, String>> {
        let data = serde_json::to_vec(&(&self.endpoint, profile))?;
        let fingerprint = Sha256::digest(&data)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(BTreeMap::from([
            ("gesang.resource".into(), resource.to_string()),
            ("gesang.definition".into(), fingerprint),
        ]))
    }

    fn url(&self, segments: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        let mut path = url.path_segments_mut().map_err(|_| Error::ProviderData)?;
        path.pop_if_empty();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }
}

fn name(resource: Uuid) -> String {
    format!("gesang-{}", resource.simple())
}
