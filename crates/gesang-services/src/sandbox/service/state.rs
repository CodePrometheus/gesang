use gesang_plugins::ServiceEndpoint;
use sqlx::types::Json;
use uuid::Uuid;

use crate::{Error, Result, access::StoredCaller};

use super::super::{ContainerId, OperationStatus, SandboxOperation};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Action {
    Create,
    Stop,
}

impl Action {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Stop => "stop",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Launch {
    Queued,
    Issued,
    Allocated,
    Starting,
    Started,
    Failed,
}

impl Launch {
    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "issued" => Ok(Self::Issued),
            "allocated" => Ok(Self::Allocated),
            "starting" => Ok(Self::Starting),
            "started" => Ok(Self::Started),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::InvalidState),
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Issued => "issued",
            Self::Allocated => "allocated",
            Self::Starting => "starting",
            Self::Started => "started",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Removal {
    None,
    Requested,
    Issued,
    Stopped,
}

impl Removal {
    pub(super) fn parse(value: &str) -> Result<Self> {
        match value {
            "none" => Ok(Self::None),
            "requested" => Ok(Self::Requested),
            "issued" => Ok(Self::Issued),
            "stopped" => Ok(Self::Stopped),
            _ => Err(Error::InvalidState),
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Requested => "requested",
            Self::Issued => "issued",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug)]
pub(super) struct Instance {
    pub id: Uuid,
    pub endpoint: ServiceEndpoint,
    pub profile: String,
    pub creator: StoredCaller,
    pub launch: Launch,
    pub physical_id: Option<ContainerId>,
    pub stop_requested: bool,
    pub removal: Removal,
    pub stopper: Option<StoredCaller>,
}

pub(super) struct InstanceRow {
    pub id: Uuid,
    pub endpoint: Json<ServiceEndpoint>,
    pub profile: String,
    pub creator: Json<StoredCaller>,
    pub launch: String,
    pub physical_id: Option<String>,
    pub stop_requested: bool,
    pub removal: String,
    pub stopper: Option<Json<StoredCaller>>,
}

impl TryFrom<InstanceRow> for Instance {
    type Error = Error;

    fn try_from(row: InstanceRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            endpoint: row.endpoint.0,
            profile: row.profile,
            creator: row.creator.0,
            launch: Launch::parse(&row.launch)?,
            physical_id: row.physical_id.map(ContainerId::try_from).transpose()?,
            stop_requested: row.stop_requested,
            removal: Removal::parse(&row.removal)?,
            stopper: row.stopper.map(|caller| caller.0),
        })
    }
}

pub(super) struct OperationRow {
    pub resource_id: Uuid,
    pub request_id: Uuid,
    pub action: String,
    pub status: String,
    pub problem: Option<String>,
    pub caller: Json<StoredCaller>,
}

impl TryFrom<OperationRow> for SandboxOperation {
    type Error = Error;

    fn try_from(row: OperationRow) -> Result<Self> {
        let status = match row.status.as_str() {
            "accepted" => OperationStatus::Accepted,
            "succeeded" => OperationStatus::Succeeded,
            "failed" => OperationStatus::Failed,
            "unknown" => OperationStatus::Unknown,
            _ => return Err(Error::InvalidState),
        };
        Ok(Self {
            resource_id: row.resource_id,
            request_id: row.request_id,
            status,
            problem: row.problem,
        })
    }
}
