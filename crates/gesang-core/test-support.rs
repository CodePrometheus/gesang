//! Metadata-only preparation fixtures shared by repository integration tests.

use gesang_core::{
    Admission, InstancePhase, InstancePlan, InstancePurpose, Result, Runtime, RuntimeClaim, Store,
};

pub(crate) trait ReadyFixture {
    async fn admit_ready_fixture(
        &self,
        claim: &RuntimeClaim,
        admission: &Admission,
    ) -> Result<Runtime>;
}

impl ReadyFixture for Store {
    async fn admit_ready_fixture(
        &self,
        claim: &RuntimeClaim,
        admission: &Admission,
    ) -> Result<Runtime> {
        let intent = self
            .begin_instance(
                claim,
                &InstancePlan {
                    purpose: InstancePurpose::Execution,
                    provider: "metadata-fixture".into(),
                    schema_version: 1,
                    definition: serde_json::json!({}),
                },
            )
            .await?;
        let admission = Admission {
            runtime_id: intent.id,
            ..admission.clone()
        };
        let intent = self.admit_instance(&intent, &admission).await?;
        let intent = self
            .transition_instance(&intent, InstancePhase::Unlocking)
            .await?;
        let intent = self
            .transition_instance(&intent, InstancePhase::Ready)
            .await?;
        intent.runtime.ok_or(gesang_core::Error::FencingRequired)
    }
}
