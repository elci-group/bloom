use crate::{
    HarnessError, HarnessHealth, InferenceHarness, InferenceRequest, InferenceStream,
    ModelInventory, ResourceSnapshot,
};
use async_trait::async_trait;
use bloom_core::{ModelId, RequestId};

pub struct DisabledHarness;

const DETAIL: &str = "local inference is disabled";

#[async_trait]
impl InferenceHarness for DisabledHarness {
    fn kind(&self) -> &'static str {
        "disabled"
    }

    async fn start(&self) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), HarnessError> {
        Ok(())
    }

    async fn health(&self) -> HarnessHealth {
        HarnessHealth::down(DETAIL)
    }

    async fn inventory(&self) -> ModelInventory {
        ModelInventory::default()
    }

    async fn resources(&self) -> ResourceSnapshot {
        ResourceSnapshot::default()
    }

    async fn load_model(&self, _: ModelId) -> Result<(), HarnessError> {
        Err(HarnessError::Unavailable(DETAIL.into()))
    }

    async fn infer(&self, _: InferenceRequest) -> Result<InferenceStream, HarnessError> {
        Err(HarnessError::Unavailable(DETAIL.into()))
    }

    async fn cancel(&self, _: RequestId) -> Result<(), HarnessError> {
        Ok(())
    }
}
