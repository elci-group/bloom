mod disabled;
pub mod error;
pub mod mock;
pub mod types;

pub use disabled::DisabledHarness;
pub use error::HarnessError;
pub use mock::MockHarness;
pub use types::{
    GenerationParameters, HarnessHealth, InferenceRequest, InferenceStream, InventoryModel,
    ModelCapability, ModelInventory, ResourceSnapshot, StreamEvent,
};

use async_trait::async_trait;
use bloom_core::{ModelId, RequestId};

#[async_trait]
pub trait InferenceHarness: Send + Sync {
    fn kind(&self) -> &'static str;

    async fn start(&self) -> Result<(), HarnessError>;

    async fn stop(&self) -> Result<(), HarnessError>;

    async fn health(&self) -> HarnessHealth;

    async fn inventory(&self) -> ModelInventory;

    async fn resources(&self) -> ResourceSnapshot;

    async fn load_model(&self, model: ModelId) -> Result<(), HarnessError>;

    async fn unload_model(&self, model: ModelId) -> Result<(), HarnessError> {
        let _ = model;
        Err(HarnessError::NotSupported("unload_model".to_string()))
    }

    async fn infer(&self, request: InferenceRequest) -> Result<InferenceStream, HarnessError>;

    async fn cancel(&self, id: RequestId) -> Result<(), HarnessError>;
}
