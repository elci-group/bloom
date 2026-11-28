use core::fmt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn generate() -> Self {
                Self(Uuid::new_v4().to_string())
            }

            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }
    };
}

string_id!(
    /// Stable cryptographic-ish identity of a participating node.
    NodeId
);
string_id!(
    /// Identity of a model within the distributed catalogue.
    ModelId
);
string_id!(
    /// Identity of a single inference request as dispatched to a harness.
    RequestId
);
string_id!(
    /// Identity of a ledger-tracked inference job.
    JobId
);
string_id!(
    /// Identity of an atomic resource reservation.
    ReservationId
);
string_id!(
    /// Identity of an execution-slot hold granted to a router before it forwards a prompt.
    AdmissionId
);
