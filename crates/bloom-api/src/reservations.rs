use bloom_core::ReservationId;
use bloom_core::capacity::unix_millis;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ReservationError {
    #[error(
        "insufficient capacity: requested {requested} bytes, available {available}, already reserved {reserved}"
    )]
    Insufficient {
        requested: u64,
        available: u64,
        reserved: u64,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Reservation {
    pub id: ReservationId,
    pub memory_bytes: u64,
    pub expires_at_ms: u64,
}

#[derive(Default)]
pub struct ReservationRegistry {
    active: Mutex<HashMap<ReservationId, Reservation>>,
}

impl ReservationRegistry {
    pub fn reserve(
        &self,
        available_bytes: u64,
        requested: u64,
        ttl: Duration,
    ) -> Result<Reservation, ReservationError> {
        let mut active = bloom_core::sync::mutex_lock(&self.active);
        let now = unix_millis();
        active.retain(|_, reservation| reservation.expires_at_ms > now);
        let reserved: u64 = active.values().map(|r| r.memory_bytes).sum();
        if requested > available_bytes.saturating_sub(reserved) {
            return Err(ReservationError::Insufficient {
                requested,
                available: available_bytes,
                reserved,
            });
        }
        let reservation = Reservation {
            id: ReservationId::generate(),
            memory_bytes: requested,
            expires_at_ms: now.saturating_add(ttl.as_millis().try_into().unwrap_or(u64::MAX)),
        };
        active.insert(reservation.id.clone(), reservation.clone());
        Ok(reservation)
    }

    pub fn release(&self, id: &ReservationId) -> bool {
        bloom_core::sync::mutex_lock(&self.active)
            .remove(id)
            .is_some()
    }

    pub fn total_reserved(&self) -> u64 {
        let mut active = bloom_core::sync::mutex_lock(&self.active);
        let now = unix_millis();
        active.retain(|_, reservation| reservation.expires_at_ms > now);
        active.values().map(|r| r.memory_bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_oversubscription() {
        let registry = ReservationRegistry::default();
        registry
            .reserve(1000, 600, Duration::from_secs(5))
            .expect("first reservation fits");
        let error = registry
            .reserve(1000, 600, Duration::from_secs(5))
            .expect_err("second reservation must fail");
        assert!(matches!(error, ReservationError::Insufficient { .. }));
        assert_eq!(registry.total_reserved(), 600);
    }

    #[test]
    fn releases_expired_reservations() {
        let registry = ReservationRegistry::default();
        registry
            .reserve(1000, 1000, Duration::from_millis(10))
            .expect("reservation fits");
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(registry.total_reserved(), 0);
        registry
            .reserve(1000, 1000, Duration::from_secs(5))
            .expect("expired slot is reusable");
    }

    #[test]
    fn unrepresentable_expiry_saturates() {
        let registry = ReservationRegistry::default();
        let reservation = registry
            .reserve(1000, 600, Duration::from_secs(u64::MAX))
            .unwrap();
        assert_eq!(reservation.expires_at_ms, u64::MAX);
        assert_eq!(registry.total_reserved(), 600);
    }
}
