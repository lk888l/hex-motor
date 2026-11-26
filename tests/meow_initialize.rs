use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use can_transport::{CanBus, CanCapabilities, CanFilter, CanFrame, CanIoError, CanRx};
use hex_motor::canopen::nmt::NmtState;
use hex_motor::cia402::PdoProfile;
use hex_motor::meow_motor::{
    MeowMotorInitializeOptions, MeowMotorLifecycle, MeowMotorLogic, MeowMotorManager,
    MeowMotorManagerOptions, PRODUCT_CODE_4310, VENDOR_ID,
};
use hex_motor::Error;
use tokio::sync::mpsc;

struct TestBus {
    nmt: Mutex<[NmtState; 3]>,
    writes: Mutex<HashMap<(u8, u16, u8), Vec<u8>>>,
    reject_disable_once: AtomicBool,
    subscribers: Mutex<Vec<(CanFilter, mpsc::UnboundedSender<CanFrame>)>>,
}

impl TestBus {
    fn publish(&self, cob_id: u16, data: &[u8]) {
        let frame = CanFrame::new_data(cob_id, data).unwrap();
        self.subscribers.lock().unwrap().retain(|(filter, tx)| {
            !tx.is_closed() && (!filter.matches(&frame) || tx.send(frame).is_ok())
        });
    }

    async fn feedback(&self) {
        let mut tick = tokio::time::interval(Duration::from_millis(4));
        let mut timestamp = 0u16;
        loop {
            tick.tick().await;
            timestamp = timestamp.wrapping_add(4000);
            let states = *self.nmt.lock().unwrap();
            for (node, state) in (1u16..).zip(states) {
                self.publish(0x700 + node, &[state as u8]);
                if state == NmtState::Operational {
                    let mut position = [0; 8];
                    position[4..6].copy_from_slice(&timestamp.to_le_bytes());
                    self.publish(0x180 + node, &position);
                    self.publish(0x280 + node, &[0; 7]); // mode display = Disabled
                }
            }
        }
    }

    fn read_object(&self, node: u8, index: u16, sub: u8) -> Vec<u8> {
        if let Some(value) = self.writes.lock().unwrap().get(&(node, index, sub)) {
            return value.clone();
        }
        match (index, sub) {
            (0x1018, 1) => VENDOR_ID.to_le_bytes().to_vec(),
            (0x1018, 2) => PRODUCT_CODE_4310.to_le_bytes().to_vec(),
            (0x1018, 3 | 4) => u32::from(node).to_le_bytes().to_vec(),
            (0x1008, 0) => b"test".to_vec(),
            (0x2001, 0) => vec![4],
            (0x2001, 1) => vec![node],
            (0x2001, 2) => 1_000_000u32.to_le_bytes().to_vec(),
            (0x2001, 3) => 5_000_000u32.to_le_bytes().to_vec(),
            (0x2001, 4) | (0x4402, 0) => vec![0],
            (0x1800 | 0x1801, 1) => vec![0; 4], // force full initial TPDO configuration
            (0x4576, 0) | (0x4102, 7) => 1f32.to_le_bytes().to_vec(),
            _ => panic!("unexpected upload from node {node} at {index:#x}:{sub}"),
        }
    }

    fn respond_to_sdo(&self, node: u8, data: &[u8]) {
        let index = u16::from_le_bytes([data[1], data[2]]);
        let sub = data[3];
        let mut reply = [0; 8];
        reply[1..4].copy_from_slice(&data[1..4]);
        if data[0] == 0x40 {
            let value = self.read_object(node, index, sub);
            reply[0] = 0x43 | ((4 - value.len() as u8) << 2);
            reply[4..4 + value.len()].copy_from_slice(&value);
        } else if node == 2
            && index == 0x4401
            && self.reject_disable_once.swap(false, Ordering::Relaxed)
        {
            reply[0] = 0x80;
            reply[4..8].copy_from_slice(&0x0800_0020u32.to_le_bytes());
        } else {
            assert!(matches!(data[0], 0x23 | 0x27 | 0x2b | 0x2f));
            let len = 4 - usize::from((data[0] >> 2) & 3);
            self.writes
                .lock()
                .unwrap()
                .insert((node, index, sub), data[4..4 + len].to_vec());
            reply[0] = 0x60;
        }
        self.publish(0x580 + u16::from(node), &reply);
    }
}

struct Receiver(mpsc::UnboundedReceiver<CanFrame>);

#[async_trait]
impl CanRx for Receiver {
    async fn recv(&mut self) -> Result<CanFrame, CanIoError> {
        self.0.recv().await.ok_or(CanIoError::Disconnected)
    }

    fn try_recv(&mut self) -> Result<Option<CanFrame>, CanIoError> {
        match self.0.try_recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(CanIoError::Disconnected),
        }
    }
}

#[async_trait]
impl CanBus for TestBus {
    async fn send(&self, frame: CanFrame) -> Result<(), CanIoError> {
        match frame.id().raw() {
            0 => {
                let state = match frame.data()[0] {
                    0x80 => NmtState::PreOperational,
                    0x01 => NmtState::Operational,
                    other => panic!("unexpected NMT command {other:#x}"),
                };
                self.nmt.lock().unwrap()[usize::from(frame.data()[1]) - 1] = state;
            }
            id @ 0x601..=0x603 => self.respond_to_sdo((id - 0x600) as u8, frame.data()),
            id => panic!("unexpected COB-ID {id:#x}"),
        }
        Ok(())
    }

    async fn subscribe(&self, filter: CanFilter) -> Result<Box<dyn CanRx>, CanIoError> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.subscribers.lock().unwrap().push((filter, tx));
        Ok(Box::new(Receiver(rx)))
    }

    fn capabilities(&self) -> CanCapabilities {
        CanCapabilities {
            fd: true,
            max_dlen: 64,
        }
    }
}

#[tokio::test]
async fn healthy_nodes_finish_with_their_own_settings_when_a_peer_aborts() {
    let bus = Arc::new(TestBus {
        nmt: Mutex::new([NmtState::PreOperational; 3]),
        writes: Default::default(),
        reject_disable_once: AtomicBool::new(true),
        subscribers: Default::default(),
    });
    let manager = MeowMotorManager::new(
        bus.clone(),
        MeowMotorManagerOptions {
            auto_identify: false,
            broadcast_heartbeat: false,
            ..Default::default()
        },
    )
    .unwrap();
    let run = async {
        for node in 1..=3 {
            manager.identify(node).await.unwrap();
            manager
                .subscribe_status(node)
                .unwrap()
                .wait_for(|s| s.connection.online)
                .await
                .unwrap();
        }
        let defaults = MeowMotorInitializeOptions::default();
        let custom = MeowMotorInitializeOptions {
            pdo_profile: PdoProfile::from_event_timer_ms(4).unwrap(),
            configure_consumer_heartbeat: false,
            ..defaults
        };
        let results = manager
            .initialize([(3, custom), (2, defaults), (1, defaults)])
            .await;
        assert_eq!(
            manager.status(2).unwrap().lifecycle,
            MeowMotorLifecycle::Identified
        );
        let retry = manager.initialize([2]).await;
        (results, retry)
    };
    let (results, retry) = tokio::select! {
        _ = bus.feedback() => unreachable!(),
        result = tokio::time::timeout(Duration::from_secs(5), run) => result.unwrap(),
    };
    assert!(
        matches!(
            results.as_slice(),
            [(1, Ok(())), (2, Err(Error::Sdo(_))), (3, Ok(()))]
        ),
        "{results:?}"
    );
    assert!(matches!(retry.as_slice(), [(2, Ok(()))]), "{retry:?}");
    for node in [1, 2, 3] {
        let state = manager.status(node).unwrap();
        assert_eq!(state.lifecycle, MeowMotorLifecycle::Initialized);
        assert_eq!(state.logic, Some(MeowMotorLogic::Disabled));
        assert!(state.measurements.tpdo1_generation > 0 && state.measurements.tpdo2_generation > 0);
    }
    let writes = bus.writes.lock().unwrap();
    assert_eq!(writes[&(1, 0x1800, 5)], 1u16.to_le_bytes());
    assert_eq!(writes[&(3, 0x1800, 5)], 4u16.to_le_bytes());
    assert!(writes.contains_key(&(1, 0x1016, 1)));
    assert!(!writes.contains_key(&(3, 0x1016, 1)));
}
