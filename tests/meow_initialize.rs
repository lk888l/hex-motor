use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use can_transport::{CanBus, CanCapabilities, CanFilter, CanFrame, CanIoError, CanRx};
use hex_motor::canopen::nmt::NmtState;
use hex_motor::cia402::PdoProfile;
use hex_motor::meow_motor::{
    CompressedMitMapping, MeowMotorInitializeOptions, MeowMotorLifecycle, MeowMotorLogic,
    MeowMotorManager, MeowMotorManagerOptions, MeowMotorSharedPdoNode, SharedHostPdoConfig,
    PRODUCT_CODE_4310, VENDOR_ID,
};
use hex_motor::Error;
use tokio::sync::{mpsc, Semaphore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BusEvent {
    LastConstantUpload(u8),
    Start(u8),
}

#[derive(Clone, Copy)]
struct Device {
    state: NmtState,
    send_preop_pdos: bool,
    operational_mode_display: Option<u8>,
}

struct TestBus {
    devices: Mutex<Vec<Device>>,
    writes: Mutex<HashMap<(u8, u16, u8), Vec<u8>>>,
    events: Mutex<Vec<BusEvent>>,
    hold_last_upload: Option<(u8, Semaphore)>,
    reject_disable_once: AtomicBool,
    subscribers: Mutex<Vec<(CanFilter, mpsc::UnboundedSender<CanFrame>)>>,
}

impl TestBus {
    fn manager(self: &Arc<Self>) -> MeowMotorManager {
        MeowMotorManager::new(
            self.clone(),
            MeowMotorManagerOptions {
                auto_identify: false,
                broadcast_heartbeat: false,
                ..Default::default()
            },
        )
        .unwrap()
    }

    async fn identify_devices(&self, manager: &MeowMotorManager) {
        let count = self.devices.lock().unwrap().len() as u8;
        for node in 1..=count {
            manager.identify(node).await.unwrap();
            manager
                .subscribe_status(node)
                .unwrap()
                .wait_for(|s| s.connection.online)
                .await
                .unwrap();
        }
    }

    async fn run<F: std::future::Future>(&self, test: F) -> F::Output {
        tokio::select! {
            _ = self.feedback() => unreachable!(),
            result = tokio::time::timeout(Duration::from_secs(5), test) => result.unwrap(),
        }
    }

    async fn wait_for_events(&self, expected: &[BusEvent]) {
        loop {
            {
                let observed = self.events.lock().unwrap();
                if expected.iter().all(|e| observed.contains(e)) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    fn new(count: usize) -> Self {
        Self {
            devices: Mutex::new(vec![
                Device {
                    state: NmtState::PreOperational,
                    send_preop_pdos: false,
                    operational_mode_display: Some(0),
                };
                count
            ]),
            writes: Default::default(),
            events: Default::default(),
            hold_last_upload: None,
            reject_disable_once: AtomicBool::new(false),
            subscribers: Default::default(),
        }
    }

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
            for (node, device) in (1u16..).zip(self.devices.lock().unwrap().iter()) {
                self.publish(0x700 + node, &[device.state as u8]);
                let mode_display = match device.state {
                    NmtState::Operational => device.operational_mode_display,
                    NmtState::PreOperational if device.send_preop_pdos => Some(0),
                    _ => continue,
                };
                let mut position = [0; 8];
                position[4..6].copy_from_slice(&timestamp.to_le_bytes());
                self.publish(0x180 + node, &position);
                if let Some(mode) = mode_display {
                    self.publish(0x280 + node, &[mode, 0, 0, 0, 0, 0, 0]);
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
            (0x1400 | 0x1401 | 0x1800 | 0x1801, 1) => vec![0; 4], // force initial PDO configuration
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
                let node = frame.data()[1];
                if state == NmtState::Operational {
                    self.events.lock().unwrap().push(BusEvent::Start(node));
                }
                self.devices.lock().unwrap()[usize::from(node) - 1].state = state;
            }
            id @ 0x601..=0x67f => {
                let node = (id - 0x600) as u8;
                if frame.data()[..4] == [0x40, 0x02, 0x41, 7] {
                    self.events
                        .lock()
                        .unwrap()
                        .push(BusEvent::LastConstantUpload(node));
                    if let Some((held_node, gate)) = &self.hold_last_upload {
                        if node == *held_node {
                            gate.acquire().await.unwrap().forget();
                        }
                    }
                }
                self.respond_to_sdo(node, frame.data());
            }
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
async fn shared_slots_follow_input_order_even_when_a_peer_fails_and_retries() {
    let bus = Arc::new(TestBus::new(3));
    bus.reject_disable_once.store(true, Ordering::Relaxed);
    let manager = bus.manager();
    let host_node_id = 0x10;
    let profile = PdoProfile::from_event_timer_ms(4).unwrap();
    let nodes = [3, 2, 1].map(|node_id| MeowMotorSharedPdoNode {
        node_id,
        compressed_mit_mapping: CompressedMitMapping {
            position_max: f32::from(node_id),
            ..Default::default()
        },
        configure_consumer_heartbeat: node_id != 3,
    });
    bus.run(async {
        bus.identify_devices(&manager).await;
        let results = manager
            .initialize_many_with_shared_pdo(&nodes, profile, host_node_id)
            .await;
        assert!(
            matches!(
                results.as_slice(),
                [(1, Ok(())), (2, Err(Error::Sdo(_))), (3, Ok(()))]
            ),
            "{results:?}"
        );
        assert!(!bus.events.lock().unwrap().contains(&BusEvent::Start(2)));
        assert_eq!(
            manager.status(2).unwrap().lifecycle,
            MeowMotorLifecycle::Identified
        );

        // Recover exactly the missing lane using the public single-device API.
        manager
            .initialize_with_options(
                2,
                MeowMotorInitializeOptions {
                    pdo_profile: profile,
                    configure_consumer_heartbeat: true,
                    shared_host_pdo: Some(SharedHostPdoConfig {
                        host_node_id,
                        motor_slot: 1,
                        total_motors: 3,
                        compressed_mit_mapping: nodes[1].compressed_mit_mapping,
                    }),
                },
            )
            .await
            .unwrap();
    })
    .await;
    let writes = bus.writes.lock().unwrap();
    for (slot, node) in (0u8..).zip(nodes) {
        let node_id = node.node_id;
        let state = manager.status(node_id).unwrap();
        assert_eq!(state.lifecycle, MeowMotorLifecycle::Initialized);
        assert_eq!(state.logic, Some(MeowMotorLogic::Disabled));
        assert_eq!(writes[&(node_id, 0x1800, 5)], 4u16.to_le_bytes());
        assert_eq!(
            writes[&(node_id, 0x1800, 3)],
            profile.inhibit_time_x100us().to_le_bytes()
        );
        assert_eq!(writes[&(node_id, 0x1801, 5)], 20u16.to_le_bytes());
        assert_eq!(writes[&(node_id, 0x1400, 1)], 0x190u32.to_le_bytes());
        assert_eq!(writes[&(node_id, 0x1401, 1)], 0x290u32.to_le_bytes());
        assert_eq!(writes[&(node_id, 0x1600, 0)], [6]);
        assert_eq!(writes[&(node_id, 0x1601, 0)], [3]);
        assert_eq!(
            writes[&(node_id, 0x1600, 2 * slot + 1)],
            0x4103_0220u32.to_le_bytes()
        );
        assert_eq!(
            writes[&(node_id, 0x1600, 2 * slot + 2)],
            0x4103_0320u32.to_le_bytes()
        );
        assert_eq!(
            writes[&(node_id, 0x1601, slot + 1)],
            0x4401_0008u32.to_le_bytes()
        );
        assert_eq!(
            writes[&(node_id, 0x4103, 5)],
            node.compressed_mit_mapping.position_max.to_le_bytes()
        );
        if node.configure_consumer_heartbeat {
            // Consume manager heartbeat node 10 with its 250 ms timeout.
            assert_eq!(writes[&(node_id, 0x1016, 1)], 0x000A_00FAu32.to_le_bytes());
        } else {
            assert!(!writes.contains_key(&(node_id, 0x1016, 1)));
        }
    }
}

#[tokio::test]
async fn ninth_node_finishes_configuration_before_any_start_and_commits_without_waiting_for_peers()
{
    let bus = Arc::new(TestBus::new(9));
    {
        let mut devices = bus.devices.lock().unwrap();
        // Node 1 reports Disabled throughout PreOp and initially provides only TPDO1 in Operational.
        devices[0].send_preop_pdos = true;
        devices[0].operational_mode_display = None;
    }
    let manager = bus.manager();
    let nodes: Vec<_> = (1..=9).collect();
    let profile = PdoProfile::from_event_timer_ms(8).unwrap();
    bus.run(async {
        bus.identify_devices(&manager).await;
        let monitor = async {
            manager
                .subscribe_status(9)
                .unwrap()
                .wait_for(|s| s.lifecycle.is_ready())
                .await
                .unwrap();
            let pending = manager.status(1).unwrap();
            assert_eq!(pending.connection.nmt_state, Some(NmtState::Operational));
            assert_eq!(pending.lifecycle, MeowMotorLifecycle::Initializing);
            assert_eq!(pending.logic, Some(MeowMotorLogic::Disabled));
            // The next fresh TPDO2 reports Enabled. Neither this nor the stale Disabled is sufficient.
            bus.devices.lock().unwrap()[0].operational_mode_display = Some(1);
        };
        let (results, ()) = tokio::join!(manager.initialize_many(&nodes, profile), monitor);
        assert_eq!(results.len(), 9);
        assert!(
            matches!(results[0], (1, Err(Error::DeviceOperationRejected { .. }))),
            "{results:?}"
        );
        for (node, result) in results.into_iter().skip(1) {
            result.unwrap();
            assert_eq!(
                manager.status(node).unwrap().lifecycle,
                MeowMotorLifecycle::Initialized
            );
        }
        assert_eq!(
            manager.status(1).unwrap().lifecycle,
            MeowMotorLifecycle::Identified
        );
        let events = bus.events.lock().unwrap();
        let first_start = events
            .iter()
            .position(|e| matches!(e, BusEvent::Start(_)))
            .unwrap();
        for node in nodes {
            assert!(
                events[..first_start].contains(&BusEvent::LastConstantUpload(node)),
                "{events:?}"
            );
            assert_eq!(
                bus.writes.lock().unwrap()[&(node, 0x1800, 5)],
                8u16.to_le_bytes()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn reboot_while_a_peer_is_still_configuring_invalidates_the_prepared_node() {
    let bus = Arc::new(TestBus {
        hold_last_upload: Some((2, Semaphore::new(0))),
        ..TestBus::new(2)
    });
    let manager = bus.manager();
    let profile = PdoProfile::from_event_timer_ms(4).unwrap();
    bus.run(async {
        bus.identify_devices(&manager).await;
        let reboot = async {
            bus.wait_for_events(&[
                BusEvent::LastConstantUpload(1),
                BusEvent::LastConstantUpload(2),
            ])
            .await;
            assert!(bus
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|e| !matches!(e, BusEvent::Start(_))));
            bus.publish(0x701, &[NmtState::BootUp as u8]);
            manager
                .subscribe_status(1)
                .unwrap()
                .wait_for(|s| s.lifecycle == MeowMotorLifecycle::Unknown)
                .await
                .unwrap();
            bus.hold_last_upload.as_ref().unwrap().1.add_permits(1);
        };
        let (results, ()) = tokio::join!(manager.initialize_many(&[1, 2], profile), reboot);
        assert!(
            matches!(results.as_slice(), [(1, Err(_)), (2, Ok(()))]),
            "{results:?}"
        );
        assert!(!bus.events.lock().unwrap().contains(&BusEvent::Start(1)));
        assert_eq!(
            manager.status(1).unwrap().lifecycle,
            MeowMotorLifecycle::Unknown
        );
    })
    .await;
}

#[tokio::test]
async fn cancelling_at_the_barrier_releases_both_nodes_and_retry_requires_fresh_feedback() {
    let bus = Arc::new(TestBus {
        hold_last_upload: Some((2, Semaphore::new(0))),
        ..TestBus::new(2)
    });
    let manager = bus.manager();
    let profile = PdoProfile::from_event_timer_ms(4).unwrap();
    bus.run(async {
        bus.identify_devices(&manager).await;
        let mut batch = Box::pin(manager.initialize_many(&[1, 2], profile));
        tokio::select! {
            _ = bus.wait_for_events(&[BusEvent::LastConstantUpload(1), BusEvent::LastConstantUpload(2)]) => {}
            results = &mut batch => panic!("configuration gate still closed: {results:?}"),
        }
        drop(batch);
        for node in [1, 2] {
            assert_eq!(manager.status(node).unwrap().lifecycle, MeowMotorLifecycle::Identified);
        }

        bus.hold_last_upload.as_ref().unwrap().1.add_permits(1);
        for device in bus.devices.lock().unwrap().iter_mut() {
            device.operational_mode_display = None;
        }
        let mut retry = Box::pin(manager.initialize_many(&[1, 2], profile));
        tokio::select! {
            _ = bus.wait_for_events(&[BusEvent::Start(1), BusEvent::Start(2)]) => {}
            results = &mut retry => panic!("feedback still missing: {results:?}"),
        }

        // Let discovery and the listener process node 1's only TPDO2 while the
        // initialization future is asleep. Node 2 receives heartbeats and TPDO1 only.
        let mut status = manager.subscribe_status(1).unwrap();
        status.wait_for(|s| s.connection.nmt_state == Some(NmtState::Operational)).await.unwrap();
        bus.publish(0x281, &[0; 7]);
        status.wait_for(|s| s.logic == Some(MeowMotorLogic::Disabled)).await.unwrap();
        let results = retry.await;
        assert!(matches!(results.as_slice(), [(1, Ok(())), (2, Err(Error::DeviceOperationTimeout { nid: 2, .. }))]), "{results:?}");
        assert_eq!(manager.status(2).unwrap().lifecycle, MeowMotorLifecycle::Identified);
    }).await;
}
