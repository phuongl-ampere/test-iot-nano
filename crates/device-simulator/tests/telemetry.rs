use std::{
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};

use chrono::{TimeZone, Utc};
use device_simulator::{
    SimulationConfig, SimulatorError, publish_simulation, simulated_telemetry, simulation_round,
    telemetry_topic,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

struct TestBroker {
    child: Child,
    port: u16,
}

impl TestBroker {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let child = Command::new("mosquitto")
            .args(["-p", &port.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        for _ in 0..50 {
            if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Self { child, port };
            }
            thread::sleep(Duration::from_millis(20));
        }

        panic!("Mosquitto test broker did not become available");
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn simulation_uses_a_stable_device_id_topic_and_sequence() {
    let event = simulated_telemetry(
        123,
        42,
        Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 0).unwrap(),
    );

    assert_eq!(event.device_id, "esp-000123");
    assert_eq!(event.sequence, 42);
    assert_eq!(
        telemetry_topic(&event.device_id),
        "iot/v1/devices/esp-000123/telemetry"
    );
}

#[test]
fn each_simulated_device_has_a_distinct_deterministic_boot_id() {
    let at = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 0).unwrap();

    let first = simulated_telemetry(1, 1, at);
    let second = simulated_telemetry(2, 1, at);

    assert_ne!(first.boot_id, second.boot_id);
}

#[test]
fn simulation_configuration_rejects_zero_devices_messages_and_interval() {
    let valid = SimulationConfig {
        broker_host: "127.0.0.1".to_owned(),
        broker_port: 1883,
        devices: 1,
        messages_per_device: 1,
        interval: Duration::from_secs(1),
    };

    let mut no_devices = valid.clone();
    no_devices.devices = 0;
    assert_eq!(no_devices.validate(), Err(SimulatorError::ZeroDevices));

    let mut no_messages = valid.clone();
    no_messages.messages_per_device = 0;
    assert_eq!(no_messages.validate(), Err(SimulatorError::ZeroMessages));

    let mut no_interval = valid;
    no_interval.interval = Duration::ZERO;
    assert_eq!(no_interval.validate(), Err(SimulatorError::ZeroInterval));
}

#[test]
fn simulation_round_generates_one_topic_and_event_per_device() {
    let config = SimulationConfig {
        broker_host: "127.0.0.1".to_owned(),
        broker_port: 1883,
        devices: 3,
        messages_per_device: 1,
        interval: Duration::from_secs(1),
    };
    let at = Utc.with_ymd_and_hms(2026, 9, 4, 10, 12, 0).unwrap();

    let events = simulation_round(&config, 9, at).unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].0, "iot/v1/devices/esp-000001/telemetry");
    assert_eq!(events[0].1.sequence, 9);
    assert_eq!(events[2].0, "iot/v1/devices/esp-000003/telemetry");
}

#[tokio::test]
async fn mqtt_simulator_publishes_one_qos_one_event_per_device() {
    let broker = TestBroker::start();
    let mut options = MqttOptions::new("simulator-test-subscriber", "127.0.0.1", broker.port);
    options.set_keep_alive(Duration::from_secs(5));
    let (subscriber, mut event_loop) = AsyncClient::new(options, 10);
    subscriber
        .subscribe("iot/v1/devices/+/telemetry", QoS::AtLeastOnce)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                event_loop.poll().await.unwrap(),
                Event::Incoming(Packet::SubAck(_))
            ) {
                return;
            }
        }
    })
    .await
    .unwrap();

    let config = SimulationConfig {
        broker_host: "127.0.0.1".to_owned(),
        broker_port: broker.port,
        devices: 2,
        messages_per_device: 1,
        interval: Duration::from_secs(1),
    };
    let publisher = tokio::spawn(publish_simulation(config));

    let topics = tokio::time::timeout(Duration::from_secs(2), async {
        let mut topics = Vec::new();
        while topics.len() < 2 {
            if let Event::Incoming(Packet::Publish(publish)) = event_loop.poll().await.unwrap() {
                topics.push(publish.topic);
            }
        }
        topics
    })
    .await
    .unwrap();

    publisher.await.unwrap().unwrap();
    assert_eq!(
        topics,
        vec![
            "iot/v1/devices/esp-000001/telemetry",
            "iot/v1/devices/esp-000002/telemetry",
        ]
    );
}
