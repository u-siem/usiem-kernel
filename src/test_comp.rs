use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::vec;
use usiem::components::common::{SiemComponentCapabilities, SiemMessage};
use usiem::components::dataset::holder::DatasetHolder;
use usiem::components::dataset::SiemDatasetType;
use usiem::components::{SiemComponent, SiemDatasetManager};
use usiem::crossbeam_channel;
use usiem::crossbeam_channel::{Receiver, Sender};
use usiem::events::SiemLog;
use usiem::prelude::storage::SiemComponentStateStorage;
use usiem::prelude::SiemResult;

#[derive(Clone)]
pub struct BasicComponent {
    /// Send actions to this components
    local_chnl_snd: Sender<SiemMessage>,
    /// Receive logs
    log_receiver: Receiver<SiemLog>,
    log_sender: Sender<SiemLog>,
}
impl BasicComponent {
    pub fn new() -> BasicComponent {
        let (local_chnl_snd, _local_chnl_rcv) = crossbeam_channel::unbounded();
        let (log_sender, log_receiver) = crossbeam_channel::unbounded();
        return BasicComponent {
            local_chnl_snd,
            log_receiver,
            log_sender,
        };
    }
}

impl SiemComponent for BasicComponent {
    fn name(&self) -> &'static str {
        "BasicParser"
    }
    fn local_channel(&self) -> Sender<SiemMessage> {
        self.local_chnl_snd.clone()
    }
    fn set_log_channel(&mut self, log_sender: Sender<SiemLog>, receiver: Receiver<SiemLog>) {
        self.log_receiver = receiver;
        self.log_sender = log_sender;
    }
    fn duplicate(&self) -> Box<dyn SiemComponent> {
        return Box::new(self.clone());
    }
    fn set_datasets(&mut self, _datasets: DatasetHolder) {}
    fn run(&mut self) -> SiemResult<()> {
        std::thread::sleep(Duration::from_millis(1_000));
        Ok(())
    }
    fn set_storage(&mut self, _conn: Box<dyn SiemComponentStateStorage>) {}

    /// Capabilities and actions that can be performed on this component
    fn capabilities(&self) -> SiemComponentCapabilities {
        SiemComponentCapabilities::new(
            Cow::Borrowed("BasicDummyComponent"),
            Cow::Borrowed("Basic dummy component for testing purposes"),
            Cow::Borrowed(""), // No HTML
            vec![],
            vec![],
            vec![],
            vec![],
        )
    }
}

#[derive(Clone)]
pub struct BasicDatasetManager {
    /// Send actions to this components
    local_chnl_snd: Sender<SiemMessage>,
    datasets: Arc<Mutex<DatasetHolder>>,
}
impl BasicDatasetManager {
    pub fn new() -> BasicDatasetManager {
        let (local_chnl_snd, _local_chnl_rcv) = crossbeam_channel::unbounded();
        return BasicDatasetManager {
            local_chnl_snd,
            datasets: Arc::new(Mutex::new(DatasetHolder::new())),
        };
    }
}

impl SiemDatasetManager for BasicDatasetManager {
    fn name(&self) -> &str {
        "BasicDatasetManager"
    }
    fn local_channel(&self) -> Sender<SiemMessage> {
        self.local_chnl_snd.clone()
    }
    fn run(&mut self) -> SiemResult<()> {
        usiem::info!("Starting DatasetManager");
        std::thread::sleep(Duration::from_millis(10));
        usiem::info!("Stopping DatasetManager");
        Ok(())
    }

    fn get_datasets(&self) -> Arc<Mutex<DatasetHolder>> {
        self.datasets.clone()
    }
    fn register_dataset(&mut self, _dataset: SiemDatasetType) {}

    fn set_id(&mut self, _id: u64) {}

    fn register_datasets(&mut self, _datasets: Vec<SiemDatasetType>) {}
}
