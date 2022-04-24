use usiem::components::dataset::holder::DatasetHolder;
use usiem::crossbeam_channel::{Receiver, Sender};
use usiem::crossbeam_channel;
use usiem::components::common::{
    SiemComponentCapabilities, SiemComponentStateStorage, SiemMessage
};
use usiem::components::dataset::{SiemDatasetType};
use usiem::components::{SiemComponent, SiemDatasetManager};
use usiem::events::SiemLog;
use std::borrow::Cow;
use std::vec;
use lazy_static::lazy_static;

lazy_static! {
    static ref DATASETS : DatasetHolder = DatasetHolder::from_datasets(vec![]);
}

#[derive(Clone)]
pub struct BasicComponent {
    /// Send actions to the kernel
    kernel_sender: Sender<SiemMessage>,
    /// Send actions to this components
    local_chnl_snd: Sender<SiemMessage>,
    /// Receive logs
    log_receiver: Receiver<SiemLog>,
    log_sender: Sender<SiemLog>,
    id: u64,
}
impl BasicComponent {
    pub fn new() -> BasicComponent {
        let (kernel_sender, _receiver) = crossbeam_channel::bounded(1000);
        let (local_chnl_snd, _local_chnl_rcv) = crossbeam_channel::unbounded();
        let (log_sender, log_receiver) = crossbeam_channel::unbounded();
        return BasicComponent {
            kernel_sender,
            local_chnl_snd,
            log_receiver,
            log_sender,
            id: 0,
        };
    }
}

impl SiemComponent for BasicComponent {
    fn id(&self) -> u64 {
        return self.id;
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn name(&self) -> &str {
        "BasicParser"
    }
    fn local_channel(&self) -> Sender<SiemMessage> {
        self.local_chnl_snd.clone()
    }
    fn set_log_channel(&mut self, log_sender: Sender<SiemLog>, receiver: Receiver<SiemLog>) {
        self.log_receiver = receiver;
        self.log_sender = log_sender;
    }
    fn set_kernel_sender(&mut self, sender: Sender<SiemMessage>) {
        self.kernel_sender = sender;
    }
    fn duplicate(&self) -> Box<dyn SiemComponent> {
        return Box::new(self.clone());
    }
    fn set_datasets(&mut self, _datasets: DatasetHolder) {}
    fn run(&mut self) {}
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
            vec![]
        )
    }
}

#[derive(Clone)]
pub struct BasicDatasetManager {
    /// Send actions to the kernel
    kernel_sender: Sender<SiemMessage>,
    /// Send actions to this components
    local_chnl_snd: Sender<SiemMessage>,
}
impl BasicDatasetManager {
    pub fn new() -> BasicDatasetManager {
        let (kernel_sender, _receiver) = crossbeam_channel::bounded(1000);
        let (local_chnl_snd, _local_chnl_rcv) = crossbeam_channel::unbounded();
        return BasicDatasetManager {
            kernel_sender,
            local_chnl_snd
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
    fn set_kernel_sender(&mut self, sender: Sender<SiemMessage>) {
        self.kernel_sender = sender;
    }
    fn run(&mut self) {}
    fn get_datasets(&self) -> DatasetHolder {
        return DATASETS.clone()
    }
    fn register_dataset(&mut self, _dataset : SiemDatasetType) {}
}
