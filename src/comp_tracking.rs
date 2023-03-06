use std::{
    collections::{BTreeMap, BTreeSet},
    thread::JoinHandle,
};

use usiem::{chrono::{self, Duration}, prelude::{SiemCommandResponse, SiemDataset, task::{SiemTask, SiemTaskResult}}};
use usiem::{
    crossbeam_channel::{self, Sender},
    prelude::{
        holder::DatasetHolder, kernel_message::KernelMessager, storage::SiemComponentStateStorage,
        SiemCommandCall, SiemCommandHeader, SiemComponent, SiemDatasetType, SiemMessage,
        SiemResult,
    },
    utilities::types::LogString,
};

use crate::{channels::ComponentChannels, comp_store::SiemComponentStore};

struct MessageTracking {
    pub original_msg_id: u64,
    pub from: u64,
    pub timestamp: i64,
}

#[derive(Default)]
pub struct ComponentTracking {
    thread_map: BTreeMap<u64, (JoinHandle<SiemResult<()>>, Sender<SiemMessage>)>,
    component_map: BTreeMap<LogString, Vec<u64>>,
    component_reverse_map: BTreeMap<u64, LogString>,
    component_ids: BTreeSet<u64>,
    command_tracking: BTreeMap<u64, MessageTracking>,
    pub command_timeout : i64,
    tasks_running : Vec<(i64,u64, async_std::task::JoinHandle<SiemTaskResult>)>,
    completed_tasks : Vec<(i64,u64, SiemTaskResult)>
}

pub struct ComponentBuildingInfo {
    pub store: SiemComponentStore,
    pub channels: ComponentChannels,
    pub datasets: DatasetHolder,
}

impl ComponentTracking {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn run_all_components(&mut self, build_info: &ComponentBuildingInfo) {
        let store = &build_info.store;

        for comp in &store.input_components {
            self.run_component_generic(comp, &build_info)
                .expect("Inputs must start correctly!!");
        }
        self.run_parser(&build_info)
            .expect("Kernel needs the Parser component!!");
        self.run_enricher(&build_info)
            .expect("Kernel needs the Enricher component!!");
        self.run_rule_engine(&build_info)
            .expect("Kernel needs the RuleEngine component!!");
        self.run_output(&build_info)
            .expect("Kernel needs the output Component!!");
        let _ = self.run_wal(&build_info);
        for comp in &store.other_components {
            let _ = self.run_component_generic(comp, &build_info);
        }
        self.run_alert(&build_info)
            .expect("Kernel needs the Alert Component!!");
    }

    pub fn run_wal(&mut self, build_info: &ComponentBuildingInfo) -> Result<(), &'static str> {
        match &build_info.store.wal_component {
            Some(comp) => self.run_component_generic(comp, build_info),
            None => Err("Kernel does not have WAL component!!"),
        }
    }

    pub fn run_enricher(&mut self, build_info: &ComponentBuildingInfo) -> Result<(), &'static str> {
        match &build_info.store.enricher_component {
            Some(comp) => self.run_component_generic(comp, build_info),
            None => Err("Kernel needs a EnricherComponent!!"),
        }
    }
    pub fn run_alert(&mut self, build_info: &ComponentBuildingInfo) -> Result<(), &'static str> {
        match &build_info.store.alert_component {
            Some(comp) => self.run_component_generic(comp, build_info),
            None => Err("Kernel needs the Alert Component!!"),
        }
    }
    pub fn run_output(&mut self, build_info: &ComponentBuildingInfo) -> Result<(), &'static str> {
        match &build_info.store.output_component {
            Some(comp) => self.run_component_generic(comp, build_info),
            None => Err("Kernel needs the Alert Component!!"),
        }
    }
    pub fn run_rule_engine(
        &mut self,
        build_info: &ComponentBuildingInfo,
    ) -> Result<(), &'static str> {
        match &build_info.store.rule_engine_component {
            Some(comp) => self.run_component_generic(comp, build_info),
            None => Err("Kernel needs the Alert Component!!"),
        }
    }
    pub fn run_parser(&mut self, build_info: &ComponentBuildingInfo) -> Result<(), &'static str> {
        match &build_info.store.parser_component {
            Some(comp) => self.run_component_generic(comp, build_info),
            None => Err("Kernel needs the Alert Component!!"),
        }
    }

    pub fn run_component_generic(
        &mut self,
        comp: &Box<dyn SiemComponent>,
        build_info: &ComponentBuildingInfo,
    ) -> Result<(), &'static str> {
        let state_storage = match &build_info.store.state_storage {
            Some(v) => v,
            None => return Err("No state storage"),
        };
        let component_id = self.random_component_id();
        let mut new_comp = (*comp).duplicate();
        new_comp.set_id(component_id);
        let comp_name = LogString::Owned(new_comp.name().to_string());
        let thread_info = Self::run_component(new_comp, &build_info, state_storage);
        self.save_component_info(component_id, comp_name, thread_info);
        Ok(())
    }

    pub fn save_component_info(
        &mut self,
        component_id: u64,
        comp_name: LogString,
        comp_info: (JoinHandle<SiemResult<()>>, Sender<SiemMessage>),
    ) {
        let (thread_join, local_channel) = comp_info;
        self.component_map
            .entry(comp_name.clone())
            .and_modify(|v| {
                v.push(component_id);
            })
            .or_insert(vec![component_id]);
        self.component_reverse_map.insert(component_id, comp_name);
        self.thread_map
            .insert(component_id, (thread_join, local_channel));
    }

    fn run_component(
        mut component: Box<dyn SiemComponent>,
        build_info: &ComponentBuildingInfo,
        state_storage: &Box<dyn SiemComponentStateStorage>,
    ) -> (JoinHandle<SiemResult<()>>, Sender<SiemMessage>) {
        let comp_id = component.id();
        let kernel_channel = build_info.channels.kernel_channel.1.clone();
        let msngr = KernelMessager::new(
            comp_id,
            component.name().to_string(),
            kernel_channel.clone(),
        );
        component.set_kernel_sender(msngr.clone());
        component.set_storage(state_storage.duplicate());
        let local_channel = component.local_channel();

        component.set_datasets(build_info.datasets.clone());

        let mut required_datasets: Vec<&SiemDatasetType> = Vec::new();
        let capabilities = component.capabilities();
        for dts in capabilities.datasets() {
            required_datasets.push(dts.name());
        }
        let thread_join = std::thread::spawn(move || {
            usiem::logging::initialize_component_logger(msngr);
            let res = component.run();
            let mut params = BTreeMap::new();
            params.insert(
                LogString::Borrowed("component_id"),
                LogString::Owned(comp_id.to_string()),
            );
            let _e = kernel_channel.send(SiemMessage::Command(
                SiemCommandHeader {
                    comp_id,
                    comm_id: 0,
                    user: String::from("kernel"),
                },
                SiemCommandCall::OTHER(LogString::Borrowed("COMPONENT_FINISHED"), params),
            ));
            res
        });
        (thread_join, local_channel)
    }

    pub fn clean_comp_id(&mut self, comp_id: u64) -> Result<(), &'static str> {
        let comp = self
            .component_reverse_map
            .get(&comp_id)
            .ok_or("Invalid component ID")?;
        self.component_map.entry(comp.clone()).and_modify(|vec| {
            match vec.iter().position(|v| *v == comp_id) {
                Some(pos) => {
                    vec.remove(pos);
                }
                None => {}
            };
        });
        let _ = self.component_reverse_map.remove(&comp_id);
        Ok(())
    }

    pub fn send_message_to_component(&self, component_id : u64, message : SiemMessage) {
        let channel = match self.channel_of_component(component_id) {
            Some(v) => v,
            None => return
        };
        let _ = channel.send(message);
    }

    pub fn _restart_component(
        &mut self,
        build_info: &ComponentBuildingInfo,
        comp_id: u64,
    ) -> Result<(), &'static str> {
        let comp = self
            .component_reverse_map
            .get(&comp_id)
            .ok_or("Invalid component ID")?;
        
        let comp = build_info
            .store
            ._get_component_by_name(comp)
            .ok_or("Component cannot be found")?;
        self.clean_comp_id(comp_id)?;
        self.run_component_generic(comp, build_info)
    }

    pub fn register_external_component(
        &mut self,
        comp_id: u64,
        info: (JoinHandle<SiemResult<()>>, Sender<SiemMessage>),
    ) {
        self.thread_map.insert(comp_id, info);
    }

    pub fn running_instances_of_component(&self, name: &str) -> usize {
        self.component_map
            .get(name)
            .and_then(|v| Some(v.len()))
            .unwrap_or(0)
    }

    pub fn channel_of_component(&self, comp_id : u64) -> Option<&Sender<SiemMessage>> {
        self.thread_map.get(&comp_id).and_then(|(_handler, channel)| Some(channel))
    }

    pub fn random_component_id(&mut self) -> u64 {
        let id = rand::random::<u64>();
        loop {
            if !self.component_ids.contains(&id) {
                self.component_ids.insert(id);
                break;
            }
        }
        id
    }
    pub fn random_id() -> u64 {
        rand::random::<u64>()
    }
    pub fn random_usize() -> usize {
        rand::random::<usize>()
    }

    pub fn route_message(
        &mut self,
        msg: SiemMessage,
        build_info: &ComponentBuildingInfo,
    ) -> Result<(), SiemMessage> {
        match msg {
            SiemMessage::Command(header, command) => {
                self.route_command(header, command, build_info)
            }
            SiemMessage::Response(header, response) => {
                self.route_response(header, response)
            },
            SiemMessage::Log(log) => match build_info.channels.parser_channel.1.try_send(log) {
                Ok(_) => Ok(()),
                Err(e) => match e {
                    crossbeam_channel::TrySendError::Full(v) => Err(SiemMessage::Log(v)),
                    crossbeam_channel::TrySendError::Disconnected(v) => Err(SiemMessage::Log(v)),
                },
            },
            SiemMessage::Notification(notification) => {
                println!("{} - {:?} - {}({}) - {}", notification.timestamp, notification.level,notification.component_name, notification.component, notification.log);
                Ok(())
            },
            SiemMessage::Dataset(dataset) => {
                self.notify_dataset(dataset, build_info);
                Ok(())
            },
            SiemMessage::Alert(alert) => {
                match build_info.channels.alert_channel.1.try_send(alert) {
                    Ok(_) => Ok(()),
                    Err(e) => match e {
                        crossbeam_channel::TrySendError::Full(v) => Err(SiemMessage::Alert(v)),
                        crossbeam_channel::TrySendError::Disconnected(v) => {
                            Err(SiemMessage::Alert(v))
                        }
                    },
                }
            }
            SiemMessage::Task(header, task) => self.process_task(header, task, build_info),
            // NO deberia pasar este caso
            SiemMessage::TaskResult(_, _) => Ok(()),
            _ => Ok(()),
        }
    }

    fn notify_dataset(&mut self, dataset : SiemDataset, build_info: &ComponentBuildingInfo) {
        let component_names = match build_info.store.get_components_for_dataset(&dataset.dataset_type()) {
            Some(v) => v,
            None => return
        };
        for comp in component_names {
            let running_comps = match self.component_map.get(&comp[..]) {
                Some(v) => v,
                None => continue,
            };
            for comp_id in running_comps {
                let channel = match self.channel_of_component(*comp_id) {
                    Some(v) => v,
                    None => continue
                };
                let _ = channel.send(SiemMessage::Dataset(dataset.clone()));
            }
        }
    }

    fn route_response(
        &mut self,
        header: SiemCommandHeader,
        response: SiemCommandResponse,
    ) -> Result<(), SiemMessage> {
        let now = chrono::Utc::now().timestamp_millis();
        let command_info = match self.command_tracking.get(&header.comm_id) {
            Some(v) => v,
            // Silently ignore when we cant find a component
            None => return Ok(())
        };
        let timeout = self.command_timeout;

        if (now - command_info.timestamp) > timeout {
            drop(command_info);
            // Remove old entries
            self.command_tracking.retain(|_k,v| (now- v.timestamp) < timeout);
            return Ok(())
        }

        let channel = match self.channel_of_component(command_info.from) {
            Some(v) => v,
            None => return Ok(())
        };
        let _ = channel.try_send(SiemMessage::Response(SiemCommandHeader { user: header.user.clone(), comp_id: header.comp_id, comm_id: command_info.original_msg_id }, response));
        Ok(())
    }

    fn route_command(
        &mut self,
        header: SiemCommandHeader,
        command: SiemCommandCall,
        build_info: &ComponentBuildingInfo,
    ) -> Result<(), SiemMessage> {
        let components_for_command = match build_info.store.get_components_for_command(&command) {
            Some(v) => v,
            None => return Ok(()),
        };

        let new_command_id = Self::random_id();

        self.command_tracking.insert(
            new_command_id,
            MessageTracking {
                original_msg_id: header.comm_id,
                from: header.comp_id,
                timestamp: chrono::Utc::now().timestamp_millis(),
            },
        );

        for comp_name in components_for_command {
            let selected_id = match self.component_map.get(&comp_name[..]) {
                Some(v) => v[Self::random_usize() % v.len()],
                None => continue,
            };
            let channel = match self.channel_of_component(selected_id) {
                Some(v) => v,
                None => continue
            };
            let _ = channel.try_send(SiemMessage::Command(SiemCommandHeader { user: header.user.clone(), comp_id: header.comp_id, comm_id: new_command_id }, command.clone()));
        }
        
        Ok(())
    }

    /// Clean up old not completed tasks and store the result of the succesfull ones
    pub fn check_tasks(&mut self) {
        let now = chrono::Utc::now().timestamp_millis();
        if let Some(position) = self.tasks_running.iter().position(|(time, _, _)| *time > now) {
            let (_, comm_id, task) = self.tasks_running.remove(position);
            match async_std::task::block_on(async move {
                async_std::future::timeout(std::time::Duration::from_millis(10), task).await
            }) {
                Ok(v) => {
                    self.completed_tasks.push((now, comm_id, v));
                },
                Err(_) => {}
            };
            
        }
    }

    pub fn get_task_result(&self, id : u64) -> Option<SiemTaskResult> {
        for (_completed_at, task_id, task) in &self.completed_tasks {
            if *task_id == id {
                return Some(task.clone())
            }
        }
        None
    }
    pub fn update_dataset(&self, dataset : SiemDataset) {
        for (_id, (_component,sender)) in &self.thread_map {
            let _ = sender.send_timeout(SiemMessage::Dataset(dataset.clone()), std::time::Duration::from_millis(20));
        }
    }

    fn process_task(
        &mut self,
        header: SiemCommandHeader,
        task: SiemTask,
        build_info: &ComponentBuildingInfo,
    ) -> Result<(), SiemMessage> {
        let task_definitions = match build_info.store.get_definitions_for_task(&task.data.class()) {
            Some(v) => v,
            None => return Ok(()),
        };

        let new_command_id = Self::random_id();
        let now = chrono::Utc::now().timestamp_millis();
        self.command_tracking.insert(
            new_command_id,
            MessageTracking {
                original_msg_id: header.comm_id,
                from: header.comp_id,
                timestamp: now,
            },
        );

        for task_def in task_definitions {
            let new_task = match task_def.builder()(task.clone(), &build_info.datasets) {
                Ok(v) => v,
                Err(_) => continue
            };
            match async_std::task::Builder::new().name(format!("Task-{}-{}",task.data.class(),now)).spawn(new_task) {
                Ok(v) => {
                    let end_task = (chrono::Utc::now() + Duration::milliseconds(task_def.max_duration() as i64)).timestamp_millis();
                    self.tasks_running.push((end_task, new_command_id, v));
                },
                Err(_) => {}
            };
        }
        Ok(())
    }


}
