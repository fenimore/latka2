pub type Offset = u64;

use std::{io, fs};
use std::path::PathBuf;
// use std::sync::{Arc, Mutex};
use std::iter::FromIterator;

use crate::partition::Partition;
use crate::partition::segment::{MaxBytes};


pub struct Broker {
    id: u32,
    host: String,
    port: u16,
}

impl Broker {
    pub fn addr(&self) -> String { format!("{}:{}", self.host, self.port) }
}


pub struct TopicPartition {
    topic: String,
    path: PathBuf,
    partition_id: u32,
    replica_ids: Vec<u32>,
    leader_id: u32,
    preferred_leader: u32,

    pub partition: Partition,
}

impl TopicPartition {
    pub fn new(
        topic: String,
        log_path: String,
        partition_id: u32,
        replicas: Vec<u32>,
        leader_id: u32,
        preferred_leader: u32,
    ) -> io::Result<TopicPartition> {
        let path = PathBuf::from(format!("{}/{}-{}", log_path, &topic, partition_id));
        let partition = Partition::create(topic.clone(), &mut path.clone(), MaxBytes(1024, 1024))?;
        return Ok(TopicPartition{
            topic: topic,
            path: path,
            partition_id: partition_id,
            replica_ids: replicas,
            leader_id: leader_id,
            preferred_leader: preferred_leader,
            partition: partition,
        })
    }

    pub fn open(
        topic: String,
        log_path: String,
        partition_id: u32,
        replicas: Vec<u32>,
        leader_id: u32,
        preferred_leader: u32,
    ) -> io::Result<TopicPartition> {
        let path = PathBuf::from(format!("{}/{}-{}", log_path, &topic, partition_id));
        let partition = Partition::load(&mut path.clone(), MaxBytes(1024, 1024))?;
        return Ok(TopicPartition{
            topic: topic,
            path: path,
            partition_id: partition_id,
            replica_ids: replicas,
            leader_id: leader_id,
            preferred_leader: preferred_leader,
            partition: partition,
        })
    }
}
