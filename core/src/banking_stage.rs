//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to construct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use crate::banking_trace::BankingPacketBatch;
use crate::tpu::ControlObj;
use crossbeam_channel::Sender;
use solana_sdk::transaction::MAX_TX_ACCOUNT_LOCKS;
use std::collections::HashMap;
use std::f32::consts::E;
use std::mem;
// use std::hash::Hash;
use crossbeam_channel::Receiver;
use min_max_heap::MinMaxHeap;
use solana_perf::packet::PacketBatch;
use solana_poh::poh_recorder::BankStart;
// use solana_poh::poh_recorder::WorkingBank;
// use solana_program_runtime::executor_cache::MAX_CACHED_EXECUTORS;
use solana_runtime::bank::Bank;
// use solana_runtime::cost_model::TransactionCost;
use solana_runtime::transaction_priority_details::GetTransactionPriorityDetails;
use solana_sdk::program_pack::Pack;
use solana_sdk::transaction::SanitizedTransaction;
use solana_sdk::{
    pubkey::Pubkey,
    transaction::{SanitizedVersionedTransaction, VersionedTransaction},
};
use solana_streamer::packet::Packet;

use crate::tpu::Buffers;
use crate::tpu::BANKING_THREADS;
use {
    self::{
        consumer::Consumer,
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        // forwarder::Forwarder,
        // packet_receiver::PacketReceiver,
    },
    crate::{
        banking_stage::committer::Committer,
        banking_trace::BankingPacketReceiver,
        // latest_unprocessed_votes::{LatestUnprocessedVotes, VoteSource},
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        qos_service::QosService,
        tracer_packet_stats::TracerPacketStats,
        unprocessed_packet_batches::*,
        // unprocessed_transaction_storage::{ThreadType, UnprocessedTransactionStorage},
    },
    crossbeam_channel::RecvTimeoutError,
    histogram::Histogram,
    solana_client::connection_cache::ConnectionCache,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::{measure, measure_us},
    solana_perf::{data_budget::DataBudget, packet::PACKETS_PER_BATCH},
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::{
        bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::timing::AtomicInterval,
    std::{
        cmp, env,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub mod committer;
pub mod consumer;
mod decision_maker;
mod forwarder;
mod packet_receiver;

// Fixed thread size seems to be fastest on GCP setup
pub const NUM_THREADS: u32 = 6;
pub const MAX_ACCTS_PER_TX: usize = 256;
pub const ONE_SEC: u64 = 1000000;

const TOTAL_BUFFERED_PACKETS: usize = 700_000;

const NUM_VOTE_PROCESSING_THREADS: u32 = 2;
const MIN_THREADS_BANKING: u32 = 1;
const MIN_TOTAL_THREADS: u32 = NUM_VOTE_PROCESSING_THREADS + MIN_THREADS_BANKING;

const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);

#[derive(Debug, Default)]
pub struct BankingStageStats {
    last_report: AtomicInterval,
    id: u32,
    receive_and_buffer_packets_count: AtomicUsize,
    dropped_packets_count: AtomicUsize,
    pub(crate) dropped_duplicated_packets_count: AtomicUsize,
    newly_buffered_packets_count: AtomicUsize,
    current_buffered_packets_count: AtomicUsize,
    rebuffered_packets_count: AtomicUsize,
    consumed_buffered_packets_count: AtomicUsize,
    forwarded_transaction_count: AtomicUsize,
    forwarded_vote_count: AtomicUsize,
    batch_packet_indexes_len: Histogram,

    // Timing
    consume_buffered_packets_elapsed: AtomicU64,
    receive_and_buffer_packets_elapsed: AtomicU64,
    filter_pending_packets_elapsed: AtomicU64,
    pub(crate) packet_conversion_elapsed: AtomicU64,
    transaction_processing_elapsed: AtomicU64,
}

impl BankingStageStats {
    pub fn new(id: u32) -> Self {
        BankingStageStats {
            id,
            batch_packet_indexes_len: Histogram::configure()
                .max_value(PACKETS_PER_BATCH as u64)
                .build()
                .unwrap(),
            ..BankingStageStats::default()
        }
    }

    fn is_empty(&self) -> bool {
        0 == self
            .receive_and_buffer_packets_count
            .load(Ordering::Relaxed) as u64
            + self.dropped_packets_count.load(Ordering::Relaxed) as u64
            + self
                .dropped_duplicated_packets_count
                .load(Ordering::Relaxed) as u64
            + self.newly_buffered_packets_count.load(Ordering::Relaxed) as u64
            + self.current_buffered_packets_count.load(Ordering::Relaxed) as u64
            + self.rebuffered_packets_count.load(Ordering::Relaxed) as u64
            + self.consumed_buffered_packets_count.load(Ordering::Relaxed) as u64
            + self
                .consume_buffered_packets_elapsed
                .load(Ordering::Relaxed)
            + self
                .receive_and_buffer_packets_elapsed
                .load(Ordering::Relaxed)
            + self.filter_pending_packets_elapsed.load(Ordering::Relaxed)
            + self.packet_conversion_elapsed.load(Ordering::Relaxed)
            + self.transaction_processing_elapsed.load(Ordering::Relaxed)
            + self.forwarded_transaction_count.load(Ordering::Relaxed) as u64
            + self.forwarded_vote_count.load(Ordering::Relaxed) as u64
            + self.batch_packet_indexes_len.entries()
    }

    fn report(&mut self, report_interval_ms: u64) {
        // skip reporting metrics if stats is empty
        if self.is_empty() {
            return;
        }
        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "banking_stage-loop-stats",
                ("id", self.id as i64, i64),
                (
                    "receive_and_buffer_packets_count",
                    self.receive_and_buffer_packets_count
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "dropped_packets_count",
                    self.dropped_packets_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "dropped_duplicated_packets_count",
                    self.dropped_duplicated_packets_count
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "newly_buffered_packets_count",
                    self.newly_buffered_packets_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "current_buffered_packets_count",
                    self.current_buffered_packets_count
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "rebuffered_packets_count",
                    self.rebuffered_packets_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "consumed_buffered_packets_count",
                    self.consumed_buffered_packets_count
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "forwarded_transaction_count",
                    self.forwarded_transaction_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "forwarded_vote_count",
                    self.forwarded_vote_count.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "consume_buffered_packets_elapsed",
                    self.consume_buffered_packets_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "receive_and_buffer_packets_elapsed",
                    self.receive_and_buffer_packets_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "filter_pending_packets_elapsed",
                    self.filter_pending_packets_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "packet_conversion_elapsed",
                    self.packet_conversion_elapsed.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "transaction_processing_elapsed",
                    self.transaction_processing_elapsed
                        .swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "packet_batch_indices_len_min",
                    self.batch_packet_indexes_len.minimum().unwrap_or(0) as i64,
                    i64
                ),
                (
                    "packet_batch_indices_len_max",
                    self.batch_packet_indexes_len.maximum().unwrap_or(0) as i64,
                    i64
                ),
                (
                    "packet_batch_indices_len_mean",
                    self.batch_packet_indexes_len.mean().unwrap_or(0) as i64,
                    i64
                ),
                (
                    "packet_batch_indices_len_90pct",
                    self.batch_packet_indexes_len.percentile(90.0).unwrap_or(0) as i64,
                    i64
                )
            );
            self.batch_packet_indexes_len.clear();
        }
    }
}

#[derive(Debug, Default)]
pub struct BatchedTransactionDetails {
    pub costs: BatchedTransactionCostDetails,
    pub errors: BatchedTransactionErrorDetails,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionCostDetails {
    pub batched_signature_cost: u64,
    pub batched_write_lock_cost: u64,
    pub batched_data_bytes_cost: u64,
    pub batched_builtins_execute_cost: u64,
    pub batched_bpf_execute_cost: u64,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionErrorDetails {
    pub batched_retried_txs_per_block_limit_count: u64,
    pub batched_retried_txs_per_vote_limit_count: u64,
    pub batched_retried_txs_per_account_limit_count: u64,
    pub batched_retried_txs_per_account_data_block_limit_count: u64,
    pub batched_dropped_txs_per_account_data_total_limit_count: u64,
}

/// Stores the stage's thread handle and output receiver.
pub struct BankingStage {
    bank_thread_hdls: Vec<JoinHandle<()>>,
}

struct LookupTable {
    acct_lookup: HashMap<Pubkey, Vec<u64>>,
}

impl LookupTable {
    fn new(capacity: usize) -> Self {
        let acct_lookup: HashMap<Pubkey, Vec<u64>> = HashMap::with_capacity(capacity);
        LookupTable { acct_lookup }
    }
}

pub struct Scheduler {
    scheduler_thread_hdls: JoinHandle<()>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SchPacket {
    pub transaction: SanitizedTransaction,
    pub priority: u64,
    pub accts: Vec<Pubkey>,
}

impl PartialOrd for SchPacket {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SchPacket {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

const PRIORITIZED_BUFFER_SIZE: usize = 700_000;
// const ACCT_ACCESS_LIST_SIZE: usize = 256;
const ACCT_LOOKUP_TABLE_SIZE: usize = 11_000_000;
// const  PACKET_BATCH_SIZE: usize = 64*64;

#[derive(Default)]
struct SchedulerStats {
    end_to_end_time: u64,
    packet_reception: u64,
    working_bank: u64,
    retry_and_garbage_collection: u64,
    batch_processing: u64,
    buffering_time: u64,
    un_buffering_time: u64,
    last_logged: u64,
    loop_counter: u64,
    tx_counter: u64,
    tx_recv_counter: u64,
    not_leader_counter: u64,
    vote_counter: u64,
    empty_buffer: u64,
    buffered_packets: u64,
    retry_counter: u64,
}

impl SchedulerStats {
    pub fn new() -> Self {
        SchedulerStats {
            end_to_end_time: 0,
            packet_reception: 0,
            working_bank: 0,
            retry_and_garbage_collection: 0,
            batch_processing: 0,
            buffering_time: 0,
            un_buffering_time: 0,
            last_logged: 0,
            loop_counter: 0,
            tx_counter: 0,
            tx_recv_counter: 0,
            not_leader_counter: 0,
            vote_counter: 0,
            empty_buffer: 0,
            buffered_packets: 0,
            retry_counter: 0,
        }
    }
}

impl Scheduler {
    pub fn new(
        non_vote_receiver: &Receiver<Vec<PacketBatch>>,
        tpu_vote_receiver: &Receiver<Vec<PacketBatch>>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        channels: &Buffers,
        retry_receiver: &Receiver<ControlObj>,
    ) -> Self {
        let packet_receiver = non_vote_receiver.clone();
        let vote_packet_receiver = tpu_vote_receiver.clone();
        let poh_recorder = poh_recorder.clone();
        let channels = channels.clone();
        let retry_receiver = retry_receiver.clone();

        let mut prioritized_buffer: MinMaxHeap<SchPacket> =
            MinMaxHeap::with_capacity(PRIORITIZED_BUFFER_SIZE);
        let mut acct_lookup_table = LookupTable::new(ACCT_LOOKUP_TABLE_SIZE);

        let scheduler_thread_hdls = Builder::new()
            .name(format!("solSchStg"))
            .spawn(move || {
                // let mut target_thread: _;
                let mut sch_stats = SchedulerStats::new();
                loop {
                    let start = Instant::now();
                    
                    // bank required for deserialization of packet into transaction
                    let (working_bank, working_bank_time_us) = measure_us!(Scheduler::is_bank_available(&poh_recorder));
                    let bank = match working_bank {
                        Some(bank) => bank,
                        None => {sch_stats.not_leader_counter += 1; continue;},
                    };                 
                    sch_stats.working_bank += working_bank_time_us;

                    // packet batches from sigverify stage
                    let ((no_of_packets, packet_batches), packet_reception_time) = measure_us!(Scheduler::receive_packet_batches(&packet_receiver, &vote_packet_receiver, &mut prioritized_buffer));
                    if no_of_packets == 0 {
                        sch_stats.working_bank -= working_bank_time_us;
                        continue;
                    }
                    sch_stats.packet_reception += packet_reception_time;
                    sch_stats.tx_recv_counter += no_of_packets as u64;

                    // garbage collection 
                    let (_, retry_and_garbage_collection_time) = measure_us!(Scheduler::retry_and_garbage_collection(&retry_receiver, &mut acct_lookup_table, &mut prioritized_buffer, &mut sch_stats));
                    sch_stats.retry_and_garbage_collection += retry_and_garbage_collection_time;

                    // batch processing
                    let (_, batch_processing_time) = measure_us!(Scheduler::process_batches(packet_batches, &bank, &channels, &mut acct_lookup_table, &mut prioritized_buffer, &mut sch_stats));
                    sch_stats.batch_processing += batch_processing_time;

                    sch_stats.last_logged += start.elapsed().as_micros() as u64;
                    sch_stats.loop_counter += 1;

                    if sch_stats.last_logged > ONE_SEC {
                        info!(
                            "scheduler loop time {}, count {}, packet_reception {}, working_bank {}, retry_and_garbage_collection {}, batch_processing {}, buffering_time {}, un_buffering_time {}, tx_counter {}, tx_recv_counter {}, not_leader_counter {}, vote_counter {}, empty_buffer {}, buffered_packets {}, retry_counter {}",
                            sch_stats.last_logged, 
                            sch_stats.loop_counter, 
                            sch_stats.packet_reception/sch_stats.loop_counter, 
                            sch_stats.working_bank/sch_stats.loop_counter, 
                            sch_stats.retry_and_garbage_collection/sch_stats.loop_counter, 
                            sch_stats.batch_processing/sch_stats.loop_counter, 
                            sch_stats.buffering_time/sch_stats.loop_counter,
                            sch_stats.un_buffering_time/sch_stats.loop_counter,
                            sch_stats.tx_counter,
                            sch_stats.tx_recv_counter,
                            sch_stats.not_leader_counter,
                            sch_stats.vote_counter,
                            sch_stats.empty_buffer,
                            sch_stats.buffered_packets,
                            sch_stats.retry_counter,
                        );
                        sch_stats = SchedulerStats::default();
                    }
                }
            })
            .unwrap();
        Self {
            scheduler_thread_hdls,
        }
    }

    fn process_batches(
        packet_batches: Vec<PacketBatch>,
        bank: &Bank,
        channels: &Buffers,
        acct_lookup_table: &mut LookupTable,
        prioritized_buffer: &mut MinMaxHeap<SchPacket>,
        sch_stats: &mut SchedulerStats,
    ) {
        // fill buffer
        let (_, filling_time) = measure_us!(Scheduler::fill_buffer(
            &packet_batches,
            bank,
            channels,
            prioritized_buffer,
            sch_stats,
        ));
        sch_stats.buffering_time += filling_time;

        // emtpty buffer
        let (_, popping_time) = measure_us!(Scheduler::schedule_and_send(
            channels,
            acct_lookup_table,
            prioritized_buffer,
            sch_stats
        ));
        sch_stats.un_buffering_time += popping_time;
    }

    fn schedule_and_send(
        channels: &Buffers,
        acct_lookup_table: &mut LookupTable,
        prioritized_buffer: &mut MinMaxHeap<SchPacket>,
        sch_stats: &mut SchedulerStats,
    ) {
        while !prioritized_buffer.is_empty() {
            if let Some(sch_packet) = prioritized_buffer.pop_max() {
                let target_thread =
                    // Scheduler::get_thread_number(&sch_packet, acct_lookup_table, &channels)
                    //     as usize;
                    // for testing
                    Scheduler::round_robin(BANKING_THREADS);
                info!("target_thread: {}", target_thread);
                sch_stats.tx_counter += 1;
                let _sending_result = channels
                    .get(target_thread as usize)
                    .unwrap()
                    .0
                    .send(sch_packet);
            } else {
                // in case the buffer is empty
                sch_stats.empty_buffer += 1;
                return;
            }
        }
    }

    fn round_robin(input: usize) -> usize {
        // static variable to keep track of last returned thread number
        static mut LAST_THREAD_NUMBER: usize = 0;

        // get the current thread number and update the static variable
        let current_thread_number = unsafe {
            let thread_number = LAST_THREAD_NUMBER;
            LAST_THREAD_NUMBER = (LAST_THREAD_NUMBER + 1) % input;
            thread_number
        };

        current_thread_number
    }

    fn fill_buffer(
        packet_batches: &Vec<PacketBatch>,
        bank: &Bank,
        channels: &Buffers,
        prioritized_buffer: &mut MinMaxHeap<SchPacket>,
        sch_stats: &mut SchedulerStats,
    ) {
        for batch in packet_batches {
            'pkt_loop: for (i, packet) in batch.iter().enumerate() {
                // discard packets based on discarded flag
                if Scheduler::check_discarded_packet(packet) == true {
                    continue 'pkt_loop;
                };

                // extracting data
                let (sanitized_transaction, priority, accts) =
                    Scheduler::get_tx_meta_info(packet, &bank);

                // handle simple vote
                if sanitized_transaction.is_simple_vote_transaction() {
                    sch_stats.vote_counter += 1;
                    Scheduler::handle_votes(&channels, sanitized_transaction, priority, accts);
                    continue 'pkt_loop;
                }

                // push in the prioritized buffer
                prioritized_buffer.push(SchPacket {
                    transaction: sanitized_transaction,
                    priority,
                    accts,
                });
                sch_stats.buffered_packets += 1;
            }
        }
    }

    fn check_discarded_packet(packet: &Packet) -> bool {
        if packet.meta().is_tracer_packet() || packet.meta().discard() || packet.meta().forwarded()
        {
            true
        } else {
            false
        }
    }

    fn handle_votes(
        channels: &Buffers,
        sanitized_transaction: SanitizedTransaction,
        priority: u64,
        accts: Vec<Pubkey>,
    ) {
        let target_thread = Self::find_least_loaded_thread(&channels);
        let _sending_result = channels
            .get(target_thread as usize)
            .unwrap()
            .0
            .send(SchPacket {
                transaction: sanitized_transaction,
                priority,
                accts,
            });
    }

    fn retry_and_garbage_collection(
        retry_receiver: &Receiver<ControlObj>,
        acct_lookup_table: &mut LookupTable,
        prioritized_buffer: &mut MinMaxHeap<SchPacket>,
        sch_stats: &mut SchedulerStats,
    ) {
        'retry_loop: while !retry_receiver.is_empty() {
            let control_obj = match retry_receiver.try_recv() {
                Ok(obj) => obj,
                Err(_) => continue 'retry_loop,
            };
            let tx = control_obj.sanitized_transaction;
            let accts = Self::get_accts_from_san_tx(tx.clone());

            if control_obj.just_del == true {
                Self::update_lookup_table_after_exec(
                    acct_lookup_table,
                    control_obj.thread_id as u64,
                    accts.clone(),
                )
            } else {
                Self::update_lookup_table_after_exec(
                    acct_lookup_table,
                    control_obj.thread_id as u64,
                    accts.clone(),
                );

                prioritized_buffer.push(SchPacket {
                    transaction: tx,
                    priority: 0,
                    accts,
                });
                sch_stats.retry_counter += 1;
            }
        }
    }

    fn is_bank_available(poh_recorder: &Arc<RwLock<PohRecorder>>) -> Option<Arc<Bank>> {
        let working_bank = Scheduler::get_working_bank(&poh_recorder);
        if working_bank.is_some() {
            Some(working_bank.unwrap().working_bank)
        } else {
            None
        }
    }

    fn receive_packet_batches(
        non_vote_receiver: &Receiver<Vec<PacketBatch>>,
        tpu_vote_receiver: &Receiver<Vec<PacketBatch>>,
        prioritized_buffer: &mut MinMaxHeap<SchPacket>,
    ) -> (usize, Vec<PacketBatch>) {
        let capacity = prioritized_buffer.capacity() - prioritized_buffer.len();
        let recv_timeout = Scheduler::get_receive_timeout(prioritized_buffer);

        let (non_vote_packets, mut packet_batches) =
            Scheduler::receive_until(non_vote_receiver, recv_timeout, capacity);
        let (vote_packets, vote_packet_batches) =
            Scheduler::receive_until(tpu_vote_receiver, recv_timeout, capacity);

        packet_batches.extend(vote_packet_batches);
        (non_vote_packets + vote_packets, packet_batches)
    }

    fn get_receive_timeout(prioritized_buffer: &MinMaxHeap<SchPacket>) -> Duration {
        if !prioritized_buffer.is_empty() {
            Duration::from_millis(0)
        } else {
            // Default wait time
            Duration::from_micros(10)
        }
    }

    fn receive_until(
        receiver: &Receiver<Vec<PacketBatch>>,
        recv_timeout: Duration,
        packet_count_upperbound: usize,
    ) -> (usize, Vec<PacketBatch>) {
        let start = Instant::now();

        let mut packet_batches = match receiver.recv_timeout(recv_timeout) {
            Ok(batches) => batches,
            Err(_) => {
                let empty_vec: Vec<PacketBatch> = Vec::new();
                return (0, empty_vec);
            }
        };

        let mut num_packets_received = packet_batches
            .iter()
            .map(|batch| batch.len())
            .sum::<usize>();

        // let num_packets_received = 0;
        while let Ok(new_batches) = receiver.try_recv() {
            trace!("got more packet batches in packet deserializer");
            let new_packet_count = new_batches.iter().map(|batch| batch.len()).sum::<usize>();
            num_packets_received += new_packet_count;
            packet_batches.extend(new_batches);

            if start.elapsed() >= recv_timeout || num_packets_received >= packet_count_upperbound {
                break;
            }
        }

        (num_packets_received, packet_batches)
    }

    fn find_least_loaded_thread(channels: &Buffers) -> usize {
        let mut min = usize::MAX;
        let mut least_loaded_thread = 0;
        for (i, (sender, _)) in channels.into_iter().enumerate() {
            if sender.len() < min {
                min = sender.len();
                least_loaded_thread = i;
            }
        }
        least_loaded_thread
    }

    fn get_thread_number(
        sch_packet: &SchPacket,
        lookup_table: &mut LookupTable,
        channels: &Buffers,
    ) -> u64 {
        let target_thread;
        let mut prev_thread_selection = 1000; // random number to detect initial value
        for acct in sch_packet.accts.iter() {
            let mut single_thread: bool = false;
            let entry = lookup_table.acct_lookup.get(acct);

            if entry.is_some() {
                for (i, x) in entry.unwrap().iter().enumerate() {
                    if *x == 0 {
                        continue;
                    } else if single_thread == false {
                        single_thread = true;
                        if prev_thread_selection == 1000 {
                            prev_thread_selection = i;
                        } else if prev_thread_selection == i {
                        } else {
                            target_thread = Self::find_least_loaded_thread(channels);
                            Self::update_lookup_table(
                                lookup_table,
                                target_thread as u64,
                                sch_packet,
                            );
                            return target_thread as u64;
                        }

                        continue;
                    } else {
                        target_thread = Self::find_least_loaded_thread(channels);
                        Self::update_lookup_table(lookup_table, target_thread as u64, sch_packet);
                        return target_thread as u64;
                    }
                }
            } else {
                continue;
            }
        }
        // in case the acct is not found in any thread
        if prev_thread_selection == 1000 {
            prev_thread_selection = Self::find_least_loaded_thread(channels);
        }
        target_thread = prev_thread_selection;
        Self::update_lookup_table(lookup_table, target_thread as u64, sch_packet);
        return target_thread as u64;
    }

    fn update_lookup_table(
        lookup_table: &mut LookupTable,
        target_thread: u64,
        sch_packet: &SchPacket,
    ) {
        for acct in &sch_packet.accts {
            let mut new_entry = vec![0; BANKING_THREADS];
            new_entry[target_thread as usize] = 1;
            lookup_table
                .acct_lookup
                .entry(*acct)
                .and_modify(|v| v[target_thread as usize] += 1)
                .or_insert(new_entry);
        }
    }

    fn update_lookup_table_after_exec(
        lookup_table: &mut LookupTable,
        target_thread: u64,
        accts: Vec<Pubkey>,
    ) {
        for acct in accts {
            if let Some(entry) = lookup_table.acct_lookup.get_mut(&acct) {
                if let Some(value) = entry.get_mut(target_thread as usize) {
                    if *value > 0 {
                        *value -= 1;
                    }
                }
            }
        }
    }

    fn get_tx_meta_info(packet: &Packet, bank: &Bank) -> (SanitizedTransaction, u64, Vec<Pubkey>) {
        let sanitized_versioned_transaction = Scheduler::get_sanitized_transaction(packet);
        let tx = Scheduler::get_tx(&bank, &sanitized_versioned_transaction);
        let priority = Scheduler::get_priority(&sanitized_versioned_transaction);
        let accts = Scheduler::get_accts(&sanitized_versioned_transaction);
        (tx, priority, accts)
    }

    fn get_priority(sanitized_transaction: &SanitizedVersionedTransaction) -> u64 {
        let tx_priority = sanitized_transaction.get_transaction_priority_details();

        if tx_priority.is_some() {
            tx_priority.unwrap().priority
        } else {
            0
        }
    }

    fn get_tx(
        bank: &Bank,
        sanitized_transaction: &SanitizedVersionedTransaction,
    ) -> SanitizedTransaction {
        let tx = SanitizedTransaction::try_new(
            sanitized_transaction.clone(),
            sanitized_transaction.get_message().message.hash(),
            false,
            bank,
        )
        .unwrap();

        tx
    }

    fn get_packet_batches(receiver: &BankingPacketReceiver) -> Vec<PacketBatch> {
        let packet_batch = receiver.recv_timeout(Duration::from_millis(10));
        if packet_batch.is_ok() {
            let packet_batch = Arc::try_unwrap(packet_batch.unwrap());
            if packet_batch.is_ok() {
                return packet_batch.unwrap().0;
            } else {
                return Vec::new();
            }
        } else {
            return Vec::new();
        }
    }

    fn get_sanitized_transaction(packet: &Packet) -> SanitizedVersionedTransaction {
        let versioned_transaction: VersionedTransaction = packet.deserialize_slice(..).unwrap();
        SanitizedVersionedTransaction::try_from(versioned_transaction).unwrap()
    }

    fn get_accts(sanitized_transaction: &SanitizedVersionedTransaction) -> Vec<Pubkey> {
        let accts = sanitized_transaction
            .get_message()
            .message
            .static_account_keys();
        accts.to_vec()
    }

    // fn get_accts_from_san_tx(tx: SanitizedTransaction, bank: &Bank) -> Vec<Pubkey> {
    fn get_accts_from_san_tx(tx: SanitizedTransaction) -> Vec<Pubkey> {
        // let tx_accts = tx
        //     .get_account_locks(bank.get_transaction_account_lock_limit())
        //     .unwrap();
        let tx_accts = tx.get_account_locks(MAX_TX_ACCOUNT_LOCKS).unwrap();
        let mut ro_accts = tx_accts.readonly;
        let wo_accts = tx_accts.writable;
        ro_accts.extend(wo_accts);
        let mut accts: Vec<Pubkey> = Vec::new();
        for acct in ro_accts {
            accts.push(*acct);
        }
        accts
    }

    fn get_working_bank(poh_recorder: &Arc<RwLock<PohRecorder>>) -> Option<BankStart> {
        poh_recorder
            .read()
            .unwrap()
            .bank_start()
            .filter(|bank_start| bank_start.should_working_bank_still_be_processing_txs())
    }

    pub fn join(self) -> thread::Result<()> {
        self.scheduler_thread_hdls.join()
    }
}

#[derive(Debug, Clone)]
pub enum ForwardOption {
    NotForward,
    ForwardTpuVote,
    ForwardTransaction,
}

#[derive(Debug, Default)]
pub struct FilterForwardingResults {
    pub(crate) total_forwardable_packets: usize,
    pub(crate) total_tracer_packets_in_buffer: usize,
    pub(crate) total_forwardable_tracer_packets: usize,
    pub(crate) total_packet_conversion_us: u64,
    pub(crate) total_filter_packets_us: u64,
}

impl BankingStage {
    /// Create the stage using `bank`. Exit when `verified_receiver` is dropped.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        // non_vote_receiver: BankingPacketReceiver,
        // tpu_vote_receiver: BankingPacketReceiver,
        // gossip_vote_receiver: BankingPacketReceiver,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        receivers: Vec<Receiver<SchPacket>>,
        retry_sender: &Sender<ControlObj>,
    ) -> Self {
        Self::new_num_threads(
            cluster_info,
            poh_recorder,
            // non_vote_receiver,
            // tpu_vote_receiver,
            // gossip_vote_receiver,
            Self::num_threads(),
            transaction_status_sender,
            replay_vote_sender,
            log_messages_bytes_limit,
            connection_cache,
            bank_forks,
            prioritization_fee_cache,
            receivers,
            retry_sender,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_num_threads(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        // non_vote_receiver: BankingPacketReceiver,
        // tpu_vote_receiver: BankingPacketReceiver,
        // _gossip_vote_receiver: BankingPacketReceiver,
        num_threads: u32,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        connection_cache: Arc<ConnectionCache>,
        bank_forks: Arc<RwLock<BankForks>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        receivers: Vec<Receiver<SchPacket>>,
        retry_sender: &Sender<ControlObj>,
    ) -> Self {
        assert!(num_threads >= MIN_TOTAL_THREADS);
        // Single thread to generate entries from many banks.
        // This thread talks to poh_service and broadcasts the entries once they have been recorded.
        // Once an entry has been recorded, its blockhash is registered with the bank.
        let data_budget = Arc::new(DataBudget::default());
        // let retry_sender = Arc::new(retry_sender).clone();
        let retry_sender = retry_sender.clone();
        // let receivers = Arc::new(receivers.clone());
        // let batch_limit =
        // TOTAL_BUFFERED_PACKETS / ((num_threads - NUM_VOTE_PROCESSING_THREADS) as usize);
        // Keeps track of extraneous vote transactions for the vote threads
        // let _latest_unprocessed_votes = Arc::new(LatestUnprocessedVotes::new());
        // Many banks that process transactions in parallel.
        let bank_thread_hdls: Vec<JoinHandle<()>> = (0..BANKING_THREADS)
            .map(|id| {
                // let x = &receivers;
                let packet_receiver = match id {
                    0 => receivers.get(0).unwrap().clone(),
                    1 => receivers.get(1).unwrap().clone(),
                    2 => receivers.get(2).unwrap().clone(),
                    _ => receivers.get(3).unwrap().clone(),
                };

                let retry_sender = retry_sender.clone();
                let poh_recorder = poh_recorder.clone();

                let committer = Committer::new(
                    transaction_status_sender.clone(),
                    replay_vote_sender.clone(),
                    prioritization_fee_cache.clone(),
                );
                let decision_maker = DecisionMaker::new(cluster_info.id(), poh_recorder.clone());

                let consumer = Consumer::new(
                    committer,
                    poh_recorder.read().unwrap().new_recorder(),
                    QosService::new(id as u32),
                    log_messages_bytes_limit,
                );

                Builder::new()
                    .name(format!("solBanknStgTx{id:02}"))
                    .spawn(move || {
                        let mut banking_stage_stats = BankingStageStats::new(id as u32);

                        let mut slot_metrics_tracker = LeaderSlotMetricsTracker::new(id as u32);
                        let mut last_metrics_update = Instant::now();

                        let mut accumalative_time = 0;
                        let mut counter = 0;
                        loop {
                            let start = Instant::now();
                            if !packet_receiver.is_empty() {
                                let (_, process_buffered_packets_time) = measure!(
                                    Self::process_buffered_packets(
                                        &packet_receiver,
                                        &banking_stage_stats,
                                        &mut slot_metrics_tracker,
                                        &consumer,
                                        &decision_maker,
                                        &retry_sender,
                                    ),
                                    "process_buffered_packets",
                                );
                                slot_metrics_tracker.increment_process_buffered_packets_us(
                                    process_buffered_packets_time.as_us(),
                                );
                                last_metrics_update = Instant::now();
                            }

                            banking_stage_stats.report(1000);

                            let elapsed = start.elapsed().as_micros();
                            accumalative_time += elapsed;
                            counter += 1;
                            if accumalative_time > 1000000 {
                                info!(
                                    "banking loop time {}, count {}, id {}",
                                    accumalative_time, counter, id
                                );
                                accumalative_time = 0;
                                counter = 0;
                            }
                        }
                    })
                    .unwrap()
            })
            .collect();
        Self { bank_thread_hdls }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_buffered_packets(
        packet_receiver: &Receiver<SchPacket>,
        banking_stage_stats: &BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
        consumer: &Consumer,
        decision_maker: &DecisionMaker,
        retry_sender: &Sender<ControlObj>,
    ) {
        let (decision, make_decision_time) =
            measure!(decision_maker.make_consume_or_forward_decision());
        let metrics_action = slot_metrics_tracker.check_leader_slot_boundary(decision.bank_start());
        slot_metrics_tracker.increment_make_decision_us(make_decision_time.as_us());

        match decision {
            BufferedPacketsDecision::Consume(bank_start) => {
                // Take metrics action before consume packets (potentially resetting the
                // slot metrics tracker to the next slot) so that we don't count the
                // packet processing metrics from the next slot towards the metrics
                // of the previous slot
                slot_metrics_tracker.apply_action(metrics_action);
                let (_, consume_buffered_packets_time) = measure!(
                    consumer.consume_buffered_packets(
                        &bank_start,
                        packet_receiver,
                        banking_stage_stats,
                        slot_metrics_tracker,
                        retry_sender,
                    ),
                    "consume_buffered_packets",
                );
                slot_metrics_tracker
                    .increment_consume_buffered_packets_us(consume_buffered_packets_time.as_us());
            }
            _ => (),
        }
    }

    // fn process_loop(id: u32, packet_receiver: Receiver<SchPacket>) {
    // let mut banking_stage_stats = BankingStageStats::new(id);
    // let mut tracer_packet_stats = TracerPacketStats::new(id);

    // let mut slot_metrics_tracker = LeaderSlotMetricsTracker::new(id);
    // let mut last_metrics_update = Instant::now();

    // loop {
    //     if packet_receiver.is_empty()
    //         || last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
    //     {
    //         let (_, process_buffered_packets_time) = measure!(
    //             Self::process_buffered_packets(
    //                 packet_receiver,
    //                 &banking_stage_stats,
    //                 &mut slot_metrics_tracker,
    //                 &mut tracer_packet_stats,
    //             ),
    //             "process_buffered_packets",
    //         );
    //         slot_metrics_tracker
    //             .increment_process_buffered_packets_us(process_buffered_packets_time.as_us());
    //         last_metrics_update = Instant::now();
    //     }

    //     tracer_packet_stats.report(1000);

    //     // match packet_receiver.receive_and_buffer_packets(
    //     //     &mut unprocessed_transaction_storage,
    //     //     &mut banking_stage_stats,
    //     //     &mut tracer_packet_stats,
    //     //     &mut slot_metrics_tracker,
    //     // ) {
    //     //     Ok(()) | Err(RecvTimeoutError::Timeout) => (),
    //     //     Err(RecvTimeoutError::Disconnected) => break,
    //     // }
    //     banking_stage_stats.report(1000);
    // }
    // }

    pub fn num_threads() -> u32 {
        cmp::max(
            env::var("SOLANA_BANKING_THREADS")
                .map(|x| x.parse().unwrap_or(NUM_THREADS))
                .unwrap_or(NUM_THREADS),
            MIN_TOTAL_THREADS,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        for bank_thread_hdl in self.bank_thread_hdls {
            bank_thread_hdl.join()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_trace::{BankingPacketBatch, BankingTracer},
        crossbeam_channel::{unbounded, Receiver},
        itertools::Itertools,
        solana_entry::entry::{Entry, EntrySlice},
        solana_gossip::cluster_info::Node,
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{
                create_genesis_config, create_genesis_config_with_leader, GenesisConfigInfo,
            },
            get_tmp_ledger_path_auto_delete,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_perf::packet::{to_packet_batches, PacketBatch},
        solana_poh::{
            poh_recorder::{
                create_test_recorder, PohRecorderError, Record, RecordTransactionsSummary,
            },
            poh_service::PohService,
        },
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            genesis_utils::{activate_feature, bootstrap_validator_stake_lamports},
        },
        solana_sdk::{
            hash::Hash,
            poh_config::PohConfig,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_transaction,
        },
        solana_streamer::socket::SocketAddrSpace,
        solana_vote_program::{
            vote_state::VoteStateUpdate, vote_transaction::new_vote_state_update_transaction,
        },
        std::{
            sync::atomic::{AtomicBool, Ordering},
            thread::sleep,
        },
    };

    pub(crate) fn new_test_cluster_info(keypair: Option<Arc<Keypair>>) -> (Node, ClusterInfo) {
        let keypair = keypair.unwrap_or_else(|| Arc::new(Keypair::new()));
        let node = Node::new_localhost_with_pubkey(&keypair.pubkey());
        let cluster_info =
            ClusterInfo::new(node.info.clone(), keypair, SocketAddrSpace::Unspecified);
        (node, cluster_info)
    }

    // #[test]
    // fn test_banking_stage_shutdown1() {
    //     let genesis_config = create_genesis_config(2).genesis_config;
    //     let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
    //     let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
    //     let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
    //     let banking_tracer = BankingTracer::new_disabled();
    //     let (non_vote_sender, non_vote_receiver) = banking_tracer.create_channel_non_vote();
    //     let (tpu_vote_sender, tpu_vote_receiver) = banking_tracer.create_channel_tpu_vote();
    //     let (gossip_vote_sender, gossip_vote_receiver) =
    //         banking_tracer.create_channel_gossip_vote();
    //     let ledger_path = get_tmp_ledger_path_auto_delete!();
    //     {
    //         let blockstore = Arc::new(
    //             Blockstore::open(ledger_path.path())
    //                 .expect("Expected to be able to open database ledger"),
    //         );
    //         let (exit, poh_recorder, poh_service, _entry_receiever) =
    //             create_test_recorder(&bank, &blockstore, None, None);
    //         let (_, cluster_info) = new_test_cluster_info(/*keypair:*/ None);
    //         let cluster_info = Arc::new(cluster_info);
    //         let (replay_vote_sender, _replay_vote_receiver) = unbounded();

    //         let banking_stage = BankingStage::new(
    //             &cluster_info,
    //             &poh_recorder,
    //             non_vote_receiver,
    //             tpu_vote_receiver,
    //             gossip_vote_receiver,
    //             None,
    //             replay_vote_sender,
    //             None,
    //             Arc::new(ConnectionCache::default()),
    //             bank_forks,
    //             &Arc::new(PrioritizationFeeCache::new(0u64)),
    //         );
    //         drop(non_vote_sender);
    //         drop(tpu_vote_sender);
    //         drop(gossip_vote_sender);
    //         exit.store(true, Ordering::Relaxed);
    //         banking_stage.join().unwrap();
    //         poh_service.join().unwrap();
    //     }
    //     Blockstore::destroy(ledger_path.path()).unwrap();
    // }

    // #[test]
    // fn test_banking_stage_tick() {
    //     solana_logger::setup();
    //     let GenesisConfigInfo {
    //         mut genesis_config, ..
    //     } = create_genesis_config(2);
    //     genesis_config.ticks_per_slot = 4;
    //     let num_extra_ticks = 2;
    //     let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
    //     let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
    //     let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
    //     let start_hash = bank.last_blockhash();
    //     let banking_tracer = BankingTracer::new_disabled();
    //     let (non_vote_sender, non_vote_receiver) = banking_tracer.create_channel_non_vote();
    //     let (tpu_vote_sender, tpu_vote_receiver) = banking_tracer.create_channel_tpu_vote();
    //     let (gossip_vote_sender, gossip_vote_receiver) =
    //         banking_tracer.create_channel_gossip_vote();
    //     let ledger_path = get_tmp_ledger_path_auto_delete!();
    //     {
    //         let blockstore = Arc::new(
    //             Blockstore::open(ledger_path.path())
    //                 .expect("Expected to be able to open database ledger"),
    //         );
    //         let poh_config = PohConfig {
    //             target_tick_count: Some(bank.max_tick_height() + num_extra_ticks),
    //             ..PohConfig::default()
    //         };
    //         let (exit, poh_recorder, poh_service, entry_receiver) =
    //             create_test_recorder(&bank, &blockstore, Some(poh_config), None);
    //         let (_, cluster_info) = new_test_cluster_info(/*keypair:*/ None);
    //         let cluster_info = Arc::new(cluster_info);
    //         let (replay_vote_sender, _replay_vote_receiver) = unbounded();

    //         let banking_stage = BankingStage::new(
    //             &cluster_info,
    //             &poh_recorder,
    //             non_vote_receiver,
    //             tpu_vote_receiver,
    //             gossip_vote_receiver,
    //             None,
    //             replay_vote_sender,
    //             None,
    //             Arc::new(ConnectionCache::default()),
    //             bank_forks,
    //             &Arc::new(PrioritizationFeeCache::new(0u64)),
    //         );
    //         trace!("sending bank");
    //         drop(non_vote_sender);
    //         drop(tpu_vote_sender);
    //         drop(gossip_vote_sender);
    //         exit.store(true, Ordering::Relaxed);
    //         poh_service.join().unwrap();
    //         drop(poh_recorder);

    //         trace!("getting entries");
    //         let entries: Vec<_> = entry_receiver
    //             .iter()
    //             .map(|(_bank, (entry, _tick_height))| entry)
    //             .collect();
    //         trace!("done");
    //         assert_eq!(entries.len(), genesis_config.ticks_per_slot as usize);
    //         assert!(entries.verify(&start_hash));
    //         assert_eq!(entries[entries.len() - 1].hash, bank.last_blockhash());
    //         banking_stage.join().unwrap();
    //     }
    //     Blockstore::destroy(ledger_path.path()).unwrap();
    // }

    pub fn convert_from_old_verified(
        mut with_vers: Vec<(PacketBatch, Vec<u8>)>,
    ) -> Vec<PacketBatch> {
        with_vers.iter_mut().for_each(|(b, v)| {
            b.iter_mut()
                .zip(v)
                .for_each(|(p, f)| p.meta_mut().set_discard(*f == 0))
        });
        with_vers.into_iter().map(|(b, _)| b).collect()
    }

    // #[test]
    // fn test_banking_stage_entries_only() {
    //     solana_logger::setup();
    //     let GenesisConfigInfo {
    //         genesis_config,
    //         mint_keypair,
    //         ..
    //     } = create_slow_genesis_config(10);
    //     let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
    //     let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
    //     let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
    //     let start_hash = bank.last_blockhash();
    //     let banking_tracer = BankingTracer::new_disabled();
    //     let (non_vote_sender, non_vote_receiver) = banking_tracer.create_channel_non_vote();
    //     let (tpu_vote_sender, tpu_vote_receiver) = banking_tracer.create_channel_tpu_vote();
    //     let (gossip_vote_sender, gossip_vote_receiver) =
    //         banking_tracer.create_channel_gossip_vote();
    //     let ledger_path = get_tmp_ledger_path_auto_delete!();
    //     {
    //         let blockstore = Arc::new(
    //             Blockstore::open(ledger_path.path())
    //                 .expect("Expected to be able to open database ledger"),
    //         );
    //         let poh_config = PohConfig {
    //             // limit tick count to avoid clearing working_bank at PohRecord then
    //             // PohRecorderError(MaxHeightReached) at BankingStage
    //             target_tick_count: Some(bank.max_tick_height() - 1),
    //             ..PohConfig::default()
    //         };
    //         let (exit, poh_recorder, poh_service, entry_receiver) =
    //             create_test_recorder(&bank, &blockstore, Some(poh_config), None);
    //         let (_, cluster_info) = new_test_cluster_info(/*keypair:*/ None);
    //         let cluster_info = Arc::new(cluster_info);
    //         let (replay_vote_sender, _replay_vote_receiver) = unbounded();

    //         let banking_stage = BankingStage::new(
    //             &cluster_info,
    //             &poh_recorder,
    //             non_vote_receiver,
    //             tpu_vote_receiver,
    //             gossip_vote_receiver,
    //             None,
    //             replay_vote_sender,
    //             None,
    //             Arc::new(ConnectionCache::default()),
    //             bank_forks,
    //             &Arc::new(PrioritizationFeeCache::new(0u64)),
    //         );

    //         // fund another account so we can send 2 good transactions in a single batch.
    //         let keypair = Keypair::new();
    //         let fund_tx =
    //             system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 2, start_hash);
    //         bank.process_transaction(&fund_tx).unwrap();

    //         // good tx
    //         let to = solana_sdk::pubkey::new_rand();
    //         let tx = system_transaction::transfer(&mint_keypair, &to, 1, start_hash);

    //         // good tx, but no verify
    //         let to2 = solana_sdk::pubkey::new_rand();
    //         let tx_no_ver = system_transaction::transfer(&keypair, &to2, 2, start_hash);

    //         // bad tx, AccountNotFound
    //         let keypair = Keypair::new();
    //         let to3 = solana_sdk::pubkey::new_rand();
    //         let tx_anf = system_transaction::transfer(&keypair, &to3, 1, start_hash);

    //         // send 'em over
    //         let packet_batches = to_packet_batches(&[tx_no_ver, tx_anf, tx], 3);

    //         // glad they all fit
    //         assert_eq!(packet_batches.len(), 1);

    //         let packet_batches = packet_batches
    //             .into_iter()
    //             .map(|batch| (batch, vec![0u8, 1u8, 1u8]))
    //             .collect();
    //         let packet_batches = convert_from_old_verified(packet_batches);
    //         non_vote_sender // no_ver, anf, tx
    //             .send(BankingPacketBatch::new((packet_batches, None)))
    //             .unwrap();

    //         drop(non_vote_sender);
    //         drop(tpu_vote_sender);
    //         drop(gossip_vote_sender);
    //         // wait until banking_stage to finish up all packets
    //         banking_stage.join().unwrap();

    //         exit.store(true, Ordering::Relaxed);
    //         poh_service.join().unwrap();
    //         drop(poh_recorder);

    //         let mut blockhash = start_hash;
    //         let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
    //         bank.process_transaction(&fund_tx).unwrap();
    //         //receive entries + ticks
    //         loop {
    //             let entries: Vec<Entry> = entry_receiver
    //                 .iter()
    //                 .map(|(_bank, (entry, _tick_height))| entry)
    //                 .collect();

    //             assert!(entries.verify(&blockhash));
    //             if !entries.is_empty() {
    //                 blockhash = entries.last().unwrap().hash;
    //                 for entry in entries {
    //                     bank.process_entry_transactions(entry.transactions)
    //                         .iter()
    //                         .for_each(|x| assert_eq!(*x, Ok(())));
    //                 }
    //             }

    //             if bank.get_balance(&to) == 1 {
    //                 break;
    //             }

    //             sleep(Duration::from_millis(200));
    //         }

    //         assert_eq!(bank.get_balance(&to), 1);
    //         assert_eq!(bank.get_balance(&to2), 0);

    //         drop(entry_receiver);
    //     }
    //     Blockstore::destroy(ledger_path.path()).unwrap();
    // }

    // #[test]
    // fn test_banking_stage_entryfication() {
    //     solana_logger::setup();
    //     // In this attack we'll demonstrate that a verifier can interpret the ledger
    //     // differently if either the server doesn't signal the ledger to add an
    //     // Entry OR if the verifier tries to parallelize across multiple Entries.
    //     let GenesisConfigInfo {
    //         genesis_config,
    //         mint_keypair,
    //         ..
    //     } = create_slow_genesis_config(2);
    //     let banking_tracer = BankingTracer::new_disabled();
    //     let (non_vote_sender, non_vote_receiver) = banking_tracer.create_channel_non_vote();

    //     // Process a batch that includes a transaction that receives two lamports.
    //     let alice = Keypair::new();
    //     let tx =
    //         system_transaction::transfer(&mint_keypair, &alice.pubkey(), 2, genesis_config.hash());

    //     let packet_batches = to_packet_batches(&[tx], 1);
    //     let packet_batches = packet_batches
    //         .into_iter()
    //         .map(|batch| (batch, vec![1u8]))
    //         .collect();
    //     let packet_batches = convert_from_old_verified(packet_batches);
    //     non_vote_sender
    //         .send(BankingPacketBatch::new((packet_batches, None)))
    //         .unwrap();

    //     // Process a second batch that uses the same from account, so conflicts with above TX
    //     let tx =
    //         system_transaction::transfer(&mint_keypair, &alice.pubkey(), 1, genesis_config.hash());
    //     let packet_batches = to_packet_batches(&[tx], 1);
    //     let packet_batches = packet_batches
    //         .into_iter()
    //         .map(|batch| (batch, vec![1u8]))
    //         .collect();
    //     let packet_batches = convert_from_old_verified(packet_batches);
    //     non_vote_sender
    //         .send(BankingPacketBatch::new((packet_batches, None)))
    //         .unwrap();

    //     let (tpu_vote_sender, tpu_vote_receiver) = banking_tracer.create_channel_tpu_vote();
    //     let (gossip_vote_sender, gossip_vote_receiver) =
    //         banking_tracer.create_channel_gossip_vote();
    //     let ledger_path = get_tmp_ledger_path_auto_delete!();
    //     {
    //         let (replay_vote_sender, _replay_vote_receiver) = unbounded();

    //         let entry_receiver = {
    //             // start a banking_stage to eat verified receiver
    //             let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
    //             let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
    //             let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
    //             let blockstore = Arc::new(
    //                 Blockstore::open(ledger_path.path())
    //                     .expect("Expected to be able to open database ledger"),
    //             );
    //             let poh_config = PohConfig {
    //                 // limit tick count to avoid clearing working_bank at
    //                 // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
    //                 target_tick_count: Some(bank.max_tick_height() - 1),
    //                 ..PohConfig::default()
    //             };
    //             let (exit, poh_recorder, poh_service, entry_receiver) =
    //                 create_test_recorder(&bank, &blockstore, Some(poh_config), None);
    //             let (_, cluster_info) = new_test_cluster_info(/*keypair:*/ None);
    //             let cluster_info = Arc::new(cluster_info);
    //             let _banking_stage = BankingStage::new_num_threads(
    //                 &cluster_info,
    //                 &poh_recorder,
    //                 non_vote_receiver,
    //                 tpu_vote_receiver,
    //                 gossip_vote_receiver,
    //                 3,
    //                 None,
    //                 replay_vote_sender,
    //                 None,
    //                 Arc::new(ConnectionCache::default()),
    //                 bank_forks,
    //                 &Arc::new(PrioritizationFeeCache::new(0u64)),
    //             );

    //             // wait for banking_stage to eat the packets
    //             while bank.get_balance(&alice.pubkey()) < 1 {
    //                 sleep(Duration::from_millis(10));
    //             }
    //             exit.store(true, Ordering::Relaxed);
    //             poh_service.join().unwrap();
    //             entry_receiver
    //         };
    //         drop(non_vote_sender);
    //         drop(tpu_vote_sender);
    //         drop(gossip_vote_sender);

    //         // consume the entire entry_receiver, feed it into a new bank
    //         // check that the balance is what we expect.
    //         let entries: Vec<_> = entry_receiver
    //             .iter()
    //             .map(|(_bank, (entry, _tick_height))| entry)
    //             .collect();

    //         let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
    //         for entry in entries {
    //             bank.process_entry_transactions(entry.transactions)
    //                 .iter()
    //                 .for_each(|x| assert_eq!(*x, Ok(())));
    //         }

    //         // Assert the user doesn't hold three lamports. If the stage only outputs one
    //         // entry, then one of the transactions will be rejected, because it drives
    //         // the account balance below zero before the credit is added.
    //         assert!(bank.get_balance(&alice.pubkey()) != 3);
    //     }
    //     Blockstore::destroy(ledger_path.path()).unwrap();
    // }

    #[test]
    fn test_bank_record_transactions() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let blockstore = Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger");
            let (poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
                // TODO use record_receiver
                bank.tick_height(),
                bank.last_blockhash(),
                bank.clone(),
                None,
                bank.ticks_per_slot(),
                &Pubkey::default(),
                &Arc::new(blockstore),
                &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
                &PohConfig::default(),
                Arc::new(AtomicBool::default()),
            );
            let recorder = poh_recorder.new_recorder();
            let poh_recorder = Arc::new(RwLock::new(poh_recorder));

            let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

            poh_recorder.write().unwrap().set_bank(&bank, false);
            let pubkey = solana_sdk::pubkey::new_rand();
            let keypair2 = Keypair::new();
            let pubkey2 = solana_sdk::pubkey::new_rand();

            let txs = vec![
                system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_config.hash())
                    .into(),
                system_transaction::transfer(&keypair2, &pubkey2, 1, genesis_config.hash()).into(),
            ];

            let _ = recorder.record_transactions(bank.slot(), txs.clone());
            let (_bank, (entry, _tick_height)) = entry_receiver.recv().unwrap();
            assert_eq!(entry.transactions, txs);

            // Once bank is set to a new bank (setting bank.slot() + 1 in record_transactions),
            // record_transactions should throw MaxHeightReached
            let next_slot = bank.slot() + 1;
            let RecordTransactionsSummary { result, .. } =
                recorder.record_transactions(next_slot, txs);
            assert_matches!(result, Err(PohRecorderError::MaxHeightReached));
            // Should receive nothing from PohRecorder b/c record failed
            assert!(entry_receiver.try_recv().is_err());

            poh_recorder
                .read()
                .unwrap()
                .is_exited
                .store(true, Ordering::Relaxed);
            let _ = poh_simulator.join();
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    pub(crate) fn create_slow_genesis_config(lamports: u64) -> GenesisConfigInfo {
        create_slow_genesis_config_with_leader(lamports, &solana_sdk::pubkey::new_rand())
    }

    pub(crate) fn create_slow_genesis_config_with_leader(
        lamports: u64,
        validator_pubkey: &Pubkey,
    ) -> GenesisConfigInfo {
        let mut config_info = create_genesis_config_with_leader(
            lamports,
            validator_pubkey,
            // See solana_ledger::genesis_utils::create_genesis_config.
            bootstrap_validator_stake_lamports(),
        );

        // For these tests there's only 1 slot, don't want to run out of ticks
        config_info.genesis_config.ticks_per_slot *= 8;
        config_info
    }

    pub(crate) fn simulate_poh(
        record_receiver: Receiver<Record>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) -> JoinHandle<()> {
        let poh_recorder = poh_recorder.clone();
        let is_exited = poh_recorder.read().unwrap().is_exited.clone();
        let tick_producer = Builder::new()
            .name("solana-simulate_poh".to_string())
            .spawn(move || loop {
                PohService::read_record_receiver_and_process(
                    &poh_recorder,
                    &record_receiver,
                    Duration::from_millis(10),
                );
                if is_exited.load(Ordering::Relaxed) {
                    break;
                }
            });
        tick_producer.unwrap()
    }

    //     #[test]
    //     fn test_unprocessed_transaction_storage_full_send() {
    //         solana_logger::setup();
    //         let GenesisConfigInfo {
    //             mut genesis_config,
    //             mint_keypair,
    //             ..
    //         } = create_slow_genesis_config(10000);
    //         activate_feature(
    //             &mut genesis_config,
    //             allow_votes_to_directly_update_vote_state::id(),
    //         );
    //         let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
    //         let bank_forks = Arc::new(RwLock::new(BankForks::new(bank)));
    //         let bank = Arc::new(bank_forks.read().unwrap().get(0).unwrap());
    //         let start_hash = bank.last_blockhash();
    //         let banking_tracer = BankingTracer::new_disabled();
    //         let (non_vote_sender, non_vote_receiver) = banking_tracer.create_channel_non_vote();
    //         let (tpu_vote_sender, tpu_vote_receiver) = banking_tracer.create_channel_tpu_vote();
    //         let (gossip_vote_sender, gossip_vote_receiver) =
    //             banking_tracer.create_channel_gossip_vote();
    //         let ledger_path = get_tmp_ledger_path_auto_delete!();
    //         {
    //             let blockstore = Arc::new(
    //                 Blockstore::open(ledger_path.path())
    //                     .expect("Expected to be able to open database ledger"),
    //             );
    //             let poh_config = PohConfig {
    //                 // limit tick count to avoid clearing working_bank at PohRecord then
    //                 // PohRecorderError(MaxHeightReached) at BankingStage
    //                 target_tick_count: Some(bank.max_tick_height() - 1),
    //                 ..PohConfig::default()
    //             };
    //             let (exit, poh_recorder, poh_service, _entry_receiver) =
    //                 create_test_recorder(&bank, &blockstore, Some(poh_config), None);
    //             let (_, cluster_info) = new_test_cluster_info(/*keypair:*/ None);
    //             let cluster_info = Arc::new(cluster_info);
    //             let (replay_vote_sender, _replay_vote_receiver) = unbounded();

    //             let banking_stage = BankingStage::new(
    //                 &cluster_info,
    //                 &poh_recorder,
    //                 non_vote_receiver,
    //                 tpu_vote_receiver,
    //                 gossip_vote_receiver,
    //                 None,
    //                 replay_vote_sender,
    //                 None,
    //                 Arc::new(ConnectionCache::default()),
    //                 bank_forks,
    //                 &Arc::new(PrioritizationFeeCache::new(0u64)),
    //             );

    //             let keypairs = (0..100).map(|_| Keypair::new()).collect_vec();
    //             let vote_keypairs = (0..100).map(|_| Keypair::new()).collect_vec();
    //             for keypair in keypairs.iter() {
    //                 bank.process_transaction(&system_transaction::transfer(
    //                     &mint_keypair,
    //                     &keypair.pubkey(),
    //                     20,
    //                     start_hash,
    //                 ))
    //                 .unwrap();
    //             }

    //             // Send a bunch of votes and transfers
    //             let tpu_votes = (0..100_usize)
    //                 .map(|i| {
    //                     new_vote_state_update_transaction(
    //                         VoteStateUpdate::from(vec![
    //                             (0, 8),
    //                             (1, 7),
    //                             (i as u64 + 10, 6),
    //                             (i as u64 + 11, 1),
    //                         ]),
    //                         Hash::new_unique(),
    //                         &keypairs[i],
    //                         &vote_keypairs[i],
    //                         &vote_keypairs[i],
    //                         None,
    //                     );
    //                 })
    //                 .collect_vec();
    //             let gossip_votes = (0..100_usize)
    //                 .map(|i| {
    //                     new_vote_state_update_transaction(
    //                         VoteStateUpdate::from(vec![
    //                             (0, 8),
    //                             (1, 7),
    //                             (i as u64 + 64 + 5, 6),
    //                             (i as u64 + 7, 1),
    //                         ]),
    //                         Hash::new_unique(),
    //                         &keypairs[i],
    //                         &vote_keypairs[i],
    //                         &vote_keypairs[i],
    //                         None,
    //                     );
    //                 })
    //                 .collect_vec();
    //             let txs = (0..100_usize)
    //                 .map(|i| {
    //                     system_transaction::transfer(
    //                         &keypairs[i],
    //                         &keypairs[(i + 1) % 100].pubkey(),
    //                         10,
    //                         start_hash,
    //                     );
    //                 })
    //                 .collect_vec();

    //             let non_vote_packet_batches = to_packet_batches(&txs, 10);
    //             let tpu_packet_batches = to_packet_batches(&tpu_votes, 10);
    //             let gossip_packet_batches = to_packet_batches(&gossip_votes, 10);

    //             // Send em all
    //             [
    //                 (non_vote_packet_batches, non_vote_sender),
    //                 (tpu_packet_batches, tpu_vote_sender),
    //                 (gossip_packet_batches, gossip_vote_sender),
    //             ]
    //             .into_iter()
    //             .map(|(packet_batches, sender)| {
    //                 Builder::new()
    //                     .spawn(move || {
    //                         sender
    //                             .send(BankingPacketBatch::new((packet_batches, None)))
    //                             .unwrap()
    //                     })
    //                     .unwrap()
    //             })
    //             .for_each(|handle| handle.join().unwrap());

    //             banking_stage.join().unwrap();
    //             exit.store(true, Ordering::Relaxed);
    //             poh_service.join().unwrap();
    //         }
    //         Blockstore::destroy(ledger_path.path()).unwrap();
    //     }
}
