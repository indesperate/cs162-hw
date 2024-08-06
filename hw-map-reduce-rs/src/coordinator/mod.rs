//! The MapReduce coordinator.
//!

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tonic::transport::Server;
use tonic::Code;
use tonic::{Request, Response, Status};

use crate::app::named;
use crate::rpc::coordinator::*;
use crate::{JobId, WorkerId, COORDINATOR_ADDR, INITIAL_JOB_ID, INITIAL_WORKER_ID};

pub mod args;

/// a type for Coordinator state
pub struct CoordinatorState {
    free_worker_id: WorkerId,
    workers: Vec<WorkerState>,
    free_job_id: JobId,
    jobs: HashMap<JobId, JobState>,
    job_queue: VecDeque<JobId>,
}

/// a type for track worker state
pub struct WorkerState {
    worker_id: WorkerId,
    last_alive_time: Instant,
}

pub struct JobState {
    files: Vec<String>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    args: Bytes,
    done: bool,
    failed: bool,
}

pub struct Coordinator {
    inner: Arc<Mutex<CoordinatorState>>, // TODO: add your own fields
}

impl Coordinator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CoordinatorState {
                free_worker_id: INITIAL_WORKER_ID,
                workers: Vec::new(),
                free_job_id: INITIAL_JOB_ID,
                jobs: HashMap::new(),
                job_queue: VecDeque::new(),
            })),
        }
    }
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl coordinator_server::Coordinator for Coordinator {
    /// An example RPC.
    ///
    /// Feel free to delete this.
    /// Make sure to also delete the RPC in `proto/coordinator.proto`.
    async fn example(
        &self,
        req: Request<ExampleRequest>,
    ) -> Result<Response<ExampleReply>, Status> {
        let req = req.get_ref();
        let message = format!("Hello, {}!", req.name);
        Ok(Response::new(ExampleReply { message }))
    }

    async fn submit_job(
        &self,
        req: Request<SubmitJobRequest>,
    ) -> Result<Response<SubmitJobReply>, Status> {
        let mut inner = self.inner.lock().await;
        let job_id = inner.free_job_id;
        inner.free_job_id += 1;
        // read jobrequest
        let job = req.into_inner();
        named(&job.app).map_err(|e| Status::new(Code::InvalidArgument, e.to_string()))?;
        // add to queue and map
        inner.jobs.insert(
            job_id,
            JobState {
                files: job.files,
                output_dir: job.output_dir,
                app: job.app,
                n_reduce: job.n_reduce,
                args: job.args.into(),
                done: false,
                failed: false,
            },
        );
        inner.job_queue.push_back(job_id);

        log::info!("Receive job {}", job_id);

        Ok(Response::new(SubmitJobReply { job_id }))
    }

    async fn poll_job(
        &self,
        req: Request<PollJobRequest>,
    ) -> Result<Response<PollJobReply>, Status> {
        let inner = self.inner.lock().await;
        let job_id = req.into_inner().job_id;
        log::info!("Poll job {}", job_id);
        let job_state = match inner.jobs.get(&job_id) {
            Some(s) => s,
            None => return Err(Status::new(Code::NotFound, "job id is invalid")),
        };

        Ok(Response::new(PollJobReply {
            done: job_state.done,
            failed: job_state.failed,
            errors: Vec::new(),
        }))
    }

    async fn heartbeat(
        &self,
        req: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let mut inner = self.inner.lock().await;
        let worker_id = req.into_inner().worker_id;
        for worker in inner.workers.iter_mut() {
            if worker.worker_id == worker_id {
                worker.last_alive_time = Instant::now();
            }
        }
        log::info!("Accept worker {} heartbeat", worker_id);
        Ok(Response::new(HeartbeatReply {}))
    }

    async fn register(
        &self,
        _req: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let mut inner = self.inner.lock().await;
        let worker_id = inner.free_worker_id;
        inner.free_worker_id += 1;
        inner.workers.push(WorkerState {
            worker_id,
            last_alive_time: Instant::now(),
        });
        log::info!("Worker register with {}", worker_id);
        Ok(Response::new(RegisterReply { worker_id }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        // TODO: Tasks
        Ok(Response::new(GetTaskReply {
            job_id: 0,
            output_dir: "".to_string(),
            app: "".to_string(),
            task: 0,
            file: "".to_string(),
            n_reduce: 0,
            n_map: 0,
            reduce: false,
            wait: true,
            map_task_assignments: Vec::new(),
            args: Vec::new(),
        }))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        // TODO: Tasks
        Ok(Response::new(FinishTaskReply {}))
    }

    async fn fail_task(
        &self,
        req: Request<FailTaskRequest>,
    ) -> Result<Response<FailTaskReply>, Status> {
        // TODO: Fault tolerance
        Ok(Response::new(FailTaskReply {}))
    }
}

pub async fn start(_args: args::Args) -> Result<()> {
    let addr = COORDINATOR_ADDR.parse().unwrap();

    let coordinator = Coordinator::new();
    let svc = coordinator_server::CoordinatorServer::new(coordinator);
    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
