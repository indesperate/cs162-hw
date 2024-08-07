//! The MapReduce coordinator.
//!

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tonic::transport::Server;
use tonic::Code;
use tonic::{Request, Response, Status};

use crate::app::named;
use crate::rpc::{coordinator::*, worker};
use crate::{
    JobId, TaskNumber, WorkerId, COORDINATOR_ADDR, INITIAL_JOB_ID, INITIAL_WORKER_ID,
    TASK_TIMEOUT_SECS,
};

pub mod args;

/// a type for Coordinator state
pub struct CoordinatorState {
    free_worker_id: WorkerId,
    workers: Vec<WorkerState>,
    free_job_id: JobId,
    jobs: HashMap<JobId, JobState>,
    job_queue: VecDeque<JobId>,
}

#[derive(Debug, Clone)]
pub struct WorkerTask {
    job: JobId,
    task: TaskNumber,
    reduce: bool,
}

/// a type for track worker state
#[derive(Debug, Clone)]
pub struct WorkerState {
    worker_id: WorkerId,
    last_alive_time: Instant,
    tasks: Vec<WorkerTask>,
}

#[derive(Debug, Clone)]
pub enum TaskState {
    Idle,
    Running,
    Done,
}

#[derive(Debug, Clone)]
pub struct MapTask {
    id: TaskNumber,
    file: String,
    state: TaskState,
}

#[derive(Debug, Clone)]
pub struct ReduceTask {
    id: TaskNumber,
    state: TaskState,
}

#[derive(Debug)]
pub enum JobProcess {
    Map,
    Reduce,
    Running,
    Done,
}

pub struct JobState {
    map_tasks: Vec<MapTask>,
    reduce_tasks: Vec<ReduceTask>,
    map_workers: Vec<MapTaskAssignment>,
    output_dir: String,
    app: String,
    n_reduce: u32,
    args: Vec<u8>,
    job_process: JobProcess,
}

impl JobState {
    pub fn new(job: SubmitJobRequest) -> Self {
        let mut reduce_tasks = Vec::new();
        for id in 0..job.n_reduce {
            reduce_tasks.push(ReduceTask {
                id: id.try_into().unwrap(),
                state: TaskState::Idle,
            })
        }
        JobState {
            map_tasks: job
                .files
                .into_iter()
                .enumerate()
                .map(|(id, file)| MapTask {
                    id,
                    file,
                    state: TaskState::Idle,
                })
                .collect(),
            reduce_tasks,
            map_workers: Vec::new(),
            output_dir: job.output_dir,
            app: job.app,
            n_reduce: job.n_reduce,
            args: job.args,
            job_process: JobProcess::Map,
        }
    }

    pub fn get_reduce_job(&mut self) -> Option<&ReduceTask> {
        let task = self
            .reduce_tasks
            .iter_mut()
            .find(|t| matches!(t.state, TaskState::Idle))?;
        task.state = TaskState::Running;
        Some(task)
    }

    pub fn get_map_job(&mut self) -> Option<&MapTask> {
        let task = self
            .map_tasks
            .iter_mut()
            .find(|t| matches!(t.state, TaskState::Idle))?;
        task.state = TaskState::Running;
        Some(task)
    }

    pub fn check_reduce_is_running(&mut self) {
        if self
            .reduce_tasks
            .iter()
            .all(|t| matches!(t.state, TaskState::Done | TaskState::Running))
        {
            self.job_process = JobProcess::Running;
        }
    }

    pub fn check_map_is_running(&mut self) {
        if self
            .map_tasks
            .iter()
            .all(|t| matches!(t.state, TaskState::Done | TaskState::Running))
        {
            self.job_process = JobProcess::Running;
        }
    }
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

    async fn check_workers(&self) {
        let mut inner = self.inner.lock().await;
        for crash_worker in Coordinator::get_crash_workers(&mut inner.workers).into_iter() {
            for task in crash_worker.tasks.into_iter() {
                let job: u32 = task.job;
                let job_state = inner.jobs.get_mut(&job).expect("Job not in job map!!!");
                if task.reduce {
                    let reduce_task = job_state
                        .reduce_tasks
                        .get_mut(task.task)
                        .expect("Task not in reduce task");
                    if let TaskState::Running = reduce_task.state {
                        reduce_task.state = TaskState::Idle;
                        job_state.job_process = JobProcess::Reduce;
                        job_state
                            .map_workers
                            .retain(|map| map.worker_id != crash_worker.worker_id)
                    }
                } else {
                    let map_task = job_state
                        .map_tasks
                        .get_mut(task.task)
                        .expect("Task not in map task");
                    map_task.state = TaskState::Idle;
                    job_state.job_process = JobProcess::Map;
                    job_state
                        .map_workers
                        .retain(|map| map.worker_id != crash_worker.worker_id);
                    for reduce_task in job_state.reduce_tasks.iter_mut() {
                        if let TaskState::Running = reduce_task.state {
                            reduce_task.state = TaskState::Idle;
                        }
                    }
                }
            }
        }
    }

    pub fn get_crash_workers(workers: &mut Vec<WorkerState>) -> Vec<WorkerState> {
        let mut crash_workers = Vec::new();
        let now = Instant::now();
        workers.retain(|worker| {
            let time_out = now - worker.last_alive_time;
            if time_out.as_secs() >= TASK_TIMEOUT_SECS {
                crash_workers.push(worker.clone());
                log::info!("crash worker detected {}", worker.worker_id);
                false
            } else {
                true
            }
        });
        crash_workers
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
        inner.jobs.insert(job_id, JobState::new(job));
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
            done: matches!(job_state.job_process, JobProcess::Done),
            failed: false,
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
            tasks: Vec::new(),
        });
        log::info!("Worker register with {}", worker_id);
        Ok(Response::new(RegisterReply { worker_id }))
    }

    async fn get_task(
        &self,
        req: Request<GetTaskRequest>,
    ) -> Result<Response<GetTaskReply>, Status> {
        self.check_workers().await;
        let mut inner = self.inner.lock().await;
        let mut task = GetTaskReply {
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
        };
        // return idle task
        let job_id = match inner.job_queue.iter().find(|id| {
            matches!(
                inner.jobs.get(id).expect("Job not in job map").job_process,
                JobProcess::Map | JobProcess::Reduce
            )
        }) {
            Some(&j) => j,
            None => return Ok(Response::new(task)),
        };
        task.job_id = job_id;
        let job_state = inner.jobs.get_mut(&job_id).expect("Job not in job map!!!");
        task.output_dir = job_state.output_dir.clone();
        task.app = job_state.app.clone();
        task.n_reduce = job_state.n_reduce;
        task.n_map = job_state.map_tasks.len().try_into().unwrap();
        task.args = job_state.args.clone();
        let worker_id = req.into_inner().worker_id;
        match job_state.job_process {
            JobProcess::Map => {
                if let Some(map_task) = job_state.get_map_job() {
                    task.task = map_task.id.try_into().unwrap();
                    log::info!("Send Map job {} to worker {}!!", task.task, worker_id);
                    task.file = map_task.file.clone();
                    task.reduce = false;
                    task.wait = false;
                    job_state.map_workers.push(MapTaskAssignment {
                        worker_id,
                        task: task.task,
                    });
                }
                job_state.check_map_is_running();
            }
            JobProcess::Reduce => {
                if let Some(reduce_task) = job_state.get_reduce_job() {
                    log::info!("Send reduce job {} to worker {} !!", task.task, worker_id);
                    task.task = reduce_task.id.try_into().unwrap();
                    task.reduce = true;
                    task.wait = false;
                    task.map_task_assignments = job_state.map_workers.clone();
                }
                job_state.check_reduce_is_running();
            }
            _ => return Ok(Response::new(task)),
        };
        // set worker status
        let worker_state = inner
            .workers
            .iter_mut()
            .find(|worker| worker.worker_id == worker_id)
            .expect("Worker not in worker state");
        worker_state.tasks.push(WorkerTask {
            job: job_id,
            task: task.task.try_into().unwrap(),
            reduce: task.reduce,
        });
        Ok(Response::new(task))
    }

    async fn finish_task(
        &self,
        req: Request<FinishTaskRequest>,
    ) -> Result<Response<FinishTaskReply>, Status> {
        let task_req = req.into_inner();
        let mut inner = self.inner.lock().await;
        let job_state = inner
            .jobs
            .get_mut(&task_req.job_id)
            .expect("Job not in job map!!!");
        let task_id: usize = task_req.task.try_into().unwrap();
        log::info!(
            "Job done with {} {}, reduce ? {}",
            task_req.worker_id,
            task_req.task,
            task_req.reduce
        );
        if task_req.reduce {
            let reduce_task = job_state
                .reduce_tasks
                .get_mut(task_id)
                .expect("Task not in reduce task");
            reduce_task.state = TaskState::Done;
            if job_state
                .reduce_tasks
                .iter()
                .all(|t| matches!(t.state, TaskState::Done))
            {
                job_state.job_process = JobProcess::Done;
                inner.job_queue.pop_front();
            }
        } else {
            let map_task = job_state
                .map_tasks
                .get_mut(task_id)
                .expect("Task not in map task!!");
            map_task.state = TaskState::Done;
            if job_state
                .map_tasks
                .iter()
                .all(|t| matches!(t.state, TaskState::Done))
            {
                job_state.job_process = JobProcess::Reduce;
            }
        }
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
