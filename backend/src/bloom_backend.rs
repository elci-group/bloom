mod api {
    tonic::include_proto!("bloom_backend.proto");
}

#[derive(tonic::Server)]
#[derive(Debug)]
pub struct BloomBackend {
    // fields for node registry, job queue, etc.
}

#[tonic::async_trait]
impl BloomService for BloomBackend {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Result<RegisterNodeResponse, Status>, Status> {
        // TODO: implement node registration and load balancing
        Ok(Ok(RegisterNodeResponse {
            success: true,
            message: "registered".into(),
        }))
    }

    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Result<ListNodesResponse, Status>, Status> {
        // TODO: return current node list sorted by load
        Ok(Ok(ListNodesResponse { nodes: vec![] }))
    }

    async fn submit_job(
        &self,
        request: Request<JobRequest>,
    ) -> Result<Result<JobResponse, Status>, Status> {
        // TODO: queue job, return job_id
        Ok(Ok(JobResponse {
            job_id: "job-1".into(),
            status: "queued".into(),
        }))
    }

    type job_status_stream =
        StreamingResult<Result<JobStatusResponse, Status>, Status>;

    fn job_status(
        &self,
        request: Request<JobStatusRequest>,
    ) -> Self::job_status_stream {
        // TODO: implement streaming status
        tonic::Streaming::new(())
    }
}