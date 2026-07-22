//! A controlled head-of-line workload for cached PEP 658 wheel metadata.
//!
//! The workload deliberately drives `DistributionDatabase`, rather than a local semaphore or a
//! `RegistryClient` loopback. It prewarms fresh PEP 658 metadata entries, starts enough distinct
//! delayed metadata requests to occupy the database's real `downloads_semaphore`, then measures
//! the cached class while that lane is occupied.

use std::future::Future;
use std::hint::black_box;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use uv_cache::Cache;
use uv_client::{BaseClientBuilder, RegistryClient, RegistryClientBuilder};
use uv_configuration::{BuildOptions, Concurrency, Constraints, IndexStrategy, NoSources};
use uv_dispatch::{BuildDispatch, SharedState};
use uv_distribution::DistributionDatabase;
use uv_distribution_filename::WheelFilename;
use uv_distribution_types::{
    BuiltDist, ConfigSettings, DependencyMetadata, Dist, ExtraBuildRequires, ExtraBuildVariables,
    File, FileLocation, HashPolicy, IndexLocations, IndexUrl, PackageConfigSettings,
    RegistryBuiltDist, RegistryBuiltWheel,
};
use uv_install_wheel::LinkMode;
use uv_preview::Preview;
use uv_pypi_types::HashDigests;
use uv_python::Interpreter;
use uv_resolver::{ExcludeNewer, FlatIndex};
use uv_types::{BuildContext, BuildIsolation, HashStrategy, SourceTreeEditablePolicy};
use uv_workspace::WorkspaceCache;

const DOWNLOADS: usize = Concurrency::DEFAULT_DOWNLOADS;
const CACHE_HITS: usize = 16;
const SLOW_RESPONSE_DELAY: Duration = Duration::from_millis(75);

struct ArrivalSignal {
    target: usize,
    arrivals: AtomicUsize,
    sender: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    receiver: tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl ArrivalSignal {
    fn new(target: usize) -> Self {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        Self {
            target,
            arrivals: AtomicUsize::new(0),
            sender: Mutex::new(Some(sender)),
            receiver: tokio::sync::Mutex::new(Some(receiver)),
        }
    }

    fn record(&self) {
        let arrived = self.arrivals.fetch_add(1, Ordering::SeqCst) + 1;
        if arrived == self.target {
            if let Some(sender) = self.sender.lock().unwrap().take() {
                let _ = sender.send(());
            }
        }
    }

    fn count(&self) -> usize {
        self.arrivals.load(Ordering::SeqCst)
    }

    async fn wait_for_target(&self) -> Result<()> {
        let receiver = self
            .receiver
            .lock()
            .await
            .take()
            .context("arrival target was already awaited")?;
        receiver
            .await
            .context("arrival target sender was dropped before the target was reached")?;
        Ok(())
    }
}

/// Holds delayed PEP 658 responses until the test has observed their arrival. The gate makes the
/// arrival, hold, and release conditions deterministic; the response delay begins only after the
/// test releases the gate.
struct SlowGate {
    arrivals: ArrivalSignal,
    released: Mutex<bool>,
    release_changed: Condvar,
}

impl SlowGate {
    fn new(arrival_target: usize) -> Self {
        Self {
            arrivals: ArrivalSignal::new(arrival_target),
            released: Mutex::new(false),
            release_changed: Condvar::new(),
        }
    }

    fn arrive_and_wait(&self) {
        self.arrivals.record();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.release_changed.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release_changed.notify_all();
    }

    fn arrival_count(&self) -> usize {
        self.arrivals.count()
    }

    async fn wait_for_arrivals(&self) -> Result<()> {
        self.arrivals.wait_for_target().await
    }
}

#[derive(Default)]
struct ServerStats {
    requests: AtomicUsize,
}

/// A tiny local PEP 658 server. It holds real metadata HTTP responses for package names that
/// start with `slow`; cached package names receive the same valid metadata without a delay.
struct MetadataServer {
    base_url: String,
    stats: Arc<ServerStats>,
    slow_gate: Arc<SlowGate>,
    stopping: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl MetadataServer {
    fn start(slow_arrival_target: usize) -> Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let base_url = format!("http://{}", listener.local_addr()?);
        let stats = Arc::new(ServerStats::default());
        let slow_gate = Arc::new(SlowGate::new(slow_arrival_target));
        let stopping = Arc::new(AtomicBool::new(false));
        let workers = Arc::new(Mutex::new(Vec::new()));

        let accept_stats = Arc::clone(&stats);
        let accept_slow_gate = Arc::clone(&slow_gate);
        let accept_stopping = Arc::clone(&stopping);
        let accept_workers = Arc::clone(&workers);
        let accept_thread = thread::Builder::new()
            .name("cached-metadata-server".to_string())
            .spawn(move || {
                while !accept_stopping.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let stats = Arc::clone(&accept_stats);
                            let slow_gate = Arc::clone(&accept_slow_gate);
                            let worker =
                                thread::spawn(move || serve_connection(stream, stats, slow_gate));
                            accept_workers.lock().unwrap().push(worker);
                        }
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => break,
                    }
                }
            })
            .context("failed to start local metadata server")?;

        Ok(Self {
            base_url,
            stats,
            slow_gate,
            stopping,
            accept_thread: Some(accept_thread),
            workers,
        })
    }

    fn base_url(&self) -> &str {
        &self.base_url
    }

    fn requests(&self) -> usize {
        self.stats.requests.load(Ordering::SeqCst)
    }

    fn slow_gate(&self) -> Arc<SlowGate> {
        Arc::clone(&self.slow_gate)
    }
}

impl Drop for MetadataServer {
    fn drop(&mut self) {
        self.slow_gate.release();
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
        for worker in self.workers.lock().unwrap().drain(..) {
            let _ = worker.join();
        }
    }
}

fn serve_connection(mut stream: TcpStream, stats: Arc<ServerStats>, slow_gate: Arc<SlowGate>) {
    let Ok(request) = read_request(&mut stream) else {
        return;
    };
    let Some(path) = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
    else {
        return;
    };

    stats.requests.fetch_add(1, Ordering::SeqCst);
    let is_slow = path
        .rsplit('/')
        .next()
        .is_some_and(|filename| filename.starts_with("slow"));
    if is_slow {
        slow_gate.arrive_and_wait();
        thread::sleep(SLOW_RESPONSE_DELAY);
    }

    let body = metadata_body(path);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nCache-Control: max-age=600\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn read_request(stream: &mut TcpStream) -> io::Result<String> {
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(request).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

fn metadata_body(path: &str) -> String {
    let filename = path.rsplit('/').next().unwrap_or_default();
    let wheel = filename.strip_suffix(".metadata").unwrap_or(filename);
    let name = wheel
        .strip_suffix("-1.0.0-py3-none-any.whl")
        .unwrap_or("unknown");
    format!("Metadata-Version: 2.3\nName: {name}\nVersion: 1.0.0\n")
}

/// Owns the production components required to construct a `BuildDispatch` for the database.
/// The benchmark only uses the metadata path, but using `BuildDispatch` ensures the database has
/// its normal client, cache, Git resolver, capabilities, and shared download semaphore.
struct Fixture {
    server: MetadataServer,
    cache: Cache,
    client: RegistryClient,
    concurrency: Concurrency,
    build_constraints: Constraints,
    interpreter: Interpreter,
    index_locations: IndexLocations,
    flat_index: FlatIndex,
    dependency_metadata: DependencyMetadata,
    config_settings: ConfigSettings,
    config_settings_package: PackageConfigSettings,
    extra_build_requires: ExtraBuildRequires,
    extra_build_variables: ExtraBuildVariables,
    build_options: BuildOptions,
    hashes: HashStrategy,
}

impl Fixture {
    async fn new(downloads: usize, slow_arrival_target: usize) -> Result<Self> {
        let server = MetadataServer::start(slow_arrival_target)?;
        let cache = Cache::temp()?.init().await?;
        let interpreter = Interpreter::query(find_python()?, &cache)?;
        let concurrency = Concurrency::new(downloads, 1, 1, CACHE_HITS);
        let client = RegistryClientBuilder::new(
            BaseClientBuilder::default().cache_read_concurrency(concurrency.cache_reads),
            cache.clone(),
        )
        .build()?;

        Ok(Self {
            server,
            cache,
            client,
            concurrency,
            build_constraints: Constraints::default(),
            interpreter,
            index_locations: IndexLocations::default(),
            flat_index: FlatIndex::default(),
            dependency_metadata: DependencyMetadata::default(),
            config_settings: ConfigSettings::default(),
            config_settings_package: PackageConfigSettings::default(),
            extra_build_requires: ExtraBuildRequires::default(),
            extra_build_variables: ExtraBuildVariables::default(),
            build_options: BuildOptions::default(),
            hashes: HashStrategy::default(),
        })
    }

    fn build_context(&self) -> BuildDispatch<'_> {
        BuildDispatch::new(
            &self.client,
            &self.cache,
            &self.build_constraints,
            &self.interpreter,
            &self.index_locations,
            &self.flat_index,
            &self.dependency_metadata,
            SharedState::default(),
            IndexStrategy::default(),
            &self.config_settings,
            &self.config_settings_package,
            BuildIsolation::default(),
            &self.extra_build_requires,
            &self.extra_build_variables,
            LinkMode::default(),
            &self.build_options,
            &self.hashes,
            ExcludeNewer::default(),
            NoSources::default(),
            SourceTreeEditablePolicy::Project,
            WorkspaceCache::default(),
            self.concurrency.clone(),
            Preview::default(),
        )
    }

    fn distribution(&self, name: &str) -> Result<Dist> {
        let filename = WheelFilename::from_str(&format!("{name}-1.0.0-py3-none-any.whl"))?;
        let url = format!("{}/wheels/{filename}", self.server.base_url());
        let index = IndexUrl::from_str(&format!("{}/simple", self.server.base_url()))?;
        let file = File {
            dist_info_metadata: true,
            filename: filename.to_string().into(),
            hashes: HashDigests::empty(),
            requires_python: None,
            size: None,
            upload_time_utc_ms: None,
            url: FileLocation::new(url.clone().into(), &url.into()),
            yanked: None,
            zstd: None,
        };

        Ok(Dist::Built(BuiltDist::Registry(RegistryBuiltDist {
            wheels: vec![RegistryBuiltWheel {
                filename,
                file: Box::new(file),
                index,
            }],
            best_wheel_index: 0,
            sdist: None,
        })))
    }
}

fn find_python() -> Result<PathBuf> {
    if let Some(configured) = std::env::var_os("PYTHON") {
        let configured = PathBuf::from(configured);
        if configured.is_file() {
            return configured
                .canonicalize()
                .context("failed to canonicalize the configured Python interpreter");
        }
    }

    let path = std::env::var_os("PATH").context("PATH is required to locate Python")?;
    for directory in std::env::split_paths(&path) {
        for name in ["python3", "python", "python.exe"] {
            let candidate = directory.join(name);
            if candidate.is_file() {
                return candidate
                    .canonicalize()
                    .context("failed to canonicalize the Python interpreter");
            }
        }
    }

    bail!("a Python interpreter is required to construct BuildDispatch")
}

async fn fetch_metadata<ContextType: BuildContext>(
    database: &DistributionDatabase<'_, ContextType>,
    dist: &Dist,
) -> Result<()> {
    // The database validates the package name before returning. Keep the parsed metadata live so
    // the compiler cannot discard the operation that produced it.
    black_box(
        database
            .get_or_build_wheel_metadata(dist, HashPolicy::None)
            .await?,
    );
    Ok(())
}

/// Starts timing before the database operation and signals only after that operation has actually
/// been polled once. On the baseline, this means each cache lookup has reached the real semaphore
/// acquisition before the delayed responses are released.
async fn timed_cache_fetch<ContextType: BuildContext>(
    database: &DistributionDatabase<'_, ContextType>,
    dist: &Dist,
    started: &ArrivalSignal,
) -> Result<Duration> {
    let started_at = Instant::now();
    let operation = database.get_or_build_wheel_metadata(dist, HashPolicy::None);
    tokio::pin!(operation);
    let mut was_polled = false;
    let metadata = future::poll_fn(|context| {
        let poll = operation.as_mut().poll(context);
        if !was_polled {
            was_polled = true;
            started.record();
        }
        poll
    })
    .await?;
    black_box(metadata);
    Ok(started_at.elapsed())
}

struct Measurement {
    p50: Duration,
    p99: Duration,
    cache_hit_http_requests: usize,
    saturated_downloads: usize,
}

/// Drive the slow requests until their real HTTP handlers have arrived at the server's hold gate.
/// A response cannot complete before the gate is released, so completion before the signal is a
/// correctness failure rather than a time-window assertion.
async fn wait_for_slow_gate<F>(
    slow_gate: &SlowGate,
    slow_requests: &mut FuturesUnordered<F>,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    let arrivals = slow_gate.wait_for_arrivals();
    tokio::pin!(arrivals);
    loop {
        tokio::select! {
            result = &mut arrivals => {
                result?;
                return Ok(());
            }
            result = slow_requests.next() => {
                match result {
                    Some(result) => {
                        result?;
                        bail!("a delayed metadata request completed before the server gate was released");
                    }
                    None => bail!("delayed metadata requests ended before reaching the server gate"),
                }
            }
        }
    }
}

async fn saturated_cache_hit_workload() -> Result<Measurement> {
    let fixture = Fixture::new(DOWNLOADS, DOWNLOADS).await?;
    let build_context = fixture.build_context();
    let database = DistributionDatabase::new(
        &fixture.client,
        &build_context,
        Arc::clone(&fixture.concurrency.downloads_semaphore),
    );

    let cached = (0..CACHE_HITS)
        .map(|index| fixture.distribution(&format!("cached{index}")))
        .collect::<Result<Vec<_>>>()?;
    for dist in &cached {
        fetch_metadata(&database, dist).await?;
    }
    ensure!(
        fixture.server.requests() == CACHE_HITS,
        "prewarming should make one PEP 658 request per cache entry"
    );

    let slow = (0..DOWNLOADS)
        .map(|index| fixture.distribution(&format!("slow{index}")))
        .collect::<Result<Vec<_>>>()?;
    let slow_gate = fixture.server.slow_gate();
    let mut slow_requests = FuturesUnordered::new();
    for dist in &slow {
        slow_requests.push(fetch_metadata(&database, dist));
    }
    wait_for_slow_gate(&slow_gate, &mut slow_requests).await?;
    ensure!(
        slow_gate.arrival_count() == DOWNLOADS,
        "cache-hit timing begins only after every delayed request reached the server hold gate"
    );
    ensure!(
        fixture.concurrency.downloads_semaphore.available_permits() == 0,
        "the delayed metadata requests must hold every real download permit"
    );

    let requests_before_cache_hits = fixture.server.requests();
    let cache_starts = Arc::new(ArrivalSignal::new(CACHE_HITS));
    let mut cache_requests = FuturesUnordered::new();
    for dist in &cached {
        let cache_starts = Arc::clone(&cache_starts);
        cache_requests.push(timed_cache_fetch(&database, dist, &cache_starts));
    }

    let cache_start_wait = cache_starts.wait_for_target();
    tokio::pin!(cache_start_wait);
    let mut latencies = Vec::with_capacity(CACHE_HITS);
    loop {
        tokio::select! {
            result = &mut cache_start_wait => {
                result?;
                break;
            }
            result = cache_requests.next() => {
                match result {
                    Some(result) => latencies.push(result?),
                    None => bail!("cache-hit requests ended before they were all polled"),
                }
            }
        }
    }
    ensure!(
        cache_starts.count() == CACHE_HITS,
        "every cache-hit operation must be polled before delayed responses are released"
    );

    // The server gate, not a scheduler deadline, controls the duration for which the real
    // download permits remain occupied after the cache-hit class has attempted the database path.
    slow_gate.release();
    while let Some(result) = cache_requests.next().await {
        latencies.push(result?);
    }
    while let Some(result) = slow_requests.next().await {
        result?;
    }

    ensure!(
        latencies.len() == CACHE_HITS,
        "the workload must observe every cache-hit result"
    );
    latencies.sort_unstable();
    let p50 = latencies[latencies.len() / 2];
    let p99_index = (latencies.len() * 99).div_ceil(100) - 1;
    let p99 = latencies[p99_index];
    let cache_hit_http_requests = fixture.server.requests() - requests_before_cache_hits;
    ensure!(
        cache_hit_http_requests == 0,
        "fresh PEP 658 cache hits must not make HTTP requests"
    );

    Ok(Measurement {
        p50,
        p99,
        cache_hit_http_requests,
        saturated_downloads: slow_gate.arrival_count(),
    })
}

async fn one_permit_network_workload() -> Result<()> {
    let fixture = Fixture::new(1, 1).await?;
    let build_context = fixture.build_context();
    let database = DistributionDatabase::new(
        &fixture.client,
        &build_context,
        Arc::clone(&fixture.concurrency.downloads_semaphore),
    );
    let slow_gate = fixture.server.slow_gate();
    let first = fixture.distribution("slowfirst")?;
    let mut first_request = FuturesUnordered::new();
    first_request.push(fetch_metadata(&database, &first));
    wait_for_slow_gate(&slow_gate, &mut first_request).await?;

    ensure!(
        slow_gate.arrival_count() == 1,
        "the cold metadata request must reach the real server before the permit check"
    );
    ensure!(
        fixture.concurrency.downloads_semaphore.available_permits() == 0,
        "a held cold metadata response must consume the supplied download permit"
    );
    ensure!(
        fixture
            .concurrency
            .downloads_semaphore
            .try_acquire()
            .is_err(),
        "cold metadata networking must remain constrained by the supplied one-permit semaphore"
    );

    slow_gate.release();
    while let Some(result) = first_request.next().await {
        result?;
    }
    let second = fixture.distribution("slowsecond")?;
    fetch_metadata(&database, &second).await?;
    ensure!(
        slow_gate.arrival_count() == 2,
        "the next cold metadata request must proceed after the held response is released"
    );
    Ok(())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread Tokio runtime")
}

fn main() -> Result<()> {
    let selector = std::env::args()
        .nth(1)
        .context("expected the cached metadata benchmark selector")?;
    ensure!(
        selector == "cached_metadata_behind_saturated_downloads",
        "unknown benchmark selector: {selector}"
    );

    let measurement = runtime().block_on(saturated_cache_hit_workload())?;
    println!(
        "{{\"metric\":\"cached_metadata_p99_latency_ns\",\"value\":{}}}",
        measurement.p99.as_nanos()
    );
    println!(
        "{{\"metric\":\"cached_metadata_p50_latency_ns\",\"value\":{}}}",
        measurement.p50.as_nanos()
    );
    println!(
        "{{\"metric\":\"cached_metadata_http_requests\",\"value\":{}}}",
        measurement.cache_hit_http_requests
    );
    println!(
        "{{\"metric\":\"saturated_downloads_arrived\",\"value\":{}}}",
        measurement.saturated_downloads
    );
    Ok(())
}

#[test]
fn fixture_uses_distribution_database_and_cache() -> Result<()> {
    let measurement = runtime().block_on(saturated_cache_hit_workload())?;
    assert_eq!(measurement.cache_hit_http_requests, 0);
    assert_eq!(measurement.saturated_downloads, DOWNLOADS);
    Ok(())
}

#[test]
fn network_metadata_stays_within_download_limit() -> Result<()> {
    runtime().block_on(one_permit_network_workload())
}
