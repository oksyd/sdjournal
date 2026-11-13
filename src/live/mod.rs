mod replay;
mod tail;
#[cfg(feature = "tokio")]
mod tokio;
#[cfg(target_os = "linux")]
mod watcher;

use crate::config::{JournalConfig, LiveQueueFullPolicy};
use crate::cursor::{Cursor, SdJournalEntryKey, compare_entry_keys, same_entry};
use crate::entry::{EntryRef, LiveEntry};
use crate::error::{LimitKind, Result, SdJournalError};
use crate::file::JournalFile;
use crate::filter::Filter;
use crate::journal::{Journal, discover_journal_candidates};
#[cfg(test)]
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering as AtomicOrdering},
};
use std::thread;
use std::time::Duration;

use self::replay::ReplayState;
#[cfg(target_os = "linux")]
use self::tail::collect_watch_paths;
use self::tail::{
    FallbackDirState, TrackedFile, build_live_snapshot, build_tracked_files_from_open_files,
    build_tracked_files_from_paths, collect_fallback_dirs, file_metadata_changed,
};
#[cfg(target_os = "linux")]
use self::watcher::InotifyWatcher;

#[cfg(all(feature = "tracing", target_os = "linux"))]
use tracing::debug;
#[cfg(feature = "tracing")]
use tracing::warn;

struct SubscriptionState {
    filter: Filter,
    tx: SyncSender<Result<LiveEntry>>,
    alive: Arc<AtomicBool>,
    replay: Option<ReplayState>,
    replay_file_tails: HashMap<[u8; 16], u64>,
    replayed_entries: RecentEntries,
    deferred_live: VecDeque<LiveEntry>,
    terminal_error: Option<SdJournalError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct LogicalEntryId {
    seqnum_id: [u8; 16],
    seqnum: u64,
    boot_id: [u8; 16],
    monotonic_usec: u64,
    realtime_usec: u64,
    xor_hash: u64,
}

// Live delivery and historical replay share the same bounded FIFO identity window.
// Copies in other journal files do not count as new entries or refresh the eviction order.
struct RecentEntries {
    entries: HashSet<LogicalEntryId>,
    order: VecDeque<LogicalEntryId>,
    capacity: usize,
}

impl RecentEntries {
    fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            entries: HashSet::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn contains(&self, key: SdJournalEntryKey) -> bool {
        self.entries.contains(&logical_entry_identity(key))
    }

    fn insert(&mut self, key: SdJournalEntryKey) -> bool {
        let logical = logical_entry_identity(key);
        if !self.entries.insert(logical) {
            return false;
        }
        self.order.push_back(logical);
        if self.order.len() > self.capacity
            && let Some(expired) = self.order.pop_front()
        {
            self.entries.remove(&expired);
        }
        true
    }
}

pub(super) struct WatchChange {
    pub(super) topology_changed: bool,
    pub(super) modified_paths: Vec<PathBuf>,
}

impl WatchChange {
    fn is_empty(&self) -> bool {
        !self.topology_changed && self.modified_paths.is_empty()
    }

    fn merge(&mut self, other: Self) {
        self.topology_changed |= other.topology_changed;
        self.modified_paths.extend(other.modified_paths);
        self.modified_paths.sort();
        self.modified_paths.dedup();
    }
}

enum SendOutcome {
    Delivered,
    Dropped,
    Closed,
}

struct RefreshedFile {
    tracked_idx: usize,
    next_tail: self::tail::FileTailCursor,
    next_live_state: crate::file::LiveFileState,
    modified: Option<std::time::SystemTime>,
    entries: Vec<EntryRef>,
    exhausted: bool,
}

/// Shared live journal engine for multi-subscription tailing.
///
/// `LiveJournal` keeps one watcher plus lightweight per-file tail checkpoints. Ordinary appends
/// open only the modified journal file, read newly appended entries, update the checkpoint, and
/// dispatch each entry once to all matching subscriptions. Topology changes such as file creation,
/// removal, or rotation, and retries of unreadable files can trigger full directory rescans.
///
/// Live delivery is bounded per cycle by [`JournalConfig::live_channel_capacity`] and
/// [`JournalConfig::max_live_batch_entries`]. Historical replay can also have a configured total
/// cap through [`JournalConfig::max_live_replay_entries`].
/// The default queue-full behavior is [`LiveQueueFullPolicy::Block`], which applies backpressure
/// instead of silently dropping entries.
/// Logical copies across different journal files are deduplicated within a bounded recent-entry
/// window, not indefinitely. During active replay, newly observed copies are also checked against
/// the replay snapshot so historical entries do not enter its live catch-up queue.
///
/// Opening requires at least one readable file. While tailing, corrupt or transiently unreadable
/// files are skipped and retried; enable the `tracing` feature to observe skipped-file diagnostics.
///
/// Create it through [`Journal::live`](crate::Journal::live), register one or more
/// [`LiveSubscription`]s, then drive the engine with [`LiveJournal::poll_once`] or
/// [`LiveJournal::run`].
///
/// # Model
///
/// Subscriptions are passive receivers. The engine only observes new journal data while
/// [`LiveJournal::poll_once`] or [`LiveJournal::run`] is being called. A subscription created with
/// [`LiveJournal::subscribe`] is live-only: it starts after the engine's current tail checkpoint
/// and does not replay existing entries. Use [`SubscriptionOptions`] when a replay window is
/// needed.
///
/// # Example
///
/// ```no_run
/// use sdjournal::LiveJournal;
/// use std::thread;
///
/// let mut live = LiveJournal::open_default()?;
///
/// let mut sshd_filter = live.filter();
/// sshd_filter.match_unit("sshd.service");
/// let sshd = live.subscribe(sshd_filter)?;
///
/// let mut systemd_filter = live.filter();
/// systemd_filter.match_unit("systemd.service");
/// let systemd = live.subscribe(systemd_filter)?;
///
/// let engine = thread::spawn(move || live.run());
///
/// let _entry = sshd.recv().expect("engine stopped")?;
/// let _entry = systemd.recv().expect("engine stopped")?;
///
/// drop(sshd);
/// drop(systemd);
/// engine.join().expect("live engine thread panicked")?;
/// # Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
/// ```
pub struct LiveJournal {
    roots: Vec<PathBuf>,
    config: JournalConfig,
    tracked_files: Vec<TrackedFile>,
    path_index: HashMap<PathBuf, usize>,
    fallback_dirs: Vec<FallbackDirState>,
    subscriptions: Vec<SubscriptionState>,
    recent_live_entries: RecentEntries,
    pending_modified_paths: Vec<PathBuf>,
    topology_refresh_requested: bool,
    retry_backoff_requested: bool,
    next_replay_index: usize,
    prefer_replay_work: bool,
    #[cfg(target_os = "linux")]
    inotify: Option<InotifyWatcher>,
    #[cfg(target_os = "linux")]
    watch_paths: Vec<PathBuf>,
}

impl LiveJournal {
    pub(crate) fn from_journal(journal: &Journal) -> Result<Self> {
        let roots = journal.inner.roots.clone();
        let config = journal.inner.config.clone();
        validate_live_config(&config)?;
        let tracked = build_tracked_files_from_open_files(
            journal.inner.file_infos.iter().map(|info| &info.file),
        )?;
        Ok(Self::with_tracked_files(roots, config, tracked))
    }

    fn with_tracked_files(
        roots: Vec<PathBuf>,
        config: JournalConfig,
        tracked_files: Vec<TrackedFile>,
    ) -> Self {
        let recent_live_entries = RecentEntries::new(live_dedup_capacity(&config));
        let path_index = tracked_files
            .iter()
            .enumerate()
            .map(|(idx, tracked)| (tracked.path.clone(), idx))
            .collect();

        let mut out = Self {
            roots,
            config,
            tracked_files,
            path_index,
            fallback_dirs: Vec::new(),
            subscriptions: Vec::new(),
            recent_live_entries,
            pending_modified_paths: Vec::new(),
            // Files may have appeared after the snapshot was opened but before watching began.
            topology_refresh_requested: true,
            retry_backoff_requested: false,
            next_replay_index: 0,
            prefer_replay_work: false,
            #[cfg(target_os = "linux")]
            inotify: None,
            #[cfg(target_os = "linux")]
            watch_paths: Vec::new(),
        };
        out.refresh_fallback_dirs();
        out.refresh_watchers(false);
        out
    }

    /// Create a live engine from the default system journal roots on Linux.
    ///
    /// This avoids opening a historical [`Journal`] first. Prefer this constructor when the
    /// application only needs live tailing.
    ///
    /// On non-Linux targets this returns [`SdJournalError::Unsupported`].
    pub fn open_default() -> Result<Self> {
        Self::open_default_with_config(JournalConfig::default())
    }

    /// Create a live engine from the default system journal roots with a custom configuration.
    ///
    /// See [`LiveJournal::open_default`] for platform behavior.
    pub fn open_default_with_config(config: JournalConfig) -> Result<Self> {
        core::cfg_select! {
            target_os = "linux" => {
                let paths = vec![
                    PathBuf::from("/run/log/journal"),
                    PathBuf::from("/var/log/journal"),
                ];
                Self::open_dirs_with_config(&paths, config)
            }
            _ => {
                let _ = config;
                Err(SdJournalError::Unsupported {
                    reason: "LiveJournal::open_default is only supported on Linux".to_string(),
                })
            }
        }
    }

    /// Create a live engine from journal files discovered under one root directory.
    ///
    /// This is the live-tail counterpart to [`Journal::open_dir`].
    pub fn open_dir(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_dir_with_config(path, JournalConfig::default())
    }

    /// Create a live engine from one root directory with a custom configuration.
    pub fn open_dir_with_config(path: impl AsRef<Path>, config: JournalConfig) -> Result<Self> {
        let paths = vec![path.as_ref().to_path_buf()];
        Self::open_dirs_with_config(&paths, config)
    }

    /// Create a live engine from journal files discovered under multiple root directories.
    ///
    /// Unlike [`Journal::open_dirs`], this opens files one at a time during initialization and
    /// stores only lightweight tail checkpoints afterward. Journal files are opened briefly when
    /// refreshing topology or reading appended entries.
    pub fn open_dirs(paths: &[PathBuf]) -> Result<Self> {
        Self::open_dirs_with_config(paths, JournalConfig::default())
    }

    /// Create a live engine from multiple root directories with a custom configuration.
    pub fn open_dirs_with_config(paths: &[PathBuf], config: JournalConfig) -> Result<Self> {
        validate_live_config(&config)?;
        let discovery = discover_journal_candidates(paths, &config)?;
        let (tracked, _) = build_tracked_files_from_paths(&discovery.candidates, &config)?;
        if tracked.is_empty() {
            return Err(SdJournalError::NotFound);
        }
        Ok(Self::with_tracked_files(discovery.roots, config, tracked))
    }

    /// Create a new live filter builder using this engine's runtime limits.
    ///
    /// Build one filter per subscription. Filters are cheap and do not start watching the journal
    /// until they are registered with [`LiveJournal::subscribe`] or
    /// [`LiveJournal::subscribe_with_options`].
    pub fn filter(&self) -> Filter {
        Filter::new(&self.config)
    }

    /// Register a live-only subscription.
    ///
    /// The returned subscription only receives entries after the engine's current tail checkpoint.
    /// It does not scan historical entries. This is the preferred path for normal tailing because
    /// registering the subscription does not rebuild query state.
    pub fn subscribe(&mut self, filter: Filter) -> Result<LiveSubscription> {
        self.subscribe_with_options(SubscriptionOptions::new(filter))
    }

    /// Register a subscription with explicit replay or resume bounds.
    ///
    /// Matching backlog covered by `options` is replayed by subsequent
    /// [`LiveJournal::poll_once`] or [`LiveJournal::run`] calls before future live entries are
    /// dispatched to that subscription.
    /// Replay deliveries are batched using [`JournalConfig::max_live_batch_entries`]; locating
    /// matching history may inspect more entries than the delivery batch contains.
    /// [`JournalConfig::max_live_replay_entries`] can add an optional per-subscription total cap.
    ///
    /// This opens an isolated snapshot to establish the replay boundary. Prefer
    /// [`LiveJournal::subscribe`] when only future entries are needed.
    /// Filter validation and starting-cursor resolution errors are returned during registration.
    /// Cursor-based subscriptions return [`SdJournalError::NotFound`] if no journal files
    /// remain; timestamp-only subscriptions can still wait for future files.
    pub fn subscribe_with_options(
        &mut self,
        options: SubscriptionOptions,
    ) -> Result<LiveSubscription> {
        let filter = options.filter;
        filter.validate_for(&self.config)?;
        let (tx, rx) = mpsc::sync_channel(self.config.live_channel_capacity);
        let alive = Arc::new(AtomicBool::new(true));
        let needs_replay = options.after_cursor.is_some() || options.since_realtime.is_some();

        let snapshot = if needs_replay {
            let discovery = discover_journal_candidates(&self.roots, &self.config)?;
            if discovery.candidates.is_empty() {
                if options.after_cursor.is_some() {
                    return Err(SdJournalError::NotFound);
                }
                None
            } else {
                Some(build_live_snapshot(&discovery.candidates, &self.config)?)
            }
        } else {
            None
        };
        let (replay, replay_file_tails) = if let Some(snapshot) = snapshot {
            let replay = ReplayState::new(
                snapshot.journal,
                &filter,
                options.after_cursor,
                options.since_realtime,
                &self.config,
            )?;
            (Some(replay), snapshot.file_tails)
        } else {
            (None, HashMap::new())
        };

        self.subscriptions.push(SubscriptionState {
            filter,
            tx,
            alive: alive.clone(),
            replay,
            replay_file_tails,
            replayed_entries: RecentEntries::new(live_dedup_capacity(&self.config)),
            deferred_live: VecDeque::new(),
            terminal_error: None,
        });

        Ok(LiveSubscription {
            rx,
            alive,
            #[cfg(feature = "tokio")]
            channel_capacity: self.config.live_channel_capacity,
        })
    }

    /// Poll for live changes once and dispatch any newly appended entries.
    ///
    /// Returns the total number of subscription deliveries performed during this cycle.
    /// This may be larger than the number of journal entries because one entry can match multiple
    /// subscriptions.
    ///
    /// Waiting for filesystem changes takes at most [`JournalConfig::poll_interval`]. On Linux
    /// this uses inotify when possible and falls back to polling. File reads and delivery are not
    /// covered by that timeout: [`LiveQueueFullPolicy::Block`] waits for a full subscription queue
    /// to become writable or disconnect.
    pub fn poll_once(&mut self) -> Result<usize> {
        self.retry_backoff_requested = false;
        self.remove_closed_subscriptions();
        if self.subscriptions.is_empty() {
            return Ok(0);
        }

        if self.prefer_replay_work
            && let Some(deliveries) = self.dispatch_next_replay_batch()?
        {
            self.remove_closed_subscriptions();
            return Ok(deliveries);
        }

        let mut processed_ready_live_work = false;
        // Transfer priority before attempting fallible work, not only after success.
        self.prefer_replay_work = true;
        if let Some(deliveries) = self.dispatch_ready_live_work()? {
            processed_ready_live_work = true;
            if deliveries != 0 {
                self.remove_closed_subscriptions();
                return Ok(deliveries);
            }
        }

        if let Some(deliveries) = self.dispatch_next_replay_batch()? {
            self.remove_closed_subscriptions();
            return Ok(deliveries);
        }

        if processed_ready_live_work {
            self.remove_closed_subscriptions();
            return Ok(0);
        }

        let change = self.wait_for_change();
        self.dispatch_change(change)
    }

    fn dispatch_ready_live_work(&mut self) -> Result<Option<usize>> {
        // Keep observing new files even while an existing file has more than one batch ready.
        // Otherwise a continuously appended file can prevent topology events being consumed.
        let change = self.try_collect_ready_change();
        if self.topology_refresh_requested || change.topology_changed {
            self.topology_refresh_requested = false;
            return self.refresh_topology_and_dispatch().map(Some);
        }

        self.pending_modified_paths.extend(change.modified_paths);
        self.deduplicate_pending_paths();
        if !self.pending_modified_paths.is_empty() {
            let paths = std::mem::take(&mut self.pending_modified_paths);
            return self.dispatch_modified_paths(&paths).map(Some);
        }

        Ok(None)
    }

    fn dispatch_change(&mut self, change: WatchChange) -> Result<usize> {
        if change.is_empty() {
            self.remove_closed_subscriptions();
            return Ok(0);
        }

        if change.topology_changed {
            return self.refresh_topology_and_dispatch();
        }

        self.dispatch_modified_paths(&change.modified_paths)
    }

    /// Run the live engine until every subscription has been dropped.
    ///
    /// This is the simplest way to drive live delivery from a background thread. Dropping all
    /// [`LiveSubscription`] handles causes the loop to exit after the next polling cycle.
    pub fn run(mut self) -> Result<()> {
        while !self.subscriptions.is_empty() {
            let delivered = self.poll_once()?;
            if delivered == 0
                && (self.retry_backoff_requested
                    || self
                        .subscriptions
                        .iter()
                        .any(|subscription| subscription.terminal_error.is_some()))
            {
                thread::sleep(self.config.poll_interval);
            }
        }
        Ok(())
    }

    fn wait_for_change(&mut self) -> WatchChange {
        core::cfg_select! {
            target_os = "linux" => {
                if let Some(w) = self.inotify.as_mut() {
                    let incomplete = !w.is_complete();
                    let mut change = w.wait(self.config.poll_interval);
                    if incomplete {
                        change.merge(self.scan_all_files());
                        self.refresh_watchers(true);
                    }
                    change
                } else {
                    let change = self.poll_all_files_after_sleep();
                    self.refresh_watchers(true);
                    change
                }
            }
            _ => {
                self.poll_all_files_after_sleep()
            }
        }
    }

    fn try_collect_ready_change(&mut self) -> WatchChange {
        core::cfg_select! {
            target_os = "linux" => {
                if let Some(w) = self.inotify.as_mut() {
                    let incomplete = !w.is_complete();
                    let mut change = w.wait(Duration::ZERO);
                    if incomplete {
                        change.merge(self.scan_all_files());
                    }
                    change
                } else {
                    self.scan_all_files()
                }
            }
            _ => {
                self.scan_all_files()
            }
        }
    }

    fn poll_all_files_after_sleep(&mut self) -> WatchChange {
        thread::sleep(self.config.poll_interval);
        self.scan_all_files()
    }

    fn scan_all_files(&mut self) -> WatchChange {
        let mut topology_changed = false;
        for dir in &self.fallback_dirs {
            match std::fs::metadata(&dir.path).and_then(|meta| meta.modified()) {
                Ok(modified) if Some(modified) != dir.modified => {
                    topology_changed = true;
                    break;
                }
                Err(_) => {
                    topology_changed = true;
                    break;
                }
                _ => {}
            }
        }

        let mut modified_paths = Vec::new();
        for tracked in &self.tracked_files {
            match std::fs::metadata(&tracked.path) {
                Ok(meta) => {
                    let len = meta.len();
                    let known = tracked.live_state.file_len;
                    if len < known {
                        topology_changed = true;
                        break;
                    }
                    if file_metadata_changed(known, tracked.modified, &meta) {
                        modified_paths.push(tracked.path.clone());
                    }
                }
                Err(_) => {
                    topology_changed = true;
                    break;
                }
            }
        }

        WatchChange {
            topology_changed,
            modified_paths,
        }
    }

    fn dispatch_next_replay_batch(&mut self) -> Result<Option<usize>> {
        let Some(idx) = self.next_replay_subscription_index() else {
            return Ok(None);
        };
        // Failed replay work still consumes its scheduling turn. Otherwise a persistent
        // replay error would prevent subsequent polls from observing healthy live files.
        self.prefer_replay_work = false;
        self.next_replay_index = idx.saturating_add(1);

        if self.subscriptions[idx].terminal_error.is_some() {
            return Ok(Some(self.dispatch_terminal_error(idx)));
        }

        let sub = &mut self.subscriptions[idx];
        let Some(replay) = sub.replay.as_mut() else {
            return Ok(Some(self.dispatch_deferred_live_batch(idx)));
        };
        if matches!(replay.remaining, Some(0)) {
            let limit = self.config.max_live_replay_entries.unwrap_or(0);
            return Err(SdJournalError::LimitExceeded {
                kind: LimitKind::LiveReplayEntries,
                limit: u64::try_from(limit).unwrap_or(u64::MAX),
            });
        };

        let limit = replay
            .remaining
            .map_or(self.config.max_live_batch_entries, |remaining| {
                self.config.max_live_batch_entries.min(remaining)
            });
        let batch = replay.collect_batch(limit)?;
        let consumed = batch.entries.len();
        let batch_exhausted = batch.exhausted;

        let mut delivered = 0usize;
        let mut closed = false;
        for entry in batch.entries {
            let key = entry.entry_key();
            match send_live_item(
                &sub.tx,
                Ok(LiveEntry::new(entry)),
                self.config.live_queue_full_policy,
            ) {
                SendOutcome::Delivered => delivered = delivered.saturating_add(1),
                SendOutcome::Dropped => {}
                SendOutcome::Closed => {
                    closed = true;
                    break;
                }
            }
            sub.replayed_entries.insert(key);
            // Free copied entries immediately: they must neither fill the catch-up buffer nor
            // outlive the bounded replay identity cache during a long historical replay.
            sub.deferred_live
                .retain(|entry| !same_entry(&entry.entry_key(), &key));
        }

        if closed {
            sub.alive.store(false, AtomicOrdering::Release);
            return Ok(Some(delivered));
        }

        if let Some(remaining) = replay.remaining.as_mut() {
            *remaining = remaining.saturating_sub(consumed);
        }
        if batch_exhausted {
            sub.replay = None;
        }

        Ok(Some(delivered))
    }

    fn dispatch_terminal_error(&mut self, idx: usize) -> usize {
        let Some(error) = self.subscriptions[idx].terminal_error.take() else {
            return 0;
        };
        match self.subscriptions[idx].tx.try_send(Err(error)) {
            Ok(()) => {
                self.subscriptions[idx]
                    .alive
                    .store(false, AtomicOrdering::Release);
                1
            }
            Err(TrySendError::Full(item)) => {
                // try_send returns the exact item passed above, which is always an error.
                self.subscriptions[idx].terminal_error = item.err();
                thread::yield_now();
                0
            }
            Err(TrySendError::Disconnected(_)) => {
                self.subscriptions[idx]
                    .alive
                    .store(false, AtomicOrdering::Release);
                0
            }
        }
    }

    fn dispatch_deferred_live_batch(&mut self, idx: usize) -> usize {
        let mut delivered = 0usize;
        let mut closed = false;
        let sub = &mut self.subscriptions[idx];
        for _ in 0..self.config.max_live_batch_entries {
            let Some(entry) = sub.deferred_live.pop_front() else {
                break;
            };
            if sub.replayed_entries.contains(entry.entry_key()) {
                continue;
            }
            match send_live_item(&sub.tx, Ok(entry), self.config.live_queue_full_policy) {
                SendOutcome::Delivered => delivered = delivered.saturating_add(1),
                SendOutcome::Dropped => {}
                SendOutcome::Closed => {
                    closed = true;
                    break;
                }
            }
        }
        if closed {
            sub.alive.store(false, AtomicOrdering::Release);
        }
        delivered
    }

    fn next_replay_subscription_index(&self) -> Option<usize> {
        let start = self.next_replay_index.min(self.subscriptions.len());
        (start..self.subscriptions.len())
            .chain(0..start)
            .find(|&idx| {
                let sub = &self.subscriptions[idx];
                sub.terminal_error.is_some()
                    || sub.replay.is_some()
                    || !sub.deferred_live.is_empty()
            })
    }

    fn refresh_topology_and_dispatch(&mut self) -> Result<usize> {
        let result = self.try_refresh_topology_and_dispatch();
        if result.is_err() {
            self.topology_refresh_requested = true;
        }
        result
    }

    fn try_refresh_topology_and_dispatch(&mut self) -> Result<usize> {
        let discovery = discover_journal_candidates(&self.roots, &self.config)?;
        let candidates = discovery.candidates;
        let candidate_paths: HashSet<PathBuf> = candidates.iter().cloned().collect();
        // A failed candidate may have no old checkpoint. Retry it independently of watcher
        // events: an in-place repair or restored permission need not change directory metadata.
        let (mut snapshot, mut deferred_paths) =
            match build_tracked_files_from_paths(&candidates, &self.config) {
                Ok(snapshot) => snapshot,
                Err(error) if is_skippable_live_file_error(&error) => (Vec::new(), candidates),
                Err(error) => return Err(error),
            };
        // A topology event must not restart scheduling from the first discovered path.
        // Preserve pending-file fairness even while files or directories keep appearing.
        let pending_positions: HashMap<_, _> = self
            .pending_modified_paths
            .iter()
            .enumerate()
            .map(|(index, path)| (path.as_path(), index))
            .collect();
        snapshot.sort_by_key(|tracked| {
            pending_positions
                .get(tracked.path.as_path())
                .copied()
                .unwrap_or(usize::MAX)
        });
        let represented_paths: HashSet<PathBuf> = snapshot
            .iter()
            .map(|tracked| tracked.path.clone())
            .collect();
        let mut old_by_identity: HashMap<([u8; 16], [u8; 16]), TrackedFile> = self
            .tracked_files
            .iter()
            .cloned()
            .map(|tracked| ((tracked.file_id, tracked.seqnum_id), tracked))
            .collect();
        let mut rebuilt = Vec::with_capacity(snapshot.len());
        let mut sources = Vec::new();
        let mut pending_count = 0usize;
        let mut unfinished_paths = Vec::new();
        let mut skipped_file = !deferred_paths.is_empty();

        for template in snapshot {
            let identity = (template.file_id, template.seqnum_id);
            let mut tracked = old_by_identity.remove(&identity).unwrap_or_else(|| {
                let mut tracked = template.clone();
                tracked.live_state = crate::file::LiveFileState {
                    used_size: 0,
                    file_len: 0,
                    n_entries: 0,
                    entry_array_offset: 0,
                    tail_object_offset: 0,
                };
                tracked.tail = self::tail::FileTailCursor::at_start();
                tracked
            });
            tracked.path = template.path.clone();
            tracked.modified = template.modified;

            let remaining = self
                .config
                .max_live_batch_entries
                .saturating_sub(pending_count);
            if remaining == 0 {
                deferred_paths.push(tracked.path.clone());
                rebuilt.push(tracked);
                continue;
            }

            // Discovery validates the tail metadata, but a writer can rotate the file between
            // opens, or an appended ENTRY can reference unreadable DATA. Keep this file's old
            // checkpoint in either case so it can be retried without stopping healthy streams.
            let refreshed = (|| {
                let file = JournalFile::open(tracked.path.clone(), &self.config)?;
                if file.file_id() != tracked.file_id || file.seqnum_id() != tracked.seqnum_id {
                    return Err(SdJournalError::Transient {
                        path: Some(tracked.path.clone()),
                        reason: "journal file changed identity during topology refresh".to_string(),
                    });
                }
                let new_state = file.live_state();
                if new_state.used_size < tracked.live_state.used_size
                    || new_state.file_len < tracked.live_state.file_len
                    || new_state.n_entries < tracked.live_state.n_entries
                {
                    return Err(SdJournalError::Transient {
                        path: Some(tracked.path.clone()),
                        reason: "journal file moved backwards during topology refresh".to_string(),
                    });
                }

                let mut next_tail = tracked.tail.clone();
                let batch = next_tail.drain_new_offsets(&file, remaining)?;
                let mut entries = Vec::with_capacity(batch.offsets.len());
                for offset in batch.offsets {
                    entries.push(file.read_entry_ref(offset)?);
                }
                Ok((
                    next_tail,
                    new_state,
                    entries,
                    batch.exhausted,
                    file.modified_at_open(),
                ))
            })();
            let (next_tail, new_state, entries, exhausted, modified) = match refreshed {
                Ok(refreshed) => refreshed,
                Err(error) if is_skippable_live_file_error(&error) => {
                    warn_skipped_live_file(&tracked.path, &error);
                    skipped_file = true;
                    deferred_paths.push(tracked.path.clone());
                    rebuilt.push(tracked);
                    continue;
                }
                Err(error) => return Err(error),
            };
            pending_count = pending_count.saturating_add(entries.len());
            tracked.tail = next_tail;
            tracked.modified = modified;
            if exhausted {
                tracked.live_state = new_state;
            } else {
                unfinished_paths.push(tracked.path.clone());
            }
            if !entries.is_empty() {
                sources.push(VecDeque::from(entries));
            }
            rebuilt.push(tracked);
        }

        for (_, tracked) in old_by_identity {
            if (candidate_paths.contains(&tracked.path)
                && !represented_paths.contains(&tracked.path))
                || discovery
                    .missing_roots
                    .iter()
                    .any(|root| tracked.path.starts_with(root))
            {
                // Retain checkpoints only for unreadable candidates or roots that discovery
                // actually found missing. Ordinary vacuum must release old file identities.
                skipped_file = true;
                deferred_paths.push(tracked.path.clone());
                rebuilt.push(tracked);
            }
        }

        self.retry_backoff_requested =
            (skipped_file || !discovery.missing_roots.is_empty()) && pending_count == 0;
        self.tracked_files = rebuilt;
        self.path_index = self
            .tracked_files
            .iter()
            .enumerate()
            .map(|(idx, tracked)| (tracked.path.clone(), idx))
            .collect();
        self.pending_modified_paths = deferred_paths;
        self.pending_modified_paths.extend(unfinished_paths);
        self.deduplicate_pending_paths();
        self.refresh_fallback_dirs();
        self.refresh_watchers(true);

        Ok(self.dispatch_entries(merge_entry_sources(sources)))
    }

    fn dispatch_modified_paths(&mut self, paths: &[PathBuf]) -> Result<usize> {
        let result = self.try_dispatch_modified_paths(paths);
        if result.is_err() {
            // The watcher events (and pending queue) have already been drained. Retain their
            // paths as well as the uncommitted checkpoints when a refresh fails.
            self.pending_modified_paths.extend_from_slice(paths);
            self.deduplicate_pending_paths();
        }
        result
    }

    fn try_dispatch_modified_paths(&mut self, paths: &[PathBuf]) -> Result<usize> {
        let mut refreshed_files = Vec::new();
        let mut pending_count = 0usize;
        let mut deferred_paths = Vec::new();
        let mut needs_topology_refresh = false;

        for (pos, path) in paths.iter().enumerate() {
            let Some(&idx) = self.path_index.get(path) else {
                if is_candidate_journal_path(path) {
                    needs_topology_refresh = true;
                    break;
                }
                continue;
            };

            let remaining = self
                .config
                .max_live_batch_entries
                .saturating_sub(pending_count);
            if remaining == 0 {
                deferred_paths.extend_from_slice(&paths[pos..]);
                break;
            }
            let Some(refreshed) = self.refresh_tracked_file(idx, remaining)? else {
                needs_topology_refresh = true;
                break;
            };
            pending_count = pending_count.saturating_add(refreshed.entries.len());
            if pending_count >= self.config.max_live_batch_entries {
                deferred_paths.extend_from_slice(&paths[pos + 1..]);
                if !refreshed.exhausted {
                    deferred_paths.push(path.clone());
                }
            }
            refreshed_files.push(refreshed);
            if pending_count >= self.config.max_live_batch_entries {
                break;
            }
        }

        if needs_topology_refresh && pending_count == 0 {
            return self.refresh_topology_and_dispatch();
        }

        let sources = refreshed_files
            .iter_mut()
            .map(|refreshed| VecDeque::from(std::mem::take(&mut refreshed.entries)))
            .collect();
        self.commit_refreshed_files(refreshed_files);
        self.pending_modified_paths.extend(deferred_paths);
        self.deduplicate_pending_paths();

        if needs_topology_refresh {
            self.topology_refresh_requested = true;
        }

        // Each source is already ordered by its per-file ENTRY_ARRAY. A pairwise k-way merge is
        // required here because systemd's conditional journal ordering is not a transitive `Ord`.
        Ok(self.dispatch_entries(merge_entry_sources(sources)))
    }

    fn refresh_tracked_file(&self, idx: usize, limit: usize) -> Result<Option<RefreshedFile>> {
        let old_state = self.tracked_files[idx].live_state;
        let reopened = match JournalFile::open(self.tracked_files[idx].path.clone(), &self.config) {
            Ok(file) => file,
            Err(err) if is_skippable_live_file_error(&err) => return Ok(None),
            Err(e) => return Err(e),
        };
        let modified = reopened.modified_at_open();

        let tracked = &self.tracked_files[idx];
        if reopened.file_id() != tracked.file_id || reopened.seqnum_id() != tracked.seqnum_id {
            return Ok(None);
        }

        let new_state = reopened.live_state();
        if new_state.used_size < old_state.used_size
            || new_state.file_len < old_state.file_len
            || new_state.n_entries < old_state.n_entries
        {
            return Ok(None);
        }
        if new_state == old_state {
            return Ok(Some(RefreshedFile {
                tracked_idx: idx,
                next_tail: tracked.tail.clone(),
                next_live_state: new_state,
                modified,
                entries: Vec::new(),
                exhausted: true,
            }));
        }

        let mut next_tail = tracked.tail.clone();
        let batch = match next_tail.drain_new_offsets(&reopened, limit) {
            Ok(batch) => batch,
            Err(SdJournalError::Transient { .. }) | Err(SdJournalError::Corrupt { .. }) => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let mut entries = Vec::with_capacity(batch.offsets.len());
        for offset in batch.offsets {
            match reopened.read_entry_ref(offset) {
                Ok(entry) => entries.push(entry),
                Err(err) if is_skippable_live_file_error(&err) => {
                    warn_live_file_error("skipping corrupt live entry", &err);
                    return Ok(None);
                }
                Err(err) => return Err(err),
            }
        }
        Ok(Some(RefreshedFile {
            tracked_idx: idx,
            next_tail,
            next_live_state: if batch.exhausted {
                new_state
            } else {
                old_state
            },
            modified,
            entries,
            exhausted: batch.exhausted,
        }))
    }

    fn commit_refreshed_files(&mut self, refreshed_files: Vec<RefreshedFile>) {
        for refreshed in refreshed_files {
            let tracked = &mut self.tracked_files[refreshed.tracked_idx];
            tracked.tail = refreshed.next_tail;
            tracked.live_state = refreshed.next_live_state;
            tracked.modified = refreshed.modified;
        }
    }

    fn dispatch_entries(&mut self, pending: Vec<EntryRef>) -> usize {
        if pending.is_empty() {
            self.remove_closed_subscriptions();
            return 0;
        }

        let mut deliveries = 0usize;
        let mut matched = Vec::with_capacity(self.subscriptions.len());

        for owned in pending {
            let key = owned.entry_key();
            if !self.recent_live_entries.insert(key) {
                continue;
            }
            matched.clear();

            for (idx, sub) in self.subscriptions.iter_mut().enumerate() {
                if !sub.alive.load(AtomicOrdering::Acquire) || sub.terminal_error.is_some() {
                    continue;
                }

                // A rotated copy can have a different file_id, so snapshot tail offsets alone
                // cannot exclude entries already covered by this subscription's replay.
                if sub.replayed_entries.contains(key) {
                    continue;
                }

                let defer = sub.replay.is_some() || !sub.deferred_live.is_empty();
                if let Some(tail_offset) = sub.replay_file_tails.get(&key.file_id).copied() {
                    if key.entry_offset <= tail_offset {
                        continue;
                    }
                    sub.replay_file_tails.remove(&key.file_id);
                }
                if !sub.filter.matches(&owned) {
                    continue;
                }
                if let Some(replay) = sub.replay.as_ref() {
                    match replay.contains_key(key) {
                        Ok(true) => continue,
                        Ok(false) => {}
                        // An unreadable old snapshot cannot prove that this is a duplicate.
                        // Preserve the live entry; normal replay processing reports its errors.
                        Err(error) => {
                            warn_live_file_error(
                                "unable to check live replay snapshot copy",
                                &error,
                            );
                        }
                    }
                }

                matched.push((idx, defer));
            }

            if !matched.is_empty() {
                let shared = LiveEntry::new(owned);
                for (idx, defer) in matched.iter().copied() {
                    if defer {
                        let sub = &mut self.subscriptions[idx];
                        if sub.deferred_live.len() < self.config.live_channel_capacity {
                            sub.deferred_live.push_back(shared.clone());
                            continue;
                        }
                        match self.config.live_queue_full_policy {
                            LiveQueueFullPolicy::DropNewest => continue,
                            LiveQueueFullPolicy::Disconnect => {
                                sub.alive.store(false, AtomicOrdering::Release);
                                continue;
                            }
                            LiveQueueFullPolicy::Block => {
                                sub.replay = None;
                                sub.deferred_live.clear();
                                sub.terminal_error = Some(SdJournalError::Transient {
                                    path: None,
                                    reason: format!(
                                        "live replay catch-up buffer exceeded {} entries",
                                        self.config.live_channel_capacity
                                    ),
                                });
                                continue;
                            }
                        }
                    }
                    match send_live_item(
                        &self.subscriptions[idx].tx,
                        Ok(shared.clone()),
                        self.config.live_queue_full_policy,
                    ) {
                        SendOutcome::Delivered => deliveries = deliveries.saturating_add(1),
                        SendOutcome::Dropped => {}
                        SendOutcome::Closed => self.subscriptions[idx]
                            .alive
                            .store(false, AtomicOrdering::Release),
                    }
                }
            }
        }

        self.remove_closed_subscriptions();

        deliveries
    }

    fn refresh_watchers(&mut self, force: bool) {
        #[cfg(target_os = "linux")]
        {
            let watch_paths = collect_watch_paths(&self.roots, &self.tracked_files);
            let incomplete = self
                .inotify
                .as_ref()
                .is_none_or(|watcher| !watcher.is_complete());
            if force || incomplete || watch_paths != self.watch_paths {
                let replacement = InotifyWatcher::new(&watch_paths);
                if incomplete
                    && replacement
                        .as_ref()
                        .is_some_and(InotifyWatcher::is_complete)
                {
                    // Newly established coverage cannot recover events from before its watches
                    // were installed. Rescan once before relying on that complete coverage.
                    self.topology_refresh_requested = true;
                }
                if let Some(previous) = self.inotify.as_mut() {
                    // Install the replacement first, then preserve topology events generated
                    // while scanning or installing watches. Rechecking tracked files alone
                    // cannot discover a newly created file whose event belonged to the old fd.
                    self.topology_refresh_requested |=
                        previous.wait(Duration::ZERO).topology_changed;
                }
                self.inotify = replacement;
                self.watch_paths = watch_paths;
                self.queue_tracked_paths_for_recheck();
                #[cfg(feature = "tracing")]
                debug!(
                    inotify = self.inotify.is_some(),
                    n_watch_paths = self.watch_paths.len(),
                    "live watcher refreshed"
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = force;
    }

    fn refresh_fallback_dirs(&mut self) {
        self.fallback_dirs = collect_fallback_dirs(&self.roots, &self.tracked_files);
    }

    fn queue_tracked_paths_for_recheck(&mut self) {
        self.pending_modified_paths.extend(
            self.tracked_files
                .iter()
                .map(|tracked| tracked.path.clone()),
        );
        self.deduplicate_pending_paths();
    }

    fn deduplicate_pending_paths(&mut self) {
        // Preserve queue order: unread files precede the just-processed file's next batch.
        // Sorting here would let a busy lexically earlier path starve every later path.
        let mut seen = HashSet::with_capacity(self.pending_modified_paths.len());
        self.pending_modified_paths
            .retain(|path| seen.insert(path.clone()));
    }

    fn remove_closed_subscriptions(&mut self) {
        self.subscriptions
            .retain(|sub| sub.alive.load(AtomicOrdering::Acquire));
        self.next_replay_index = self.next_replay_index.min(self.subscriptions.len());
    }
}

/// Options for `LiveJournal` subscriptions.
///
/// Without additional bounds, subscriptions are live-only and start after the current tail.
/// Adding [`SubscriptionOptions::after_cursor`] or [`SubscriptionOptions::since_realtime`]
/// requests a replay before the subscription switches to live delivery.
#[derive(Clone)]
pub struct SubscriptionOptions {
    filter: Filter,
    after_cursor: Option<Cursor>,
    since_realtime: Option<u64>,
}

impl SubscriptionOptions {
    /// Create subscription options from a live filter.
    pub fn new(filter: Filter) -> Self {
        Self {
            filter,
            after_cursor: None,
            since_realtime: None,
        }
    }

    /// Replay matching entries strictly after `cursor` before switching to live delivery.
    ///
    /// This is intended for checkpoint-based consumers that persist [`crate::Cursor`] values.
    pub fn after_cursor(&mut self, cursor: Cursor) -> &mut Self {
        self.after_cursor = Some(cursor);
        self
    }

    /// Replay matching entries from `usec` before switching to live delivery.
    ///
    /// `usec` is a realtime timestamp in microseconds since the Unix epoch.
    pub fn since_realtime(&mut self, usec: u64) -> &mut Self {
        self.since_realtime = Some(usec);
        self
    }
}

/// Receiving end of a live subscription.
///
/// Values are delivered as shared [`LiveEntry`] handles so one decoded entry can be fanned out to
/// multiple subscribers without duplicating field storage.
/// Each subscription has a bounded queue controlled by [`JournalConfig::live_channel_capacity`].
///
/// Dropping a subscription unregisters it from the engine. When all subscriptions are dropped,
/// [`LiveJournal::run`] exits.
pub struct LiveSubscription {
    rx: Receiver<Result<LiveEntry>>,
    alive: Arc<AtomicBool>,
    #[cfg(feature = "tokio")]
    channel_capacity: usize,
}

impl LiveSubscription {
    /// Receive the next entry, blocking until the subscription closes or a value arrives.
    ///
    /// The outer `Result` reports channel closure. The inner crate [`Result`] reports journal
    /// decoding or I/O errors produced by the live engine.
    pub fn recv(&self) -> std::result::Result<Result<LiveEntry>, mpsc::RecvError> {
        self.rx.recv()
    }

    /// Receive the next entry, waiting at most `timeout`.
    ///
    /// The outer `Result` distinguishes timeout or channel closure from a delivered value. The
    /// delivered value is still a crate [`Result`] because reading the journal can fail.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<Result<LiveEntry>, mpsc::RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    /// Try to receive the next queued entry without blocking.
    ///
    /// This is useful when integrating [`LiveJournal::poll_once`] into an existing event loop.
    pub fn try_recv(&self) -> std::result::Result<Result<LiveEntry>, mpsc::TryRecvError> {
        self.rx.try_recv()
    }
}

impl Drop for LiveSubscription {
    fn drop(&mut self) {
        self.alive.store(false, AtomicOrdering::Release);
    }
}

fn validate_live_config(config: &JournalConfig) -> Result<()> {
    if config.max_open_files == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_open_files must be greater than zero".to_string(),
        });
    }
    if config.live_channel_capacity == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "live_channel_capacity must be greater than zero".to_string(),
        });
    }
    // The queued values alone must fit a representable allocation. This checks the public item
    // type, without depending on the standard channel's private slot layout or reserving memory.
    if std::alloc::Layout::array::<Result<LiveEntry>>(config.live_channel_capacity).is_err() {
        return Err(SdJournalError::InvalidQuery {
            reason: "live_channel_capacity is too large to represent a bounded channel".to_string(),
        });
    }
    if config.max_live_batch_entries == 0 {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_live_batch_entries must be greater than zero".to_string(),
        });
    }
    if config.max_live_replay_entries == Some(0) {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_live_replay_entries must be greater than zero".to_string(),
        });
    }
    if config.live_queue_full_policy == LiveQueueFullPolicy::Block
        && config.max_live_batch_entries > config.live_channel_capacity
    {
        return Err(SdJournalError::InvalidQuery {
            reason: "max_live_batch_entries must not exceed live_channel_capacity when live_queue_full_policy is Block".to_string(),
        });
    }
    Ok(())
}

fn send_live_item(
    tx: &SyncSender<Result<LiveEntry>>,
    item: Result<LiveEntry>,
    policy: LiveQueueFullPolicy,
) -> SendOutcome {
    match policy {
        LiveQueueFullPolicy::Block => match tx.send(item) {
            Ok(()) => SendOutcome::Delivered,
            Err(_) => SendOutcome::Closed,
        },
        LiveQueueFullPolicy::DropNewest => match tx.try_send(item) {
            Ok(()) => SendOutcome::Delivered,
            Err(TrySendError::Full(_)) => SendOutcome::Dropped,
            Err(TrySendError::Disconnected(_)) => SendOutcome::Closed,
        },
        LiveQueueFullPolicy::Disconnect => match tx.try_send(item) {
            Ok(()) => SendOutcome::Delivered,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => SendOutcome::Closed,
        },
    }
}

fn is_skippable_live_file_error(err: &SdJournalError) -> bool {
    matches!(
        err,
        SdJournalError::Corrupt { .. }
            | SdJournalError::Transient { .. }
            | SdJournalError::PermissionDenied { .. }
            | SdJournalError::Io { .. }
            | SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectChainSteps,
                ..
            }
    )
}

fn warn_skipped_live_file(path: &std::path::Path, err: &SdJournalError) {
    #[cfg(feature = "tracing")]
    warn!(
        path = %path.display(),
        error = %err,
        "skipping journal file for live tailing"
    );

    let _ = (path, err);
}

fn warn_live_file_error(message: &'static str, err: &SdJournalError) {
    #[cfg(feature = "tracing")]
    warn!(error = %err, "{message}");

    let _ = (message, err);
}

fn is_candidate_journal_path(path: &std::path::Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("journal") | Some("journal~")
    )
}

fn logical_entry_identity(key: SdJournalEntryKey) -> LogicalEntryId {
    LogicalEntryId {
        seqnum_id: key.seqnum_id,
        seqnum: key.seqnum,
        boot_id: key.boot_id,
        monotonic_usec: key.monotonic_usec,
        realtime_usec: key.realtime_usec,
        xor_hash: key.xor_hash,
    }
}

fn live_dedup_capacity(config: &JournalConfig) -> usize {
    config
        .max_journal_files
        .saturating_add(config.max_live_batch_entries)
        .max(config.live_channel_capacity)
        .clamp(1, 65_536)
}

fn merge_entry_sources(mut sources: Vec<VecDeque<EntryRef>>) -> Vec<EntryRef> {
    let capacity = sources
        .iter()
        .fold(0usize, |total, source| total.saturating_add(source.len()));
    let mut merged = Vec::with_capacity(capacity);

    loop {
        let mut selected: Option<usize> = None;
        for (idx, source) in sources.iter().enumerate() {
            let Some(candidate) = source.front() else {
                continue;
            };
            let Some(best_idx) = selected else {
                selected = Some(idx);
                continue;
            };
            let Some(best) = sources[best_idx].front() else {
                continue;
            };
            let ordering = compare_entry_keys(&candidate.entry_key(), &best.entry_key());
            // Ascending source indices keep the first source on an equal key.
            if ordering.is_lt() {
                selected = Some(idx);
            }
        }

        let Some(selected) = selected else {
            break;
        };
        let Some(entry) = sources[selected].pop_front() else {
            break;
        };
        merged.push(entry);
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_denied_is_a_retryable_live_file_error() {
        assert!(is_skippable_live_file_error(
            &SdJournalError::PermissionDenied {
                path: PathBuf::from("temporarily-unreadable.journal"),
            }
        ));
        assert!(!is_skippable_live_file_error(
            &SdJournalError::LimitExceeded {
                kind: LimitKind::ObjectSizeBytes,
                limit: 1024,
            }
        ));
    }

    #[test]
    fn previously_unreadable_candidates_retry_without_new_watcher_events() {
        for polling in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let mut valid = vec![0u8; 272];
            valid[..8].copy_from_slice(b"LPKSHHRH");
            valid[24] = 1;
            valid[88..96].copy_from_slice(&272u64.to_le_bytes());
            std::fs::write(root.path().join("healthy.journal"), &valid).unwrap();
            let recovering = root.path().join("recovering.journal");
            std::fs::write(&recovering, &valid[..16]).unwrap();
            let mut live = LiveJournal::open_dir(root.path()).unwrap();
            let _subscription = live.subscribe(live.filter()).unwrap();
            for _ in 0..3 {
                live.dispatch_ready_live_work().unwrap();
            }
            assert_eq!(live.tracked_files.len(), 1);

            let directory_modified = std::fs::metadata(root.path()).unwrap().modified().unwrap();
            valid[24] = 2;
            std::fs::write(&recovering, valid).unwrap();
            assert_eq!(
                std::fs::metadata(root.path()).unwrap().modified().unwrap(),
                directory_modified,
            );
            #[cfg(target_os = "linux")]
            {
                if polling {
                    live.inotify = None;
                } else {
                    // Recovery must not require MODIFY: restoring read permission, for example,
                    // produces only ATTRIB, which is not part of the watched event mask.
                    live.inotify.as_mut().unwrap().wait(Duration::ZERO);
                }
            }
            #[cfg(not(target_os = "linux"))]
            let _ = polling;

            live.dispatch_ready_live_work().unwrap();
            assert!(
                live.tracked_files
                    .iter()
                    .any(|tracked| tracked.path == recovering),
                "the previously skipped candidate was forgotten (polling={polling})",
            );
        }
    }

    #[test]
    fn vacuumed_files_do_not_retain_checkpoints_in_an_existing_root() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("vacuumed.journal");
        let mut valid = vec![0u8; 272];
        valid[..8].copy_from_slice(b"LPKSHHRH");
        valid[88..96].copy_from_slice(&272u64.to_le_bytes());
        std::fs::write(&path, valid).unwrap();
        let mut live = LiveJournal::open_dir(root.path()).unwrap();
        std::fs::remove_file(path).unwrap();
        live.refresh_topology_and_dispatch().unwrap();
        assert!(live.tracked_files.is_empty());
        assert!(live.pending_modified_paths.is_empty());
        assert!(matches!(
            LiveJournal::open_dir(root.path()),
            Err(SdJournalError::NotFound)
        ));
    }

    #[test]
    fn ordinary_file_id_copies_are_not_retry_candidates() {
        let root = tempfile::tempdir().unwrap();
        let original = root.path().join("original.journal");
        let copy = root.path().join("copy.journal");
        let mut valid = vec![0u8; 272];
        valid[..8].copy_from_slice(b"LPKSHHRH");
        valid[88..96].copy_from_slice(&272u64.to_le_bytes());
        std::fs::write(&original, valid).unwrap();
        std::fs::copy(&original, &copy).unwrap();
        let (tracked, skipped) =
            build_tracked_files_from_paths(&[original, copy], &JournalConfig::default()).unwrap();
        assert_eq!(tracked.len(), 1);
        assert!(skipped.is_empty());
    }

    #[test]
    fn missing_roots_without_old_checkpoints_still_back_off() {
        let root = tempfile::tempdir().unwrap();
        let mut live = LiveJournal::with_tracked_files(
            vec![root.path().join("missing")],
            JournalConfig::default(),
            Vec::new(),
        );
        live.refresh_topology_and_dispatch().unwrap();
        assert!(live.tracked_files.is_empty());
        assert!(live.retry_backoff_requested);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn restored_watch_coverage_rechecks_preexisting_unwatched_directories_once() {
        for partial_coverage in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let watched = root.path().join("watched");
            std::fs::create_dir(&watched).unwrap();
            let mut live = LiveJournal::with_tracked_files(
                vec![root.path().to_path_buf()],
                JournalConfig::default(),
                Vec::new(),
            );
            live.inotify = if partial_coverage {
                let watcher =
                    InotifyWatcher::new(&[watched, root.path().join("not-yet-existing")]).unwrap();
                assert!(!watcher.is_complete());
                Some(watcher)
            } else {
                None
            };
            live.topology_refresh_requested = false;

            // This change happened outside the old watcher coverage. A replacement can watch
            // the directory from now on, but cannot recover its past creation event.
            std::fs::create_dir(root.path().join("0123456789abcdef0123456789abcdef")).unwrap();
            live.refresh_watchers(false);
            assert!(live.inotify.as_ref().unwrap().is_complete());
            assert!(live.topology_refresh_requested);

            // Once coverage is complete, ordinary handoffs need no unconditional extra scan.
            live.topology_refresh_requested = false;
            live.refresh_watchers(true);
            assert!(!live.topology_refresh_requested);
        }
    }

    #[test]
    fn live_dispatch_uses_one_liveness_state_and_keeps_other_subscriptions() {
        let config = JournalConfig {
            live_channel_capacity: 1,
            max_live_batch_entries: 3,
            live_queue_full_policy: LiveQueueFullPolicy::Disconnect,
            ..Default::default()
        };
        let mut live = LiveJournal::with_tracked_files(Vec::new(), config, Vec::new());
        let slow = live.subscribe(live.filter()).unwrap();
        let mut healthy_filter = live.filter();
        healthy_filter.match_exact("MESSAGE", b"third");
        let healthy = live.subscribe(healthy_filter).unwrap();
        let mut absent_filter = live.filter();
        absent_filter.match_present("ABSENT");
        let dropped = live.subscribe(absent_filter).unwrap();
        drop(dropped);

        let entries = ["first", "second", "third"]
            .into_iter()
            .enumerate()
            .map(|(idx, message)| {
                EntryRef::new_parsed(
                    SdJournalEntryKey {
                        file_id: [1; 16],
                        seqnum_id: [2; 16],
                        boot_id: [3; 16],
                        entry_offset: (idx as u64 + 1) * 8,
                        seqnum: idx as u64 + 1,
                        realtime_usec: idx as u64 + 1,
                        monotonic_usec: idx as u64 + 1,
                        xor_hash: 0,
                    },
                    vec![crate::entry::FieldRef {
                        name: "MESSAGE".into(),
                        payload: crate::reader::ByteBuf::from_vec(
                            format!("MESSAGE={message}").into_bytes(),
                        ),
                    }],
                )
            })
            .collect();
        live.next_replay_index = usize::MAX;
        assert_eq!(live.dispatch_entries(entries), 2);
        assert_eq!(
            slow.try_recv().unwrap().unwrap().get("MESSAGE"),
            Some(&b"first"[..])
        );
        assert!(matches!(
            slow.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        assert!(!slow.alive.load(AtomicOrdering::Acquire));
        assert_eq!(
            healthy.try_recv().unwrap().unwrap().get("MESSAGE"),
            Some(&b"third"[..])
        );
        assert!(healthy.alive.load(AtomicOrdering::Acquire));
        assert_eq!(live.subscriptions.len(), 1);
        assert!(Arc::ptr_eq(&live.subscriptions[0].alive, &healthy.alive));
        assert_eq!(live.next_replay_index, 1);
    }

    #[test]
    fn replay_subscription_selection_wraps_and_skips_idle_subscriptions() {
        let config = JournalConfig::default();
        let mut live = LiveJournal::with_tracked_files(Vec::new(), config.clone(), Vec::new());
        live.next_replay_index = usize::MAX;
        assert_eq!(live.next_replay_subscription_index(), None);
        let _receivers: Vec<_> = (0..3)
            .map(|_| live.subscribe(live.filter()).unwrap())
            .collect();
        assert_eq!(live.next_replay_subscription_index(), None);
        let journal = Journal {
            inner: Arc::new(crate::journal::JournalInner {
                config: config.clone(),
                roots: Vec::new(),
                file_infos: Vec::new(),
            }),
        };
        live.subscriptions[1].replay =
            Some(ReplayState::new(journal, &live.filter(), None, None, &config).unwrap());
        live.subscriptions[2].terminal_error = Some(SdJournalError::NotFound);
        for (start, expected) in [(0, 1), (1, 1), (2, 2), (3, 1), (usize::MAX, 1)] {
            live.next_replay_index = start;
            assert_eq!(live.next_replay_subscription_index(), Some(expected));
        }
        live.subscriptions[1].replay = None;
        assert_eq!(live.next_replay_subscription_index(), Some(2));
    }

    #[test]
    fn recent_entries_deduplicates_copies_without_refreshing_fifo_eviction() {
        let first = SdJournalEntryKey {
            file_id: [0x11; 16],
            entry_offset: 8,
            seqnum_id: [0x22; 16],
            seqnum: 1,
            boot_id: [0x33; 16],
            monotonic_usec: 10,
            realtime_usec: 100,
            xor_hash: 1,
        };
        let second = SdJournalEntryKey { seqnum: 2, ..first };
        let third = SdJournalEntryKey { seqnum: 3, ..first };
        let copy = SdJournalEntryKey {
            file_id: [0x44; 16],
            entry_offset: 16,
            ..first
        };

        let mut recent = RecentEntries::new(2);
        assert!(recent.insert(first));
        assert!(recent.insert(second));
        assert!(recent.contains(copy));
        assert!(!recent.insert(copy));
        assert!(recent.insert(third));
        assert!(!recent.contains(first));
        assert!(recent.contains(second));
        assert!(recent.contains(third));
        assert_eq!(recent.entries.len(), 2);
        assert_eq!(recent.order.len(), 2);

        assert!(recent.insert(copy));
        assert!(!recent.contains(second));
        assert!(recent.contains(first));
        assert_eq!(recent.entries.len(), 2);
        assert_eq!(recent.order.len(), 2);
    }

    #[test]
    fn live_journal_remains_send_and_sync() {
        fn assert_send_and_sync<T: Send + Sync>() {}
        assert_send_and_sync::<LiveJournal>();
    }

    #[test]
    fn compare_entry_keys_keeps_sequence_order_across_realtime_rollback() {
        let earlier = SdJournalEntryKey {
            file_id: [0x11; 16],
            entry_offset: 1,
            seqnum_id: [0x33; 16],
            seqnum: 2,
            boot_id: [0x44; 16],
            monotonic_usec: 20,
            realtime_usec: 100,
            xor_hash: 7,
        };
        let later = SdJournalEntryKey {
            file_id: [0x22; 16],
            entry_offset: 4,
            seqnum_id: [0x33; 16],
            seqnum: 5,
            boot_id: [0x44; 16],
            monotonic_usec: 30,
            realtime_usec: 50,
            xor_hash: 8,
        };

        assert_eq!(compare_entry_keys(&earlier, &later), Ordering::Less);
        assert_eq!(compare_entry_keys(&later, &earlier), Ordering::Greater);
        assert_eq!(compare_entry_keys(&earlier, &earlier), Ordering::Equal);
    }

    #[test]
    fn validate_live_config_rejects_unbounded_or_blocking_unsafe_values() {
        let mut cfg = JournalConfig {
            live_channel_capacity: 0,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            max_open_files: 0,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            max_live_batch_entries: 0,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            max_live_replay_entries: Some(0),
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg = JournalConfig {
            live_channel_capacity: 1,
            max_live_batch_entries: 2,
            live_queue_full_policy: LiveQueueFullPolicy::Block,
            ..Default::default()
        };
        assert!(matches!(
            validate_live_config(&cfg),
            Err(SdJournalError::InvalidQuery { .. })
        ));

        cfg.live_queue_full_policy = LiveQueueFullPolicy::Disconnect;
        assert!(validate_live_config(&cfg).is_ok());
    }
}
