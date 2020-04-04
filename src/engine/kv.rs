use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;


use crate::engine::KvEngine;
use crate::{KvError, Result};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;
use std::sync::Mutex;

use crossbeam_skiplist::SkipMap;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

fn get_log_list(path: &Path) -> Result<Vec<u64>> {
    let mut log_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    log_list.sort_unstable();
    Ok(log_list)
}

fn log_path(dir: &Path, version: u64) -> PathBuf {
    dir.join(format!("{}.log", version))
}

fn load(
    gen: u64,
    reader: &mut BufReaderWithIndex<File>,
    index: &SkipMap<String, CommandIndex>,
) -> Result<u64> {
    // To make sure we read from the beginning of the file
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0; // number of bytes that can be saved after a compaction
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = index.get(&key) {
                    uncompacted += old_cmd.value().len;
                }
                index.insert(key, (gen, pos..new_pos).into());
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.value().len;
                }
                // the "remove" command itself can be deleted in the next compaction
                // so we add its length to `uncompacted`
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

fn new_log_file(
    path: &Path,
    version: u64,
    readers: &mut HashMap<u64, BufReaderWithIndex<File>>,
) -> Result<BufWriterWithIndex<File>> {
    let path = log_path(&path, version);
    let writer = BufWriterWithIndex::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(version, BufReaderWithIndex::new(File::open(&path)?)?);
    Ok(writer)
}


struct KvStoreReader {
    path: Arc<PathBuf>,
    curr_version: Arc<AtomicU64>,
    readers: RefCell<BTreeMap<u64, BufReaderWithIndex<File>>>,
}

impl KvStoreReader {
    fn remove_timeout_log(&self) {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let version = *readers.keys().next().unwrap();
            if version >= self.curr_version.load(Ordering::SeqCst) {
                break;
            }
            readers.remove(&version);
        }
    }

    fn read_command(&self, cmd_index: CommandIndex) -> Result<Command> {
        self.remove_timeout_log();
        let mut readers = self.readers.borrow_mut();
        if !readers.contains_key(&cmd_index.version) {
            let reader = BufReaderWithIndex::new(File::open(log_path(&self.path, cmd_index.version))?)?;
            readers.insert(cmd_index.version, reader);
        }
        let reader = readers.get_mut(&cmd_index.version).unwrap();
        reader.seek(SeekFrom::Start(cmd_index.start))?;
        let cmd_reader = reader.take(cmd_index.len);
        Ok(serde_json::from_reader(cmd_reader)?)
    }

    fn read_and<F, R>(&self, cmd_pos: CommandIndex, f: F) -> Result<R>
        where
            F: FnOnce(io::Take<&mut BufReaderWithIndex<File>>) -> Result<R>,
    {
        self.remove_timeout_log();

        let mut readers = self.readers.borrow_mut();
        // Open the file if we haven't opened it in this `KvStoreReader`.
        // We don't use entry API here because we want the errors to be propogated.
        if !readers.contains_key(&cmd_pos.version) {
            let reader = BufReaderWithIndex::new(File::open(log_path(&self.path, cmd_pos.version))?)?;
            readers.insert(cmd_pos.version, reader);
        }
        let reader = readers.get_mut(&cmd_pos.version).unwrap();
        reader.seek(SeekFrom::Start(cmd_pos.start))?;
        let cmd_reader = reader.take(cmd_pos.len);
        f(cmd_reader)
    }
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        KvStoreReader {
            path: Arc::clone(&self.path),
            curr_version: Arc::clone(&self.curr_version),
            readers: RefCell::new(BTreeMap::new()),
        }
    }
}

struct KvStoreWriter {
    reader: KvStoreReader,
    writer: BufWriterWithIndex<File>,
    curr_version: u64,
    uncompacted: u64,
    path: Arc<PathBuf>,
    index: Arc<SkipMap<String, CommandIndex>>,
}

impl KvStoreWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.index;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set {key, ..} = cmd {
            if let Some(old_cmd) = self.index.get(&key) {
                self.uncompacted += old_cmd.value().len;
            }
            self.index.insert(key, (self.curr_version, pos..self.writer.index).into());
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            let pos = self.writer.index;
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove {key} = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.value().len;
                self.uncompacted += self.writer.index - pos;
            }
            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }
    fn compact(&mut self) -> Result<()> {
        let compact_version = self.curr_version + 1;
        self.curr_version += 2;

        self.writer = new_log_file_(&self.path, self.curr_version)?;

        let mut compact_writer = new_log_file_(&self.path, compact_version)?;

        let mut new_pos = 0;
        for entry in self.index.iter() {
            let len = self.reader.read_and(*entry.value(), |mut entry_reader| {
                Ok(io::copy(&mut entry_reader, &mut compact_writer)?)
            })?;
            self.index.insert(
                entry.key().clone(),
                (compact_version, new_pos..new_pos + len).into(),
            );
            new_pos += len;
        }
        compact_writer.flush()?;
        self.reader.curr_version.store(compact_version, Ordering::SeqCst);
        self.reader.remove_timeout_log();

        let stale_gens = get_log_list(&self.path)?
            .into_iter()
            .filter(|&gen| gen < compact_version);

        for stale_gen in stale_gens {
            let file_path = log_path(&self.path, stale_gen);
            if let Err(e) = fs::remove_file(&file_path) {
                error!("{:?} cannot be deleted: {}", file_path, e);
            }
        }
        self.uncompacted = 0;

        Ok(())
    }
}

fn new_log_file_(path: &Path, gen: u64) -> Result<BufWriterWithIndex<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithIndex::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    Ok(writer)
}

#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,

    index: Arc<SkipMap<String, CommandIndex>>,

    reader: KvStoreReader,

    writer: Arc<Mutex<KvStoreWriter>>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = Arc::new(path.into());
        fs::create_dir_all(&*path)?;

        let mut readers = BTreeMap::new();
        let index = Arc::new(SkipMap::new());

        let gen_list = get_log_list(&path)?;
        let mut uncompacted = 0;

        for &gen in &gen_list {
            let mut reader = BufReaderWithIndex::new(File::open(log_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &*index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file_(&path, current_gen)?;
        let safe_point = Arc::new(AtomicU64::new(0));

        let reader = KvStoreReader {
            path: Arc::clone(&path),
            curr_version: safe_point,
            readers: RefCell::new(readers),
        };

        let writer = KvStoreWriter {
            reader: reader.clone(),
            writer,
            curr_version: current_gen,
            uncompacted,
            path: Arc::clone(&path),
            index: Arc::clone(&index),
        };

        Ok(KvStore {
            path,
            reader,
            index,
            writer: Arc::new(Mutex::new(writer)),
        })

    }
}

impl KvEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            if let Command::Set { value, .. } = self.reader.read_command(*cmd_pos.value())? {
                Ok(Some(value))
            } else {
                Err(KvError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

/// 操作类型，序列化到日志中，便于后续恢复
#[derive(Deserialize, Serialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

#[derive(Debug, Clone, Copy)]
struct CommandIndex {
    version: u64,
    start: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandIndex {
    fn from((v, range): (u64, Range<u64>)) -> Self {
        CommandIndex {
            version: v,
            start: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithIndex<R: Read + Seek> {
    reader: BufReader<R>,
    index: u64,
}

impl<R: Read + Seek> BufReaderWithIndex<R> {
    fn new(mut inner: R) -> Result<Self> {
        let index = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithIndex {
            reader: BufReader::new(inner),
            index,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithIndex<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.index += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithIndex<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.index = self.reader.seek(pos)?;
        Ok(self.index)
    }
}

struct BufWriterWithIndex<W: Write + Seek> {
    writer: BufWriter<W>,
    index: u64,
}

impl<W: Write + Seek> BufWriterWithIndex<W> {
    fn new(mut inner: W) -> Result<Self> {
        let index = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithIndex {
            writer: BufWriter::new(inner),
            index,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithIndex<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.index += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithIndex<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let pos = self.writer.seek(pos)?;
        self.index = pos;
        Ok(pos)
    }
}
