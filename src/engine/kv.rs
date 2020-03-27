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
    version: u64,
    reader: &mut BufReaderWithIndex<File>,
    kvs: &mut BTreeMap<String, CommandIndex>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut uncompacted = 0;
    while let Some(cmd) = stream.next() {
        let curr_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                if let Some(old_cmd) = kvs.insert(key, (version, pos..curr_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            Command::Remove { key } => {
                if let Some(old_cmd) = kvs.remove(&key) {
                    uncompacted += old_cmd.len;
                }
            }
        }
        pos = curr_pos;
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

pub struct KvStore {
    path: PathBuf,

    // version -> file reader
    readers: HashMap<u64, BufReaderWithIndex<File>>,

    // curr file writer
    writer: BufWriterWithIndex<File>,

    curr_version: u64,

    // key -> command position of log file
    kvs: BTreeMap<String, CommandIndex>,

    uncompacted: u64,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut kvs = BTreeMap::new();

        let log_list = get_log_list(&path)?;
        let mut uncompacted = 0;

        for &version in &log_list {
            let mut reader = BufReaderWithIndex::new(File::open(log_path(&path, version))?)?;
            uncompacted += load(version, &mut reader, &mut kvs)?;
            readers.insert(version, reader);
        }

        let curr_version = log_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(&path, curr_version, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            curr_version,
            kvs,
            uncompacted,
        })
    }

    fn compact(&mut self) -> Result<()> {
        let compact_version = self.curr_version + 1;
        self.curr_version += 2;
        self.writer = self.new_log_file(self.curr_version)?;

        let mut compact_writer = self.new_log_file(compact_version)?;
        let mut offset = 0;

        for cmd in self.kvs.values_mut() {
            let reader = self.readers.get_mut(&cmd.version).expect("not found key");
            if reader.index != cmd.start {
                reader.seek(SeekFrom::Start(cmd.start))?;
            }
            let mut log_reader = reader.take(cmd.len);
            let len = io::copy(&mut log_reader, &mut compact_writer)?;
            *cmd = (compact_version, offset..offset + len).into();
            offset += len;
        }
        compact_writer.flush()?;
        let remove_versions: Vec<_> = self
            .readers
            .keys()
            .filter(|&&version| version < compact_version)
            .cloned()
            .collect();

        for log_version in remove_versions {
            self.readers.remove(&log_version);
            fs::remove_file(log_path(&self.path, log_version))?;
        }
        self.uncompacted = 0;
        Ok(())
    }

    fn new_log_file(&mut self, version: u64) -> Result<BufWriterWithIndex<File>> {
        new_log_file(&self.path, version, &mut self.readers)
    }
}

impl KvEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let pre_pos = self.writer.index;
        let cmd = Command::Set { key, value };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .kvs
                .insert(key, (self.curr_version, pre_pos..self.writer.index).into())
            {
                self.uncompacted += old_cmd.len;
            }
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_index) = self.kvs.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_index.version)
                .expect("can not find reader");
            reader.seek(SeekFrom::Start(cmd_index.start))?;
            let cmd_reader = reader.take(cmd_index.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.kvs.contains_key(&key) {
            let cmd = Command::Remove { key };
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                let old_cmd = self.kvs.remove(&key).expect("key not found");
                self.uncompacted += old_cmd.len;
            }
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }
}

/// 操作类型，序列化到日志中，便于后续恢复
#[derive(Deserialize, Serialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

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
