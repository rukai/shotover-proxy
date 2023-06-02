use anyhow::{anyhow, bail, Result};
use bytes::{Buf, Bytes};
use kafka_protocol::protocol::{buf::ByteBuf, DecodeError};
use kafka_protocol::records::Compression;
use kafka_protocol::{
    compression::{self as cmpr, Decompressor},
    records::CASTAGNOLI,
};

const MAGIC_BYTE_OFFSET: usize = 16;

pub struct RecordIterator {
    records_remaining: usize,
    buf: Bytes,
}

impl Iterator for RecordIterator {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        if self.records_remaining == 0 {
            None
        } else {
            use integer_encoding::VarInt;
            let peek_bytes = self.buf.peek_bytes(0..4);
            let (record_size, record_size_size) = i32::decode_var(&peek_bytes).unwrap();
            self.buf.advance(record_size_size);
            self.records_remaining -= 1;
            Some(self.buf.try_get_bytes(record_size as usize).unwrap())
        }
    }
}

impl RecordIterator {
    pub fn new<B: ByteBuf>(buf: &mut B) -> Result<Self> {
        let version = buf.try_peek_bytes(MAGIC_BYTE_OFFSET..(MAGIC_BYTE_OFFSET + 1))?[0] as i8;
        match version {
            2 => Self::decode_new_batch(buf, version),
            _ => Err(anyhow!("Unknown record batch version ({})", version)),
        }
    }

    fn decode_new_batch<B: ByteBuf>(buf: &mut B, version: i8) -> Result<Self> {
        buf.advance(8);

        let batch_length: i32 = buf.get_i32();
        let buf = &mut buf.try_get_bytes(batch_length as usize)?;

        buf.advance(4);

        // Magic byte
        let magic: i8 = buf.get_i8();
        if magic != version {
            bail!("Version mismatch ({} != {})", magic, version);
        }

        // CRC
        let supplied_crc: u32 = buf.get_u32();
        let actual_crc = CASTAGNOLI.checksum(buf);

        if supplied_crc != actual_crc {
            bail!(
                "Cyclic redundancy check failed ({} != {})",
                supplied_crc,
                actual_crc
            );
        }

        // Attributes
        let attributes: i16 = buf.get_i16();
        let compression = match attributes & 0x7 {
            0 => Compression::None,
            1 => Compression::Gzip,
            2 => Compression::Snappy,
            3 => Compression::Lz4,
            4 => Compression::Zstd,
            other => {
                bail!("Unknown compression algorithm used: {}", other);
            }
        };

        buf.advance(4 + 8 + 8 + 8 + 2 + 4);

        // Record count
        let record_count: i32 = buf.get_i32();
        let record_count = record_count as usize;

        // Records
        match compression {
            Compression::None => cmpr::None::decompress(buf, |buf| Self::iter(buf, record_count)),
            Compression::Snappy => {
                cmpr::Snappy::decompress(buf, |buf| Self::iter(buf, record_count))
            }
            Compression::Gzip => cmpr::Gzip::decompress(buf, |buf| Self::iter(buf, record_count)),
            _ => unimplemented!(),
        }
        .map_err(|e| anyhow!(e).context(format!("Failed to compress with {:?}", compression)))
    }

    fn iter(buf: &mut Bytes, records_remaining: usize) -> Result<Self, DecodeError> {
        Ok(RecordIterator {
            records_remaining,
            buf: buf.clone(),
        })
    }
}
