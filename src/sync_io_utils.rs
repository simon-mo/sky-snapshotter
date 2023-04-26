use std::io::{Read, Write};

pub struct BlockingReader<T>
where
    T: Read,
{
    inner_reader: T,
}

impl<T> BlockingReader<T>
where
    T: Read,
{
    pub fn new(inner_reader: T) -> Self {
        BlockingReader { inner_reader }
    }
}
impl<T> Read for BlockingReader<T>
where
    T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.inner_reader.read(buf) {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // spin here.
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
        }
    }
}

struct BlockingWriter<T>
where
    T: Write,
{
    inner_writer: T,
}

impl<T> BlockingWriter<T>
where
    T: Write,
{
    #[allow(dead_code)]
    fn new(inner_writer: T) -> Self {
        BlockingWriter { inner_writer }
    }
}

impl<T> Write for BlockingWriter<T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        loop {
            match self.inner_writer.write(buf) {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // spin here.
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        loop {
            match self.inner_writer.flush() {
                Ok(n) => return Ok(n),
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        // spin here.
                        std::thread::yield_now();
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            };
        }
    }
}
