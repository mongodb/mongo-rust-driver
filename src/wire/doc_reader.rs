use std::io::{self, Read};

pub struct DocReader<'a, R: 'a + Read> {
    r: &'a mut R,
    bytes_read: usize,
}

impl<'a, R: 'a + Read> DocReader<'a, R> {
    pub fn new(r: &'a mut R) -> Self {
        DocReader { r, bytes_read: 0 }
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }
}

impl<'a, R: 'a + Read> Read for DocReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let size = self.r.read(buf)?;
        self.bytes_read += size;

        Ok(size)
    }
}
