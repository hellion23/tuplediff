package com.hellion23.tuplediff.api.stream.source;

import com.hellion23.tuplediff.api.model.TDException;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

/**
 * Wraps a File Source
 */
@Slf4j
@EqualsAndHashCode
@ToString
public class FileStreamSource extends StreamSource {
    File file;

    public FileStreamSource (String filePath) {
        this (new File(filePath));
    }

    public FileStreamSource (File file) {
        this.file = file;
    }
    @Override
    protected InputStream openSource() throws TDException {
        try {
            return new FileInputStream(file);
        }
        catch (IOException ex) {
            throw new TDException("Could not open FILE stream source " + ex.getMessage(), ex);
        }

    }

    @Slf4j
    @ToString
    static class LoggingFileInputStream extends InputStream{
        File file;
        FileInputStream fis;
        public LoggingFileInputStream (File file) throws FileNotFoundException {
            this.file = file;
            log.info("Opening file " + file);
            this.fis = new FileInputStream(file);
        }


        @Override
        public int read() throws IOException {
            log.info("Reading from file: " + file);
            return fis.read();
        }

        @Override
        public void close() throws IOException {
//            log.error("Closing file: " + file, new Exception());
            fis.close();
        }
    }

}
