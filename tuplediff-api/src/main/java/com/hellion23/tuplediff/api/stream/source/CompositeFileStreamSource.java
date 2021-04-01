package com.hellion23.tuplediff.api.stream.source;

import com.hellion23.tuplediff.api.model.TDException;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;


/**
 * Chains file streams together into a single stream. 0 byte files or files that don't exist are skipped.
 *
 */
@Slf4j
@EqualsAndHashCode
@ToString
public class CompositeFileStreamSource extends CompositeStreamSource<FileStreamSource> {
    Iterator<String> filePaths;
    File nextFile;

    public CompositeFileStreamSource (Iterator<String> filePaths) {
        this.filePaths = filePaths;
    }

    @Override
    public FileStreamSource doNext() {
        FileStreamSource next = null;
        while (next == null  && filePaths.hasNext()) {
            String fileName = filePaths.next();
            log.info("Advancing to next file " + fileName);
            nextFile = new File(fileName);
            if (!nextFile.exists()) {
                log.info("File {} does not exist. Moving to next file. ", nextFile);
            }
            else if (nextFile.length() == 0 ) {
                log.info("File {} has 0 byte length. Moving to next file. ", nextFile);
            }
            else {
                next = new FileStreamSource(nextFile);
                try {
                    log.info("Opening underlying source " + next);
                    next.open();
                } catch (IOException e) {
                    throw new TDException("Could not open file: " + nextFile + " Reason: " + e.getMessage(), e);
                }
            }
        }
        return next;
    }

}
