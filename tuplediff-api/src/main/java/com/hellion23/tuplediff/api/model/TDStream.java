package com.hellion23.tuplediff.api.model;

/**
 * Created by hleung on 5/23/2017.
 */
public interface TDStream <M extends TDStreamMetaData, T extends TDTuple > extends AutoCloseable {
    /**
     * Gets the metadata after the TDStream. Must not return null and should also not block for too long.
     * This method is called during the initialization phase before nearly anything else is invoked. If the metadata
     * cannot be ascertained prior to the stream being opened, then it is the responsiblility of the implemeter to
     * open the stream.
     *
     * @return
     */
    public M getMetaData();

    /**
     * Prepares the stream ready for reading. After this call, hasNext() and next() should not be blocked and the data
     * should be sorted by the primary key.
     * What this usually means for SQL streams is that the query has been executed, while for file based streams,
     * this creates a temp file data sorted (unless it is already sorted).
     *
     * @throws TDException If the Stream cannot be opened (underlying exception could be
     *  SQLException or IOException)
     */
    public void open () throws TDException;

    /**
     * Can read the next TDTuple.
     *
     * @return
     */
    public boolean hasNext ();

    /**
     * Get the next tuple. Increment the index.
     * @return
     */
    public T next();

    /**
     * Should return true if Stream has been opened successfully and not closed. Returns false otherwise.
     * @return
     */
    public boolean isOpen ();

    /**
     * Name is used to identify this TDStream. The name will appear in Exceptions and is useful for debugging.
     * TDCompare will by default generically label a Stream as "LEFT" or "RIGHT" if none provided.
     * @return
     */
    public String getName();

    /**
     * Is the stream's rows sorted by the Primary Key?
     * @return
     */
    default boolean isSorted() {
        return false;
    }
}
