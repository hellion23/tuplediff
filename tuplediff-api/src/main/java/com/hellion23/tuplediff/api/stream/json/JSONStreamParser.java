package com.hellion23.tuplediff.api.stream.json;

import com.fasterxml.jackson.core.*;
import com.hellion23.tuplediff.api.compare.ComparableLinkedHashMap;
import com.hellion23.tuplediff.api.compare.ComparableLinkedList;
import com.hellion23.tuplediff.api.model.TDException;
import com.hellion23.tuplediff.api.stream.source.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Parses a JSON stream into a iterable JSONMembers. A JSONMember is either a field-element pair of a JSON
 * object or an element of an array. A JSONMember contains the field name (if relevant), the element and the
 * JSONType as interpreted by the Jackson parser. The value of this parsed JSONMember is transformed to a TDTuple
 * by the JSONTDStream.
 *
 * JSONStreamParser can also process naked JSON values such as Strings and Numbers (i.e. non-Objects), but these
 * cannot be used by JSONTDStream as naked values cannot be converted into a tuple. This class supports iterating
 * over non-Objects to support the myriad use cases of JSON Stream processing.
 *
 */
public class JSONStreamParser implements Iterator<JSONMember>  {
    private static final Logger log = LoggerFactory.getLogger(JSONStreamParser.class);

    JsonParser jParser;
    StreamSource streamSource;
    boolean isTupleCollectionAnArray;
    JsonPointer readToPointer = null;
    JSONMember next;
    boolean didNext;

    public JSONStreamParser (StreamSource streamSource, String readToPath) {
        this.streamSource = streamSource;
        if (!(readToPath == null || "".equals(readToPath.trim()) || "/".equalsIgnoreCase(readToPath.trim()))) {
            readToPointer = JsonPointer.compile(readToPath);
        }
    }

    /**
     * !) Gets the InputStream from the streamSource (if the streamSource wraps a RESTful call, this makes the connection
     * to the URL.
     * 2) Create a JSONParser that wraps the InputStream.
     * 3) Move parser cursor to the point where we need to start reading tuples.
     *
     * @throws IOException
     */
    public void open () throws IOException {
        streamSource.open();
        JsonFactory jfactory = new JsonFactory();
        log.info("Initializing parser for stream source: " + streamSource);
        InputStream inputStream = streamSource.inputStream();
        this.jParser = jfactory.createParser(inputStream);
        init();
    }

    protected void init() throws IOException {
        // Set pointer in JSON map to begin reading TDTuples.
        fastForwardToReadPointer();

        if (jParser.currentTokenId() == JsonTokenId.ID_NO_TOKEN) {
            // There is nothing to read! EOF
        }
        // the entire object is to be compared as a singleton tuple if the first token is an array object.
        else if (jParser.currentTokenId() == JsonTokenId.ID_START_OBJECT) {
            // Currently isTupleCollectionAnArray is set to true as this will be treated as an array of one.
            // Later on potentially support map based objects, where the field name is the primary key,
            // in which case this value is set to false.
            this.isTupleCollectionAnArray = true;
        }
        // If the first character is the start of an array, then forward to the beginning of the first object of this
        // array::
        else if (jParser.currentTokenId() == JsonTokenId.ID_START_ARRAY) {
            // This signifies that the first character is an array of objects.
            this.isTupleCollectionAnArray = true;
            jParser.nextToken();
        }
        else {
            throw new IOException ("The first token must be an ID_START_TOKEN " +
                    "({) or an ID_START_ARRAY ([), but" + " instead got a " + jParser.currentToken() +
                    ". " + contextInfo(jParser));
        }
    }

    protected boolean fastForwardToReadPointer () throws IOException {
        jParser.nextToken();
        if (readToPointer == null) {
            return true;
        }
        else {
            while(jParser.currentTokenId() != JsonTokenId.ID_NO_TOKEN) {
                if (readToPointer.equals(jParser.getParsingContext().pathAsPointer())) {
                    // if we have fastforwarded to the pointer, then set the pointer to the next token.
                    log.info("Successfully fast forwarded to: " + jParser.getParsingContext().pathAsPointer());
                    jParser.nextToken();
                    return true;
                }
                jParser.nextToken();
            }
            return false;
        }
    }

    @Override
    public boolean hasNext() {
        if (!didNext) {
            next = doNext();
            this.didNext = true;
        }
        return next != null;
    }

    @Override
    public JSONMember next() {
        if (!didNext) {
            doNext();
        }
        didNext = false;
        return next;
    }

    protected JSONMember doNext () {
//        log.info("Doing Next. Current location: " + contextInfo(jParser));
        JSONMember next = null;
        try {
            while (jParser.currentTokenId() != JsonTokenId.ID_NO_TOKEN && next == null) {
                switch (jParser.currentTokenId()) {
                    //These are naked JSONValue objects. They are cast into a JSONMember but not usable to TDStream
                    // because this is a naked, unnamed series of JSON values. However, it's in here to support
                    // uses of this StreamParser beyond that of TDStreams.
                    case JsonTokenId.ID_FALSE:
                    case JsonTokenId.ID_TRUE:
                    case JsonTokenId.ID_NULL:
                    case JsonTokenId.ID_STRING:
                    case JsonTokenId.ID_NUMBER_INT:
                    case JsonTokenId.ID_NUMBER_FLOAT:
                        JSONValue value = readInValue(jParser);
                        next = new JSONMember(value.jsonType, value.comparable);
                        break;
                    case JsonTokenId.ID_START_OBJECT:
                        next = constructJSONMember();
                        break;
                    case JsonTokenId.ID_END_ARRAY:
                        init();
                        break;
                }
            }
        } catch (IOException ex) {
            log.error("Cannot read any more tokens  "+ ex.getMessage(), ex);
            next = null;
        }
        return next;
    }

    protected JSONMember constructJSONMember() {
        JSONMember member;
        if (this.isTupleCollectionAnArray) {
            try {
                member= new JSONMember(readInObject(jParser));
            } catch (IOException e) {
                throw new TDException("Could not read an element in an array " + e.getMessage() + contextInfo(jParser));
            }
        }
        else {
            Map.Entry<String, JSONValue> mec;
            try {
                mec = readInFieldAndValue(jParser);
            } catch (IOException e) {
                throw new TDException("Could not read an field & value pair " + e.getMessage() + contextInfo(jParser));
            }
            Comparable c = mec.getValue();
            if (mec.getKey() == null) {
                throw new TDException("A null cannot be the field name in an Object. " + contextInfo(jParser));
            }
            if (!(c instanceof ComparableLinkedHashMap)) {
                throw new TDException("Expect to parse a JSON Object, but instead got a " + c.getClass() +
                        " class. Either change the readToPath variable to an array, a map of Object values or" +
                        " " + contextInfo(jParser));
            }
            else {
                // Insert the field member name as a key (it should be a primary key)
                member= new JSONMember(mec.getKey(), (ComparableLinkedHashMap)c);
            }
        }
//        log.info("Member created {} from source {} ", member, streamSource);
        return member;
    }

    protected static ComparableLinkedHashMap<String, JSONValue> readInObject(JsonParser jParser) throws IOException {
        if (jParser.currentTokenId() != JsonTokenId.ID_START_OBJECT) {
            throw new IOException("Expected START_OBJECT Json Token ({) to begin reading JSON object. " + contextInfo(jParser));
        }

        // Traverse to first field/value pair:
        jParser.nextToken();
//        log.info("ReadInObject Start: " + contextInfo(jParser));

        ComparableLinkedHashMap<String, JSONValue> map = new ComparableLinkedHashMap<>();

        while (jParser.currentTokenId() != JsonTokenId.ID_END_OBJECT) {
            Map.Entry <String, JSONValue> e = readInFieldAndValue(jParser);
            // Normalize field names if creating a TDTuple object.
            // Don't normalize field name if not a column for the TDTuple.
            map.put(e.getKey(), e.getValue());
            jParser.currentToken();
        }
//        log.info("Advancing readInObject END_OBJECT " + contextInfo(jParser));
        jParser.nextToken(); // Read past END_OBJECT
        return map;
    }

    protected static Map.Entry<String, JSONValue> readInFieldAndValue(JsonParser jParser) throws IOException {
        if (jParser.currentTokenId() != JsonTokenId.ID_FIELD_NAME) {
            throw new IOException("Expected FIELD_NAME Json Token while reading JSON object. " + contextInfo(jParser));
        }
        String key = jParser.currentName();

        if (key == null) {
            throw new IOException("Expected FIELD_NAME to be non null value.  " + contextInfo(jParser));
        }
        // traverse to the field value.
        jParser.nextToken();
//        log.info("readInFieldAndValue: " + contextInfo(jParser));

        JSONValue value = readInValue(jParser);
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    protected static ComparableLinkedList readInArray(JsonParser jParser) throws IOException {
        ComparableLinkedList<Comparable> list = new ComparableLinkedList<>();

        if (jParser.currentTokenId() != JsonTokenId.ID_START_ARRAY) {
            throw new IOException("Expected START_ARRAY Json Token ([) to begin reading JSON object. " + contextInfo(jParser));
        }

        // Advance to the first object.
        jParser.nextToken();
        while (jParser.currentTokenId() != JsonTokenId.ID_END_ARRAY) {
            list.add(readInValue(jParser));
        }
//        log.info("Advancing readInObject END_ARRAY " + contextInfo(jParser));
        jParser.nextToken(); // Read past END_ARRAY

        return list;
    }

    protected static JSONValue readInValue(JsonParser jParser) throws IOException {
        JSONValue value;
        switch (jParser.currentTokenId()) {
            case JsonTokenId.ID_START_OBJECT:
                value = new JSONValue(JSONType.OBJECT, readInObject(jParser));
                break;
            case JsonTokenId.ID_START_ARRAY:
                value = new JSONValue(JSONType.ARRAY, readInArray(jParser));
                break;
            case JsonTokenId.ID_TRUE:
                value = JSONValue.TRUE;
                jParser.nextToken();
                break;
            case JsonTokenId.ID_FALSE:
                value = JSONValue.FALSE;
                jParser.nextToken();
                break;
            case JsonTokenId.ID_NULL:
                value = JSONValue.NULL;
                jParser.nextToken();
                break;
            case JsonTokenId.ID_STRING:
                value = new JSONValue(JSONType.STRING, jParser.getValueAsString());
                jParser.nextToken();
                break;
            case JsonTokenId.ID_NUMBER_FLOAT:
            case JsonTokenId.ID_NUMBER_INT:
                value = new JSONValue(JSONType.NUMBER, jParser.getDecimalValue());
                jParser.nextToken();
                break;
            default:
                throw new IOException ("Could not read value/object/array. Unsupported token " + jParser.currentToken() + contextInfo(jParser));
        }
//        log.info("readInValue: " + contextInfo(jParser));
        return value;
    }

    /**
     * Prints the parsing context info. Used when an exception w/r to the parsing process is encountered and useful
     * parse location is helpful for debugging purposes.
     *
     * @param jParser
     * @return
     */
    protected static String contextInfo(JsonParser jParser) {
        JsonStreamContext context = jParser.getParsingContext();
        JsonLocation location = jParser.getCurrentLocation();

        // This needs to be try/catch because this method is used for logging and an Exception would result in nothing
        // being available to print.
        String text;
        try {
            text = jParser.getText();
        }
        catch (IOException ioe) {
            text = ioe.getMessage();
        }

        return String.format(" Current Token <%s>, name <%s>, value <%s>, path <%s>, Location: line=%s, col=%s",
                text , context.getCurrentName(), context.getCurrentValue(), context.pathAsPointer(false).toString(),
                location.getLineNr(), location.getColumnNr());
    }

}
