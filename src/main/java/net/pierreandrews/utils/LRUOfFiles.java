package net.pierreandrews.utils;

import org.apache.commons.collections4.map.LRUMap;

import java.io.PrintWriter;

/**
 * An extention of LRUMap that knows how to close a writer
 *  when it's evicted from the map
 *  @see https://commons.apache.org/proper/commons-collections/apidocs/org/apache/commons/collections4/map/LRUMap.html#removeLRU
 * User: pierre
 * Date: 11/28/14
 */
public class LRUOfFiles extends LRUMap<String, PrintWriter> {

    public LRUOfFiles(int size) {
        super(size);
    }

    protected boolean removeLRU(LinkEntry<String, PrintWriter> entry) {
        entry.getValue().close();
        return true;
    }

    public void closeAll() {
        for(PrintWriter pw: values()) {
            pw.close();
        }
    }

}
