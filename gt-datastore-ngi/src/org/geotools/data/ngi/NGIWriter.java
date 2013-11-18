/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2012, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.ngi;

import java.io.FileWriter;
import java.util.logging.Logger;

import org.geotools.util.logging.Logging;

/**
 * NGI Writer
 * 
 * @author MapPlus, mapplus@gmail.com, http://onspatial.com
 * @since 2012-10-30
 * @see
 * 
 */
public class NGIWriter {
    protected static final Logger LOGGER = Logging.getLogger(NGIWriter.class);

    FileWriter fileWriter;

    public NGIWriter(FileWriter fileWriter) {
        this.fileWriter = fileWriter;
    }

    public void writeRecord(String[] headers) {
        // TODO :
    }

    public void write(Object value) {
        // TODO :
    }

    public void endRecord() {
        // TODO :

    }

    public void close() {
        // TODO :
    }
}
