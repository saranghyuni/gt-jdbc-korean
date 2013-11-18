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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.util.logging.Logging;

import com.vividsolutions.jts.geom.Coordinate;

/**
 * Abstract NGI Reader
 * 
 * @author MapPlus, mapplus@gmail.com, http://onspatial.com
 * @since 2012-10-30
 * @see
 * 
 */
public abstract class AbstractNGIReader {
    protected static final Logger LOGGER = Logging.getLogger(AbstractNGIReader.class);

    protected String seekLayer(BufferedReader reader, String layerName) {
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.equalsIgnoreCase("$LAYER_NAME")) {
                    String name = reader.readLine(); // "건물" => 건물
                    name = name.substring(1, name.length() - 1);
                    if (name.equalsIgnoreCase(layerName)) {
                        return name;
                    }
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }
        return null;
    }

    protected boolean seekPosition(BufferedReader reader, String cat) {
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.toUpperCase().startsWith(cat)) {
                    return true;
                }
                line = reader.readLine();
            }
        } catch (IOException e) {
            LOGGER.log(Level.FINER, e.getMessage(), e);
        }
        return false;
    }

    protected int parseInteger(String text) {
        return Integer.parseInt(text.trim());
    }

    protected double parseDouble(String text) {
        return Double.parseDouble(text.trim());
    }

    protected Coordinate parseCoordinate(String text) {
        String[] coords = text.split(" ");
        double x = Double.parseDouble(coords[0].trim());
        double y = Double.parseDouble(coords[1].trim());
        return new Coordinate(x, y);
    }
}
